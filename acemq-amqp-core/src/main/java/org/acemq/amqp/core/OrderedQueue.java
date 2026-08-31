/*
 * Copyright 2026 AceMQ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.acemq.amqp.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.MessageHandler;
import org.acemq.amqp.api.Partitioning;
import org.acemq.amqp.api.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Messages that share a key are handled in the order they were sent.
 *
 * <pre>{@code
 * try (OrderedQueue<Order> orders = mq.ordered("orders", Order.class)
 *         .partitions(8)
 *         .keyedBy(Order::customerId)
 *         .declare()) {
 *
 *     orders.send(order);
 *     orders.consume(message -> ledger.post(message.payload()));
 * }
 * }</pre>
 *
 * <h2>What this actually costs</h2>
 *
 * <p>Ordering and parallelism are opposed, and no amount of API hides that. What this does is
 * make the standard trade cheap: the key decides a partition, each partition is a queue, and each
 * queue has exactly one consumer. Eight partitions gives eight-way parallelism and ordering
 * within any single key — which is almost always the ordering anyone actually wanted. Two orders
 * for one customer are sequenced; two orders for different customers are not, and never needed to
 * be.
 *
 * <p>Throughput is therefore bounded by the busiest key, not by the number of partitions. One
 * very hot customer is one queue, one consumer, and no way to go faster without giving up the
 * guarantee.
 *
 * <h2>Retries reorder, so the retry ladder is not available here</h2>
 *
 * <p>This is the part usually left unsaid. If message five fails and is republished to come back
 * in thirty seconds, message six is handled first and the sequence is broken. Every ordered
 * consumer has to choose between blocking the partition and breaking the order, so
 * {@link OrderedQueue.OnFailure} makes the choice explicit and offers nothing that quietly does
 * neither.
 *
 * <p>{@code RETRY_IN_PLACE} sleeps in the handler, which is a bug in an unordered consumer and
 * is the right answer here: the partition is meant to stall, because the alternative is
 * delivering message six before message five.
 *
 * @param <T> payload type
 */
public final class OrderedQueue<T> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrderedQueue.class);

    /** What happens when a handler fails. All three preserve order; they differ in what they cost. */
    public enum OnFailure {

        /**
         * Stop this partition and leave the message on the queue.
         *
         * <p>The default. The sequence for every key in that partition pauses until somebody
         * fixes the handler and restarts, and no message is lost or reordered. Other partitions
         * carry on, so a bad message stops a fraction of the traffic rather than all of it.
         */
        STOP,

        /**
         * Retry the same message, in the handler, with a delay between attempts.
         *
         * <p>Blocks the partition while it retries, which is exactly the intent: a transient
         * failure resolves without breaking the sequence. Sleeping in a handler is wrong for an
         * unordered consumer and right for this one. When the attempts run out it behaves as
         * {@link #STOP}.
         */
        RETRY_IN_PLACE,

        /**
         * Give up on the message, acknowledge it, and carry on.
         *
         * <p>Order is preserved for everything that follows, and one message has silently left
         * the sequence. Only reasonable where the sequence is advisory. {@link #skipped()} is
         * the only record that it happened, and is worth alerting on.
         */
        SKIP
    }

    private final AceMq mq;
    private final String name;
    private final Class<T> payloadType;
    private final int partitions;
    private final Function<T, String> key;
    private final int prefetch;
    private final OnFailure onFailure;
    private final int retryAttempts;
    private final java.time.Duration retryDelay;

    private final Map<Integer, Publisher<T>> publishers = new ConcurrentHashMap<>();
    private final List<MessageConsumer> consumers = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final AtomicLong handled = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final java.util.Set<Integer> halted = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Closing a consumer from inside its own handler would wait for the thread it is running on.
     * Halting therefore happens here instead.
     */
    private final ExecutorService halter = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "acemq-ordered-halt");
        thread.setDaemon(true);
        return thread;
    });

    OrderedQueue(
            AceMq mq,
            String name,
            Class<T> payloadType,
            int partitions,
            Function<T, String> key,
            int prefetch,
            OnFailure onFailure,
            int retryAttempts,
            java.time.Duration retryDelay) {
        this.mq = mq;
        this.name = name;
        this.payloadType = payloadType;
        this.partitions = partitions;
        this.key = key;
        this.prefetch = prefetch;
        this.onFailure = onFailure;
        this.retryAttempts = retryAttempts;
        this.retryDelay = retryDelay;
    }

    /** Declares the exchange, the partition queues and the bindings. */
    void declareTopology() {
        mq.declareExchange(name, "direct");

        // Replicated where the broker can, because a partition is a sequence: losing the node
        // holding it loses the middle of somebody's order history, and the messages that follow
        // arrive against a gap nobody can reconstruct. Where quorum queues are unavailable this
        // falls back rather than refusing, and says so once.
        org.acemq.amqp.transport.QueueType type = mq.supports(org.acemq.amqp.api.Capability.QUORUM_QUEUES)
                ? org.acemq.amqp.transport.QueueType.QUORUM
                : org.acemq.amqp.transport.QueueType.CLASSIC;
        if (type == org.acemq.amqp.transport.QueueType.CLASSIC) {
            log.warn("{} has no quorum queues, so the partitions of {} are classic. A node failure loses the"
                    + " middle of a sequence rather than pausing it.", mq.transportName(), name);
        }

        for (int partition = 0; partition < partitions; partition++) {
            String queue = queueFor(partition);
            mq.declareQueue(queue, type, java.util.Collections.emptyMap());
            mq.bind(queue, name, Partitioning.routingKeyFor(partition));
        }
        log.info("ordered queue {} declared with {} {} partitions", name, partitions,
                type.name().toLowerCase(java.util.Locale.ROOT));
    }

    /** @return the queue backing a partition */
    public String queueFor(int partition) {
        return name + "." + Partitioning.routingKeyFor(partition);
    }

    /** @return the logical name, which is also the exchange */
    public String name() {
        return name;
    }

    /** @return how many partitions there are */
    public int partitions() {
        return partitions;
    }

    /**
     * @param payload the message
     * @return which partition it went to, which is worth having in a log when a sequence looks
     *     wrong
     */
    public int send(T payload) {
        return send(payload, null);
    }

    /**
     * @param payload the message
     * @param envelope metadata to publish with, or {@code null}
     * @return the partition it went to
     */
    public int send(T payload, @org.jspecify.annotations.Nullable Envelope envelope) {
        String orderingKey = key.apply(payload);
        int partition = Partitioning.partitionFor(orderingKey, partitions);
        Publisher<T> publisher = publishers.computeIfAbsent(
                partition, p -> mq.publisher(name, Partitioning.routingKeyFor(p), payloadType));
        if (envelope == null) {
            publisher.send(payload);
        } else {
            publisher.send(payload, envelope);
        }
        return partition;
    }

    /**
     * Starts one consumer per partition.
     *
     * <p>One, deliberately. A second consumer on a partition would take the next message while
     * the first was still working, and the ordering would be gone.
     *
     * @param handler called for each message, in order within any one key
     * @return this queue
     */
    public OrderedQueue<T> consume(MessageHandler<T> handler) {
        for (int partition = 0; partition < partitions; partition++) {
            int index = partition;
            // Prefetch above one is safe for ordering because a single consumer dispatches
            // serially; it only means the broker has more in hand for this consumer.
            consumers.add(mq.consume(
                    queueFor(index),
                    payloadType,
                    ConsumerOptions.prefetch(prefetch).requeueOnFailure(),
                    message -> handleInPartition(index, message, handler)));
        }
        return this;
    }

    private void handleInPartition(int partition, org.acemq.amqp.api.Message<T> message, MessageHandler<T> handler)
            throws Exception {
        Exception lastFailure = null;
        int attempts = onFailure == OnFailure.RETRY_IN_PLACE ? retryAttempts : 1;

        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                handler.handle(message);
                handled.incrementAndGet();
                return;
            } catch (Exception e) {
                lastFailure = e;
                failed.incrementAndGet();
                if (attempt < attempts) {
                    log.warn("partition {} of {} failed on attempt {} of {}; holding the partition and retrying",
                            partition, name, attempt, attempts, e);
                    sleepBetweenAttempts();
                }
            }
        }

        if (onFailure == OnFailure.SKIP) {
            skipped.incrementAndGet();
            log.warn("skipping a message on partition {} of {}; the sequence for its key now has a gap and"
                    + " nothing else will report it", partition, name, lastFailure);
            return;
        }

        halt(partition, lastFailure);
        // Rethrown so the delivery is requeued rather than acknowledged: the message must still
        // be there when somebody fixes the handler and starts this partition again.
        throw lastFailure == null
                ? new IllegalStateException("handler failed on partition " + partition)
                : lastFailure;
    }

    private void sleepBetweenAttempts() {
        try {
            Thread.sleep(retryDelay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void halt(int partition, @org.jspecify.annotations.Nullable Throwable cause) {
        if (!halted.add(partition)) {
            return;
        }
        log.error("stopping partition {} of {} after a handler failure. The message stays on {}; other partitions"
                + " keep running.", partition, name, queueFor(partition), cause);
        if (partition < consumers.size()) {
            MessageConsumer consumer = consumers.get(partition);
            // Off this thread: closing waits for the dispatcher, and the dispatcher is what is
            // running this handler.
            halter.execute(consumer::close);
        }
    }

    /** @return partitions stopped by a handler failure */
    public java.util.Set<Integer> haltedPartitions() {
        return Collections.unmodifiableSet(new java.util.TreeSet<>(halted));
    }

    /** @return how many messages have been handled successfully */
    public long handled() {
        return handled.get();
    }

    /** @return how many handler attempts have failed */
    public long failed() {
        return failed.get();
    }

    /** @return how many messages were given up on; only ever non-zero with {@link OnFailure#SKIP} */
    public long skipped() {
        return skipped.get();
    }

    /** @return the queues behind the partitions, in order */
    public List<String> queues() {
        List<String> names = new ArrayList<>(partitions);
        for (int partition = 0; partition < partitions; partition++) {
            names.add(queueFor(partition));
        }
        return names;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (MessageConsumer consumer : consumers) {
            consumer.close();
        }
        consumers.clear();
        publishers.values().forEach(Publisher::close);
        publishers.clear();
        halter.shutdown();
        try {
            if (!halter.awaitTermination(5, TimeUnit.SECONDS)) {
                halter.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "OrderedQueue{name=" + name + ", partitions=" + partitions + ", halted=" + halted + "}";
    }
}
