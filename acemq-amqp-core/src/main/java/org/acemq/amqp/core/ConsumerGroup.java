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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Several consumers on one queue, resized while the application runs.
 *
 * <p>Two numbers govern how fast a queue drains, and both of them are usually guessed once, put
 * in a properties file, and never revisited — because changing either has meant a redeploy.
 * Neither actually has to.
 *
 * <ul>
 *   <li><strong>Prefetch</strong> — how many messages one consumer holds unacknowledged. Too low
 *       and the consumer idles between round trips; too high and one consumer hoards the queue
 *       while its neighbours starve, and a restart redelivers all of it.
 *   <li><strong>Concurrency</strong> — how many consumers there are. This is the one that adds
 *       throughput when handlers are slow because they are waiting on something else.
 * </ul>
 *
 * <pre>{@code
 * ConsumerGroup group = mq.consumeGroup("orders.new", Order.class, handler)
 *         .concurrency(4)
 *         .prefetch(50)
 *         .start();
 *
 * group.scaleTo(8);      // Friday afternoon
 * group.prefetch(100);
 * group.scaleTo(2);      // Sunday night, draining the four that go
 * }</pre>
 *
 * <p><strong>Scaling down drains rather than cancels.</strong> Removing a consumer that is
 * mid-message would leave the broker to redeliver it somewhere else, which is only harmless if
 * every handler is idempotent — and if that were reliably true, half this library would not need
 * to exist. So a consumer being removed stops taking new work and is given time to finish what it
 * holds.
 */
public final class ConsumerGroup implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroup.class);

    /**
     * How long a <em>set</em> of consumers being stopped is given to finish what it is holding,
     * in total.
     *
     * <p>Twenty seconds rather than thirty, and shared rather than per consumer, because the
     * number this has to fit inside is Kubernetes' default {@code terminationGracePeriodSeconds}
     * of 30. A budget handed to each consumer in turn is not a budget: eight consumers at thirty
     * seconds each is four minutes of a thirty-second grace period, at the end of which the pod
     * is killed anyway and every message still held is redelivered — the exact outcome draining
     * exists to avoid, arrived at slowly.
     */
    private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(20);

    private final String queue;
    private final Supplier<MessageConsumer> factory;
    private final List<MessageConsumer> members = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object resizeLock = new Object();

    private volatile int prefetch;
    private volatile Duration drainTimeout = DEFAULT_DRAIN_TIMEOUT;

    ConsumerGroup(String queue, int prefetch, Supplier<MessageConsumer> factory) {
        this.queue = queue;
        this.prefetch = prefetch;
        this.factory = factory;
    }

    /** @return the queue every member consumes */
    public String queue() {
        return queue;
    }

    /** @return how many consumers are running */
    public int size() {
        return members.size();
    }

    /** @return the prefetch each member is using */
    public int prefetch() {
        return prefetch;
    }

    /**
     * @param timeout how long the consumers being removed have between them to finish their work
     * @return this group
     */
    public ConsumerGroup drainTimeout(Duration timeout) {
        this.drainTimeout = Objects.requireNonNull(timeout, "timeout");
        return this;
    }

    /**
     * Grows or shrinks the group.
     *
     * <p>Growing is immediate. Shrinking drains the consumers being removed, so this call blocks
     * for as long as they need, up to {@link #drainTimeout(Duration)}. That is deliberate: a
     * scale-down that returned instantly and left work being abandoned in the background would
     * be the more convenient lie.
     *
     * @param size how many consumers there should be, at least one
     * @return this group
     */
    public ConsumerGroup scaleTo(int size) {
        if (size < 1) {
            throw new IllegalArgumentException("a group needs at least one consumer, was asked for " + size
                    + ". To stop consuming altogether, close the group.");
        }
        if (closed.get()) {
            throw new org.acemq.amqp.api.AceMqException("this consumer group is closed");
        }

        // Serialised, because two threads resizing at once would each read a stale size and
        // between them create or destroy the wrong number of consumers.
        synchronized (resizeLock) {
            int current = members.size();
            if (size > current) {
                grow(size - current);
            } else if (size < current) {
                shrink(current - size);
            }
        }
        return this;
    }

    private void grow(int howMany) {
        for (int i = 0; i < howMany; i++) {
            MessageConsumer consumer = factory.get();
            consumer.prefetch(prefetch);
            members.add(consumer);
        }
        log.info("consumer group on {} grew by {} to {}", queue, howMany, members.size());
    }

    private void shrink(int howMany) {
        List<MessageConsumer> removed = new ArrayList<>();
        for (int i = 0; i < howMany; i++) {
            // From the end, so the consumers that have been running longest stay. They are the
            // ones with warm connections and whatever caches the handler keeps.
            removed.add(members.remove(members.size() - 1));
        }
        if (!drainWithin(removed, drainTimeout)) {
            log.warn("consumers removed from the group on {} did not all finish within {}; what they still held"
                    + " will be redelivered", queue, drainTimeout);
        }
        for (MessageConsumer consumer : removed) {
            consumer.close();
        }
        log.info("consumer group on {} shrank by {} to {}", queue, howMany, members.size());
    }

    /**
     * Changes the prefetch of every member, and of members added later.
     *
     * @param prefetch the new limit, at least 1
     * @return this group
     */
    public ConsumerGroup prefetch(int prefetch) {
        if (prefetch < 1) {
            throw new IllegalArgumentException("prefetch must be at least 1, was " + prefetch);
        }
        this.prefetch = prefetch;
        for (MessageConsumer consumer : members) {
            consumer.prefetch(prefetch);
        }
        return this;
    }

    /** @return how many messages the whole group is handling right now */
    public long inFlight() {
        return sum(MessageConsumer::inFlight);
    }

    /** @return how many messages the group has acknowledged */
    public long acknowledged() {
        return sum(MessageConsumer::acknowledged);
    }

    /** @return how many deliveries the group has rejected */
    public long rejected() {
        return sum(MessageConsumer::rejected);
    }

    /** @return how many deliveries the group has retried */
    public long retried() {
        return sum(MessageConsumer::retried);
    }

    private long sum(java.util.function.ToLongFunction<MessageConsumer> field) {
        long total = 0;
        for (MessageConsumer consumer : members) {
            total += field.applyAsLong(consumer);
        }
        return total;
    }

    /**
     * Stops every member, letting them finish what they hold.
     *
     * @param timeout how long to wait in total, shared by every member
     * @return whether everything finished in time
     */
    public boolean drain(Duration timeout) {
        return drainWithin(members, timeout);
    }

    @Override
    public void close() {
        closeWithin(drainTimeout);
    }

    /**
     * Closes the group, giving every member together at most {@code budget} to finish.
     *
     * <p>Separate from {@link #close()} so that a caller stopping several groups at once can
     * hand out what is left of one budget rather than a fresh one per group, which is the same
     * multiplication this class avoids internally, one level up.
     *
     * @param budget the whole time the group's members have between them
     */
    void closeWithin(Duration budget) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        synchronized (resizeLock) {
            if (!drainWithin(members, budget)) {
                log.warn("the consumer group on {} did not finish within {}; what it still held will be"
                        + " redelivered", queue, budget);
            }
            for (MessageConsumer consumer : members) {
                consumer.close();
            }
            members.clear();
        }
    }

    /**
     * Drains consumers in turn against one deadline rather than giving each the full timeout.
     *
     * <p>Every consumer is still visited even once the deadline has passed, because draining
     * cancels the subscription before it waits: a consumer skipped entirely would keep taking
     * new work while the rest of the group shut down. What it loses past the deadline is the
     * waiting, not the stopping.
     */
    private boolean drainWithin(List<MessageConsumer> consumers, Duration budget) {
        long deadline = System.nanoTime() + budget.toNanos();
        boolean quiet = true;
        for (MessageConsumer consumer : consumers) {
            long remaining = deadline - System.nanoTime();
            quiet &= consumer.drain(Duration.ofNanos(Math.max(0L, remaining)));
        }
        return quiet;
    }

    @Override
    public String toString() {
        return "ConsumerGroup{queue=" + queue + ", size=" + members.size() + ", prefetch=" + prefetch + "}";
    }
}
