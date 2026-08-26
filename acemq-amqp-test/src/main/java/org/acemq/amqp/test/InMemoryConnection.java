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
package org.acemq.amqp.test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.transport.Acknowledger;
import org.acemq.amqp.transport.ConfirmResult;
import org.acemq.amqp.transport.DeliveryListener;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.Subscription;
import org.acemq.amqp.transport.TransportConnection;
import org.acemq.amqp.transport.TransportException;

/** A connection to an {@link InMemoryBroker}. */
final class InMemoryConnection implements TransportConnection {

    private static final AtomicInteger DISPATCHER_COUNT = new AtomicInteger();

    private final InMemoryBroker broker;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final java.util.List<InMemorySubscription> subscriptions = new java.util.concurrent.CopyOnWriteArrayList<>();

    InMemoryConnection(InMemoryBroker broker) {
        this.broker = broker;
    }

    @Override
    public void declareExchange(String name, String type, boolean durable) {
        requireOpen();
        broker.declareExchange(name, type, durable);
    }

    @Override
    public void declareQueue(String name, QueueType type, boolean durable, Map<String, Object> arguments) {
        requireOpen();
        broker.declareQueue(name, type, durable, arguments);
    }

    @Override
    public void bindQueue(String queue, String exchange, String routingKey) {
        requireOpen();
        broker.bindQueue(queue, exchange, routingKey);
    }

    @Override
    public void deleteQueue(String name) {
        requireOpen();
        broker.deleteQueue(name);
    }

    @Override
    public ConfirmResult send(OutboundMessage message) {
        requireOpen();
        long startedAt = System.nanoTime();
        Set<String> delivered = broker.route(message);
        Duration latency = Duration.ofNanos(System.nanoTime() - startedAt);

        // An in-memory broker always accepts the message. Whether anything was bound to
        // receive it is a separate question, and the one that catches real mistakes.
        return delivered.isEmpty()
                ? ConfirmResult.unroutable(latency, "no queue is bound for routing key '" + message.routingKey() + "'")
                : ConfirmResult.confirmed(latency);
    }

    @Override
    public Subscription subscribe(String queue, int prefetch, DeliveryListener listener) {
        requireOpen();
        if (prefetch < 1) {
            throw new IllegalArgumentException("prefetch must be at least 1, was " + prefetch);
        }
        InMemorySubscription subscription = new InMemorySubscription(broker.queue(queue), prefetch, listener,
                subscriptions);
        subscriptions.add(subscription);
        subscription.start();
        return subscription;
    }

    @Override
    public boolean queueExists(String name) {
        requireOpen();
        return broker.hasQueue(name);
    }

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        subscriptions.forEach(InMemorySubscription::close);
        subscriptions.clear();
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new TransportException("the connection is closed");
        }
    }

    /**
     * Delivers messages from one queue to one listener.
     *
     * <p>A single dispatcher thread pulls from the queue, and a semaphore caps how many
     * deliveries may be unsettled at once. That is prefetch, modelled the same way a broker
     * models it, so a test can observe backpressure rather than only assuming it.
     */
    private static final class InMemorySubscription implements Subscription {

        private final InMemoryBroker.Queue queue;
        private final Semaphore unsettled;
        private final DeliveryListener listener;
        private final java.util.List<InMemorySubscription> registry;
        private final ExecutorService dispatcher;
        private final AtomicBoolean active = new AtomicBoolean(true);

        InMemorySubscription(
                InMemoryBroker.Queue queue,
                int prefetch,
                DeliveryListener listener,
                java.util.List<InMemorySubscription> registry) {
            this.queue = queue;
            this.unsettled = new Semaphore(prefetch);
            this.listener = listener;
            this.registry = registry;
            ThreadFactory factory = runnable -> {
                Thread thread = new Thread(runnable, "acemq-memory-dispatch-" + DISPATCHER_COUNT.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            };
            this.dispatcher = Executors.newSingleThreadExecutor(factory);
        }

        void start() {
            // execute rather than submit: submit hands back a Future that nobody
            // reads, so an exception escaping the pump loop would be captured in it
            // and never seen. The dispatcher would stop, the queue would stop being
            // consumed, and the test would hang rather than fail. With execute the
            // exception reaches the thread's uncaught handler and is printed.
            dispatcher.execute(this::pump);
        }

        private void pump() {
            while (active.get()) {
                try {
                    if (!unsettled.tryAcquire(50, TimeUnit.MILLISECONDS)) {
                        continue; // at the prefetch limit: wait for a settlement
                    }
                    OutboundMessage message = queue.poll(50);
                    if (message == null) {
                        unsettled.release();
                        continue;
                    }
                    deliver(message);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (RuntimeException e) {
                    // A listener that throws must not kill the dispatcher, or the queue stops
                    // being consumed and the test hangs rather than failing.
                    unsettled.release();
                }
            }
        }

        private void deliver(OutboundMessage message) {
            InboundDelivery delivery = new InboundDelivery(
                    queue.name(),
                    message.exchange(),
                    message.routingKey(),
                    message.body(),
                    message.headers(),
                    message.messageId(),
                    message.contentType(),
                    false);

            AtomicBoolean settled = new AtomicBoolean();
            Acknowledger acknowledger = new Acknowledger() {
                @Override
                public void accept() {
                    if (settled.compareAndSet(false, true)) {
                        unsettled.release();
                    }
                }

                @Override
                public void reject(boolean requeue) {
                    if (settled.compareAndSet(false, true)) {
                        if (requeue) {
                            queue.requeue(message);
                        }
                        unsettled.release();
                    }
                }
            };

            listener.onDelivery(delivery, acknowledger);

            // A listener that returns without settling would leak a prefetch slot. Real
            // brokers hold the message until the connection drops; here it is surfaced
            // immediately, because a silent leak in a fake is a debugging nightmare.
            if (!settled.get()) {
                unsettled.release();
                throw new TransportException("the delivery listener returned without settling a delivery from queue '"
                        + queue.name() + "'. Every delivery must be accepted or rejected exactly once.");
            }
        }

        @Override
        public String queue() {
            return queue.name();
        }

        @Override
        public boolean isActive() {
            return active.get();
        }

        @Override
        public void close() {
            if (!active.compareAndSet(true, false)) {
                return;
            }
            dispatcher.shutdownNow();
            try {
                dispatcher.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            registry.remove(this);
        }
    }
}
