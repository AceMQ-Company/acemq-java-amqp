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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.QueueType;
import org.acemq.amqp.transport.TransportException;

/**
 * An in-process broker: exchanges, bindings, queues and routing, with no network and no
 * container.
 *
 * <p>State is keyed by broker name, so two connections to {@code memory://orders} see the same
 * exchanges and queues while {@code memory://payments} is fully isolated. Tests that want a
 * clean slate use a unique name, which is cheaper and more reliable than tearing down shared
 * state.
 *
 * <p>What it does <em>not</em> do is as important as what it does. There is no replication and
 * no persistence. Queue-level time-to-live and dead-lettering are implemented, because the
 * retry ladder is built from them. The capability set reported by {@link InMemoryTransport}
 * states exactly which of these are real, and the conformance suite is what will keep this fake
 * honest against real brokers as behaviour is added.
 */
final class InMemoryBroker {

    private static final Map<String, InMemoryBroker> BROKERS = new ConcurrentHashMap<>();

    /** Fires queue expiries. One shared daemon thread is ample for test workloads. */
    private static final java.util.concurrent.ScheduledExecutorService EXPIRY = java.util.concurrent.Executors
            .newScheduledThreadPool(2, runnable -> {
                Thread thread = new Thread(runnable, "acemq-memory-expiry");
                thread.setDaemon(true);
                return thread;
            });

    private final String name;
    private final Map<String, Exchange> exchanges = new ConcurrentHashMap<>();
    private final Map<String, Queue> queues = new ConcurrentHashMap<>();

    private InMemoryBroker(String name) {
        this.name = name;
    }

    static InMemoryBroker named(String name) {
        return BROKERS.computeIfAbsent(name, InMemoryBroker::new);
    }

    /** Discards every broker. Useful between test classes that share a name. */
    static void reset() {
        BROKERS.values().forEach(broker -> {
            broker.exchanges.clear();
            broker.queues.clear();
        });
        BROKERS.clear();
    }

    String name() {
        return name;
    }

    void declareExchange(String exchangeName, String type, boolean durable) {
        Exchange existing = exchanges.get(exchangeName);
        if (existing != null && !existing.type.equalsIgnoreCase(type)) {
            // Real brokers refuse to redeclare an exchange with a different type. Copying that
            // here is the point: a test must fail for the same reason production would.
            throw new TransportException("exchange '" + exchangeName + "' already exists as type '" + existing.type
                    + "' and cannot be redeclared as '" + type + "'");
        }
        exchanges.computeIfAbsent(exchangeName, key -> new Exchange(key, type));
    }

    void declareQueue(String queueName, QueueType type, boolean durable, Map<String, Object> arguments) {
        Map<String, Object> args = arguments == null ? Collections.emptyMap() : arguments;
        Queue queue = queues.computeIfAbsent(queueName, key -> new Queue(key, type));

        // Queue-level time-to-live with a dead-letter target is what makes the retry ladder
        // work: a message waits in a rung doing nothing, expires, and is routed onward. Without
        // it here, retry behaviour could only ever be tested against a container.
        Object ttl = args.get("x-message-ttl");
        if (ttl instanceof Number) {
            queue.expireAfter(
                    ((Number) ttl).longValue(),
                    asString(args.get("x-dead-letter-exchange")),
                    asString(args.get("x-dead-letter-routing-key")),
                    this);
        }
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    /**
     * Routes a message that has expired out of a queue.
     *
     * <p>Kept separate from {@link #route} so the intent is visible in a stack trace: a message
     * arriving here was not published by an application, it timed out of a rung.
     */
    void routeExpired(String exchange, String routingKey, OutboundMessage message) {
        OutboundMessage rerouted = OutboundMessage.body(message.body())
                .exchange(exchange == null ? "" : exchange)
                .routingKey(routingKey == null ? "" : routingKey)
                .headers(message.headers())
                .messageId(message.messageId())
                .contentType(message.contentType())
                .build();
        route(rerouted);
    }

    void bindQueue(String queueName, String exchangeName, String routingKey) {
        Exchange exchange = exchanges.get(exchangeName);
        if (exchange == null) {
            throw new TransportException("cannot bind to exchange '" + exchangeName + "': it does not exist");
        }
        if (!queues.containsKey(queueName)) {
            throw new TransportException("cannot bind queue '" + queueName + "': it does not exist");
        }
        exchange.bindings.add(new Binding(queueName, routingKey));
    }

    void deleteQueue(String queueName) {
        queues.remove(queueName);
        exchanges.values().forEach(exchange -> exchange.bindings.removeIf(binding -> binding.queue.equals(queueName)));
    }

    Queue queue(String queueName) {
        Queue queue = queues.get(queueName);
        if (queue == null) {
            throw new TransportException("queue '" + queueName + "' does not exist");
        }
        return queue;
    }

    /**
     * Routes a message and returns the queues it reached.
     *
     * @return the queues the message was delivered to; empty means unroutable, which the core
     *     turns into a failed publish
     */
    Set<String> route(OutboundMessage message) {
        Set<String> delivered = new LinkedHashSet<>();

        if (message.exchange().isEmpty()) {
            // The default exchange: the routing key names a queue directly.
            Queue queue = queues.get(message.routingKey());
            if (queue != null) {
                queue.offer(message);
                delivered.add(queue.name);
            }
            return delivered;
        }

        Exchange exchange = exchanges.get(message.exchange());
        if (exchange == null) {
            return delivered;
        }
        for (Binding binding : exchange.bindings) {
            if (matches(exchange.type, binding.routingKey, message.routingKey())) {
                Queue queue = queues.get(binding.queue);
                if (queue != null && delivered.add(queue.name)) {
                    queue.offer(message);
                }
            }
        }
        return delivered;
    }

    /**
     * Applies AMQP routing rules.
     *
     * <p>Topic matching is the fiddly one: {@code *} matches exactly one dot-separated word and
     * {@code #} matches zero or more. Getting this wrong would let tests pass against
     * bindings that a real broker would not match, so it is implemented properly rather than
     * approximated with a substring check.
     */
    private static boolean matches(String exchangeType, String bindingKey, String routingKey) {
        switch (exchangeType.toLowerCase(java.util.Locale.ROOT)) {
            case "fanout" :
                return true;
            case "direct" :
                return bindingKey.equals(routingKey);
            case "topic" :
                return topicMatches(bindingKey, routingKey);
            case "headers" :
                // Header-based routing is not implemented; the transport does not claim the
                // capability, so reaching here means a test asked for something unsupported.
                throw new TransportException("the in-memory broker does not implement headers routing");
            default :
                throw new TransportException("unknown exchange type '" + exchangeType + "'");
        }
    }

    private static boolean topicMatches(String bindingKey, String routingKey) {
        String[] pattern = bindingKey.split("\\.", -1);
        String[] words = routingKey.split("\\.", -1);
        return topicMatches(pattern, 0, words, 0);
    }

    private static boolean topicMatches(String[] pattern, int p, String[] words, int w) {
        if (p == pattern.length) {
            return w == words.length;
        }
        if (pattern[p].equals("#")) {
            // '#' matches zero or more words, so every remaining split has to be considered.
            for (int skip = w; skip <= words.length; skip++) {
                if (topicMatches(pattern, p + 1, words, skip)) {
                    return true;
                }
            }
            return false;
        }
        if (w == words.length) {
            return false;
        }
        if (pattern[p].equals("*") || pattern[p].equals(words[w])) {
            return topicMatches(pattern, p + 1, words, w + 1);
        }
        return false;
    }

    /** An exchange and the bindings hanging off it. */
    private static final class Exchange {

        private final String name;
        private final String type;
        private final List<Binding> bindings = new java.util.concurrent.CopyOnWriteArrayList<>();

        Exchange(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }

    /** One binding from an exchange to a queue. */
    private static final class Binding {

        private final String queue;
        private final String routingKey;

        Binding(String queue, String routingKey) {
            this.queue = queue;
            this.routingKey = routingKey;
        }
    }

    /** A queue holding messages until a consumer settles them. */
    static final class Queue {

        private final String name;
        private final QueueType type;
        private final LinkedBlockingDeque<OutboundMessage> messages = new LinkedBlockingDeque<>();
        private volatile long timeToLiveMillis = -1;
        private volatile String deadLetterExchange;
        private volatile String deadLetterRoutingKey;
        private volatile InMemoryBroker owner;

        Queue(String name, QueueType type) {
            this.name = name;
            this.type = type;
        }

        String name() {
            return name;
        }

        /** Configures queue-level expiry with a dead-letter target. */
        void expireAfter(long millis, String exchange, String routingKey, InMemoryBroker owner) {
            this.timeToLiveMillis = millis;
            this.deadLetterExchange = exchange;
            this.deadLetterRoutingKey = routingKey;
            this.owner = owner;
        }

        void offer(OutboundMessage message) {
            messages.addLast(message);
            long ttl = timeToLiveMillis;
            if (ttl >= 0) {
                // The message is still visible in the queue while it waits, so depth() reports
                // what an operator would see, and it is only routed onward if it is still here
                // when the timer fires.
                EXPIRY.schedule(
                        () -> {
                            if (messages.remove(message) && owner != null) {
                                owner.routeExpired(deadLetterExchange, deadLetterRoutingKey, message);
                            }
                        },
                        ttl,
                        java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        }

        /** Returns the message to the front of the queue, as a requeue does. */
        void requeue(OutboundMessage message) {
            messages.addFirst(message);
        }

        OutboundMessage poll(long timeoutMillis) throws InterruptedException {
            return messages.pollFirst(timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        /** @return how many messages are waiting; the assertion most tests actually want */
        int depth() {
            return messages.size();
        }

        List<OutboundMessage> drain() {
            List<OutboundMessage> drained = new ArrayList<>();
            messages.drainTo(drained);
            return drained;
        }
    }
}
