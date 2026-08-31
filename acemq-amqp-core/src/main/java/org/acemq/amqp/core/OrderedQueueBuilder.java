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
import java.util.Objects;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

/**
 * Says how a stream of messages is partitioned, and then declares it.
 *
 * <pre>{@code
 * OrderedQueue<Order> orders = mq.ordered("orders", Order.class)
 *         .partitions(8)
 *         .keyedBy(Order::customerId)
 *         .onFailure(OrderedQueue.OnFailure.RETRY_IN_PLACE, 3, Duration.ofSeconds(2))
 *         .declare();
 * }</pre>
 *
 * <p>The key is required and has no sensible default. A partitioned queue with no key is an
 * ordinary queue with extra steps.
 *
 * @param <T> payload type
 */
public final class OrderedQueueBuilder<T> {

    private static final int DEFAULT_PARTITIONS = 8;
    private static final int DEFAULT_PREFETCH = 20;

    private final AceMq mq;
    private final String name;
    private final Class<T> payloadType;
    private final int partitions;
    private final @Nullable Function<T, String> key;
    private final int prefetch;
    private final OrderedQueue.OnFailure onFailure;
    private final int retryAttempts;
    private final Duration retryDelay;

    OrderedQueueBuilder(AceMq mq, String name, Class<T> payloadType) {
        this(mq, name, payloadType, DEFAULT_PARTITIONS, null, DEFAULT_PREFETCH,
                OrderedQueue.OnFailure.STOP, 3, Duration.ofSeconds(1));
    }

    private OrderedQueueBuilder(
            AceMq mq,
            String name,
            Class<T> payloadType,
            int partitions,
            @Nullable Function<T, String> key,
            int prefetch,
            OrderedQueue.OnFailure onFailure,
            int retryAttempts,
            Duration retryDelay) {
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

    private OrderedQueueBuilder<T> with(
            int partitions,
            @Nullable Function<T, String> key,
            int prefetch,
            OrderedQueue.OnFailure onFailure,
            int retryAttempts,
            Duration retryDelay) {
        return new OrderedQueueBuilder<>(
                mq, name, payloadType, partitions, key, prefetch, onFailure, retryAttempts, retryDelay);
    }

    /**
     * @param partitions how many queues to spread keys across. This is the ceiling on parallelism
     *     and cannot be changed later without draining first, because the partition a key belongs
     *     to is a function of the count
     * @return a builder with that many partitions
     */
    public OrderedQueueBuilder<T> partitions(int partitions) {
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be at least 1, was " + partitions);
        }
        return with(partitions, key, prefetch, onFailure, retryAttempts, retryDelay);
    }

    /**
     * @param key what decides the sequence a message belongs to — a customer, an account, an
     *     aggregate identifier. Messages sharing one are handled in the order they were sent
     * @return a builder using it
     */
    public OrderedQueueBuilder<T> keyedBy(Function<T, String> key) {
        return with(partitions, Objects.requireNonNull(key, "key"), prefetch, onFailure, retryAttempts, retryDelay);
    }

    /**
     * @param prefetch how many messages each partition's consumer may hold. Safe for ordering at
     *     any value, because one consumer dispatches serially
     * @return a builder with that prefetch
     */
    public OrderedQueueBuilder<T> prefetch(int prefetch) {
        if (prefetch < 1) {
            throw new IllegalArgumentException("prefetch must be at least 1, was " + prefetch);
        }
        return with(partitions, key, prefetch, onFailure, retryAttempts, retryDelay);
    }

    /**
     * @param onFailure what a failing handler does to the sequence
     * @return a builder with that policy
     */
    public OrderedQueueBuilder<T> onFailure(OrderedQueue.OnFailure onFailure) {
        return with(partitions, key, prefetch, Objects.requireNonNull(onFailure, "onFailure"),
                retryAttempts, retryDelay);
    }

    /**
     * @param onFailure what a failing handler does to the sequence
     * @param attempts how many times to try, when the policy retries
     * @param delay how long to wait between attempts, during which the partition is stalled
     * @return a builder with that policy
     */
    public OrderedQueueBuilder<T> onFailure(OrderedQueue.OnFailure onFailure, int attempts, Duration delay) {
        if (attempts < 1) {
            throw new IllegalArgumentException("attempts must be at least 1, was " + attempts);
        }
        return with(partitions, key, prefetch, Objects.requireNonNull(onFailure, "onFailure"), attempts,
                Objects.requireNonNull(delay, "delay"));
    }

    /**
     * Creates the exchange, the partition queues and the bindings.
     *
     * @return the queue, ready to publish to and consume from
     */
    public OrderedQueue<T> declare() {
        if (key == null) {
            throw new IllegalStateException("an ordered queue needs a key: call keyedBy(...). Without one there is"
                    + " nothing to order by, and this would be an ordinary queue with extra steps.");
        }
        OrderedQueue<T> queue = new OrderedQueue<>(
                mq, name, payloadType, partitions, key, prefetch, onFailure, retryAttempts, retryDelay);
        queue.declareTopology();
        return queue;
    }

    @Override
    public String toString() {
        return "OrderedQueueBuilder{name=" + name + ", partitions=" + partitions + ", onFailure=" + onFailure + "}";
    }
}
