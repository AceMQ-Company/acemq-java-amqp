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

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Says how many consumers to run and how much each may hold, and then starts them.
 *
 * <pre>{@code
 * ConsumerGroup orders = mq.consumeGroup("orders.new", Order.class, handler)
 *         .concurrency(4)
 *         .prefetch(50)
 *         .start();
 * }</pre>
 *
 * <p>Nothing consumes until {@link #start()}. Every other method returns a new builder, so a
 * half-built one can be kept as a field or reused.
 *
 * @param <T> payload type
 */
public final class ConsumerGroupBuilder<T> {

    private final String queue;
    private final Class<T> payloadType;
    private final ConsumerOptions options;
    private final int concurrency;
    private final BiFunction<ConsumerGroupBuilder<T>, Integer, ConsumerGroup> starter;

    ConsumerGroupBuilder(
            String queue,
            Class<T> payloadType,
            ConsumerOptions options,
            int concurrency,
            BiFunction<ConsumerGroupBuilder<T>, Integer, ConsumerGroup> starter) {
        this.queue = queue;
        this.payloadType = payloadType;
        this.options = options;
        this.concurrency = concurrency;
        this.starter = starter;
    }

    private ConsumerGroupBuilder<T> with(ConsumerOptions updated, int howMany) {
        return new ConsumerGroupBuilder<>(queue, payloadType, updated, howMany, starter);
    }

    /**
     * @param concurrency how many consumers to run. More helps when handlers spend their time
     *     waiting on something else; it does nothing for a handler that is already CPU-bound
     * @return a builder with that concurrency
     */
    public ConsumerGroupBuilder<T> concurrency(int concurrency) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be at least 1, was " + concurrency);
        }
        return with(options, concurrency);
    }

    /**
     * @param prefetch how many messages each consumer may hold unacknowledged
     * @return a builder with that prefetch
     */
    public ConsumerGroupBuilder<T> prefetch(int prefetch) {
        return with(options.prefetch(prefetch), concurrency);
    }

    /**
     * @param options everything else about how the consumers behave — retries, idempotency, the
     *     codec to read with
     * @return a builder using them
     */
    public ConsumerGroupBuilder<T> options(ConsumerOptions options) {
        return with(Objects.requireNonNull(options, "options"), concurrency);
    }

    /** @return the running group */
    public ConsumerGroup start() {
        return starter.apply(this, concurrency);
    }

    String queue() {
        return queue;
    }

    Class<T> payloadType() {
        return payloadType;
    }

    ConsumerOptions options() {
        return options;
    }

    @Override
    public String toString() {
        return "ConsumerGroupBuilder{queue=" + queue + ", concurrency=" + concurrency + ", prefetch="
                + options.prefetch() + "}";
    }
}
