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
import java.time.Instant;
import java.util.Objects;
import java.util.function.BiFunction;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.MessageHandler;
import org.acemq.amqp.api.StreamOffset;

/**
 * Says where to start reading a stream, and then starts.
 *
 * <pre>{@code
 * mq.stream("orders.log", OrderPlaced.class)
 *         .fromFirst()
 *         .prefetch(200)
 *         .consume(message -> projection.apply(message.payload()));
 *
 * mq.stream("orders.log", OrderPlaced.class)
 *         .fromLast(Duration.ofHours(1))
 *         .consume(message -> dashboard.show(message.payload()));
 *
 * mq.stream("orders.log", OrderPlaced.class)
 *         .fromOffset(resumeFrom)
 *         .consume(message -> ledger.post(message.payload()));
 * }</pre>
 *
 * <p>Nothing happens until {@link #consume}. Every other method returns a new reader, so a
 * half-built one can be passed around, kept as a field, or reused for two consumers reading the
 * same stream from different places.
 *
 * @param <T> payload type
 */
public final class StreamReader<T> {

    private final String queue;
    private final Class<T> payloadType;
    private final StreamOptions options;
    private final BiFunction<StreamReader<T>, MessageHandler<T>, StreamConsumer> starter;

    StreamReader(
            String queue,
            Class<T> payloadType,
            StreamOptions options,
            BiFunction<StreamReader<T>, MessageHandler<T>, StreamConsumer> starter) {
        this.queue = queue;
        this.payloadType = payloadType;
        this.options = options;
        this.starter = starter;
    }

    private StreamReader<T> with(StreamOptions updated) {
        return new StreamReader<>(queue, payloadType, updated, starter);
    }

    /**
     * @return a reader starting at the oldest message the stream still holds. On a stream with a
     *     year of history this reads the year, which is sometimes exactly right
     */
    public StreamReader<T> fromFirst() {
        return with(options.fromFirst());
    }

    /** @return a reader starting at the last chunk the stream holds, and everything after it */
    public StreamReader<T> fromLast() {
        return with(options.fromLast());
    }

    /** @return a reader taking only messages written after it attaches; the default */
    public StreamReader<T> fromNext() {
        return with(options.fromNext());
    }

    /**
     * @param offset the exact position, normally one past the last offset recorded as handled
     * @return a reader resuming there
     */
    public StreamReader<T> fromOffset(long offset) {
        return with(options.fromOffset(offset));
    }

    /**
     * @param timestamp the moment to start from
     * @return a reader starting at the first message written at or after it
     */
    public StreamReader<T> from(Instant timestamp) {
        return with(options.from(timestamp));
    }

    /**
     * @param age how far back to begin
     * @return a reader replaying that period and then continuing live
     */
    public StreamReader<T> fromLast(Duration age) {
        return with(options.fromLast(age));
    }

    /**
     * @param offset where to start
     * @return a reader starting there
     */
    public StreamReader<T> from(StreamOffset offset) {
        return with(options.withOffset(offset));
    }

    /**
     * @param prefetch how many unacknowledged messages the broker may keep in flight
     * @return a reader with that prefetch
     */
    public StreamReader<T> prefetch(int prefetch) {
        return with(options.prefetch(prefetch));
    }

    /**
     * Carries on past a message whose handler failed, counting it.
     *
     * <p>The alternative to stopping, and the one to think about. Nothing else will report the
     * gap; {@link StreamConsumer#skipped()} is the only record that it happened.
     *
     * @return a reader that skips failures
     */
    public StreamReader<T> skipFailures() {
        return with(options.skipFailures());
    }

    /**
     * @param codec the format to read with, for formats whose bytes describe nothing
     * @return a reader using that codec
     */
    public StreamReader<T> as(Codec codec) {
        return with(options.as(codec));
    }

    /**
     * Starts reading.
     *
     * @param handler called for each message in offset order; returning normally advances the
     *     position, throwing applies the failure policy
     * @return the running consumer, closed automatically when the connection closes
     */
    public StreamConsumer consume(MessageHandler<T> handler) {
        return starter.apply(this, Objects.requireNonNull(handler, "handler"));
    }

    String queue() {
        return queue;
    }

    Class<T> payloadType() {
        return payloadType;
    }

    StreamOptions options() {
        return options;
    }

    @Override
    public String toString() {
        return "StreamReader{queue=" + queue + ", " + options + "}";
    }
}
