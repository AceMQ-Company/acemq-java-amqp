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

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.StreamOffset;
import org.jspecify.annotations.Nullable;

/**
 * How a stream should be read.
 *
 * <p>Built by the DSL rather than by hand:
 *
 * <pre>{@code
 * mq.stream("orders.log", OrderPlaced.class)
 *         .fromFirst()
 *         .prefetch(200)
 *         .skipFailures()
 *         .consume(message -> project(message.payload()));
 * }</pre>
 */
public final class StreamOptions {

    /** What to do when a handler throws. There is no third answer a stream can give. */
    public enum OnFailure {

        /**
         * Stop the consumer and record why.
         *
         * <p>The default, because a stream is usually read to build something — a projection, a
         * report, a search index — and a gap in that is invisible once it exists. Stopping is
         * loud, and the offset is still there to resume from once the handler is fixed.
         */
        STOP,

        /**
         * Pass over the message, count it, and carry on.
         *
         * <p>Right when lateness is worse than completeness, such as a live dashboard. Wrong
         * whenever anything downstream is derived from every message, because nothing will ever
         * say what was missed.
         */
        SKIP
    }

    private static final int DEFAULT_PREFETCH = 100;

    private final StreamOffset offset;
    private final int prefetch;
    private final OnFailure onFailure;
    private final @Nullable Codec codec;

    private StreamOptions(StreamOffset offset, int prefetch, OnFailure onFailure, @Nullable Codec codec) {
        if (prefetch < 1) {
            // Not merely unwise here but refused by the broker: a stream consumer with no
            // prefetch has no way to apply backpressure, so RabbitMQ closes the channel with an
            // error that does not explain itself.
            throw new IllegalArgumentException("prefetch must be at least 1, was " + prefetch
                    + ". A stream consumer must have one: it is the only backpressure a stream has.");
        }
        this.offset = offset;
        this.prefetch = prefetch;
        this.onFailure = onFailure;
        this.codec = codec;
    }

    /** @return read only messages written from now on, with a prefetch of 100 */
    public static StreamOptions defaults() {
        return new StreamOptions(StreamOffset.next(), DEFAULT_PREFETCH, OnFailure.STOP, null);
    }

    /** @return everything the stream still holds, oldest first */
    public StreamOptions fromFirst() {
        return withOffset(StreamOffset.first());
    }

    /** @return the last chunk the stream holds and everything after it */
    public StreamOptions fromLast() {
        return withOffset(StreamOffset.last());
    }

    /** @return only messages written after this consumer attaches */
    public StreamOptions fromNext() {
        return withOffset(StreamOffset.next());
    }

    /**
     * @param offset exact position, normally one past the last offset recorded as handled
     * @return options starting there
     */
    public StreamOptions fromOffset(long offset) {
        return withOffset(StreamOffset.at(offset));
    }

    /**
     * @param timestamp point in time to start from
     * @return options starting at the first message written at or after it
     */
    public StreamOptions from(Instant timestamp) {
        return withOffset(StreamOffset.from(timestamp));
    }

    /**
     * @param age how far back to begin
     * @return options replaying that period and then continuing
     */
    public StreamOptions fromLast(Duration age) {
        return withOffset(StreamOffset.lastly(age));
    }

    /**
     * @param offset where to start
     * @return options starting there
     */
    public StreamOptions withOffset(StreamOffset offset) {
        return new StreamOptions(Objects.requireNonNull(offset, "offset"), prefetch, onFailure, codec);
    }

    /**
     * @param prefetch how many unacknowledged messages the broker may have in flight
     * @return options with that prefetch
     */
    public StreamOptions prefetch(int prefetch) {
        return new StreamOptions(offset, prefetch, onFailure, codec);
    }

    /**
     * Carries on past a message whose handler failed.
     *
     * @return options that skip failures rather than stopping
     */
    public StreamOptions skipFailures() {
        return new StreamOptions(offset, prefetch, OnFailure.SKIP, codec);
    }

    /**
     * @param codec the format to read with, for formats whose bytes describe nothing
     * @return options reading that format
     */
    public StreamOptions as(Codec codec) {
        return new StreamOptions(offset, prefetch, onFailure, Objects.requireNonNull(codec, "codec"));
    }

    /** @return where reading starts */
    public StreamOffset offset() {
        return offset;
    }

    /** @return how many unacknowledged messages may be in flight */
    public int prefetchCount() {
        return prefetch;
    }

    /** @return what happens when a handler throws */
    public OnFailure onFailure() {
        return onFailure;
    }

    /** @return the format this consumer was told to read, if it was told one */
    public java.util.Optional<Codec> codec() {
        return java.util.Optional.ofNullable(codec);
    }

    @Override
    public String toString() {
        return "StreamOptions{from=" + offset + ", prefetch=" + prefetch + ", onFailure=" + onFailure + "}";
    }
}
