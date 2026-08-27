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
package org.acemq.amqp.api;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Where in a stream a consumer starts reading.
 *
 * <p>The idea a queue has no room for. A queue delivers what is there and forgets it; a stream
 * keeps everything until retention removes it, and every consumer holds its own position. Saying
 * where to start is therefore a normal thing to do rather than a recovery procedure, and it is
 * what replay is.
 *
 * <p>{@link #next()} is the default, and deliberately the timid one. A consumer deployed against
 * a stream holding a year of history and starting at {@link #first()} will read the year — which
 * is sometimes exactly right and is never what somebody wanted by accident.
 */
public final class StreamOffset {

    private enum Kind {
        FIRST, LAST, NEXT, ABSOLUTE, TIMESTAMP, INTERVAL
    }

    private static final StreamOffset FIRST = new StreamOffset(Kind.FIRST, 0L, null, null);
    private static final StreamOffset LAST = new StreamOffset(Kind.LAST, 0L, null, null);
    private static final StreamOffset NEXT = new StreamOffset(Kind.NEXT, 0L, null, null);

    private final Kind kind;
    private final long offset;
    private final @Nullable Instant timestamp;
    private final @Nullable Duration interval;

    private StreamOffset(
            Kind kind, long offset, @Nullable Instant timestamp, @Nullable Duration interval) {
        this.kind = kind;
        this.offset = offset;
        this.timestamp = timestamp;
        this.interval = interval;
    }

    /** @return everything the stream still holds, oldest first */
    public static StreamOffset first() {
        return FIRST;
    }

    /**
     * @return the last chunk the stream holds, and everything after it. Not the same as
     *     {@link #next()}: this replays a little, because a stream is written in chunks and the
     *     last one is where the broker can cheaply seek to
     */
    public static StreamOffset last() {
        return LAST;
    }

    /** @return only messages written after this consumer attached; the default */
    public static StreamOffset next() {
        return NEXT;
    }

    /**
     * @param offset the exact position to resume from, normally one past the last offset this
     *     consumer recorded as handled
     * @return that position
     */
    public static StreamOffset at(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative, was " + offset);
        }
        return new StreamOffset(Kind.ABSOLUTE, offset, null, null);
    }

    /**
     * @param timestamp the point in time to start from
     * @return the first message written at or after that moment
     */
    public static StreamOffset from(Instant timestamp) {
        return new StreamOffset(Kind.TIMESTAMP, 0L, Objects.requireNonNull(timestamp, "timestamp"), null);
    }

    /**
     * @param age how far back to start
     * @return the first message written within that period, so {@code ofHours(1)} reads the last
     *     hour and then continues
     */
    public static StreamOffset lastly(Duration age) {
        Objects.requireNonNull(age, "age");
        if (age.isNegative() || age.isZero()) {
            throw new IllegalArgumentException("age must be positive, was " + age);
        }
        return new StreamOffset(Kind.INTERVAL, 0L, null, age);
    }

    /**
     * The value the broker expects as the {@code x-stream-offset} consumer argument.
     *
     * <p>Types matter here rather than only values: the broker reads a string as a keyword, a
     * number as an absolute offset and a date as a timestamp, so a position sent as the wrong
     * type is not rejected but silently means something else.
     *
     * @return the argument value
     */
    public Object toConsumerArgument() {
        switch (kind) {
            case FIRST :
                return "first";
            case LAST :
                return "last";
            case NEXT :
                return "next";
            case ABSOLUTE :
                return offset;
            case TIMESTAMP :
                // Non-null by construction: only from(Instant) produces this kind.
                return Date.from(Objects.requireNonNull(timestamp));
            case INTERVAL :
                // The broker's own interval syntax. Seconds cover every duration exactly, where
                // rounding to days or hours would quietly move the starting point.
                return Objects.requireNonNull(interval).getSeconds() + "s";
            default :
                throw new IllegalStateException("unreachable offset kind " + kind);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StreamOffset)) {
            return false;
        }
        StreamOffset that = (StreamOffset) other;
        return kind == that.kind
                && offset == that.offset
                && Objects.equals(timestamp, that.timestamp)
                && Objects.equals(interval, that.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, offset, timestamp, interval);
    }

    @Override
    public String toString() {
        return "StreamOffset{" + toConsumerArgument() + "}";
    }
}
