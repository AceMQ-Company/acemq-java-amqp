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
package org.acemq.amqp.transport.rabbitmq;

import java.util.Map;
import java.util.TreeMap;

import org.jspecify.annotations.Nullable;

/**
 * How far a stream subscription has got, so a recovered one carries on from there.
 *
 * <p>A queue forgets what it delivered, so consuming it again after a recovery takes whatever is
 * left. A stream forgets nothing, and consuming it again starts wherever {@code x-stream-offset}
 * says, and the client recovers a consumer with the arguments it was first given. A reader that
 * began at {@code first} replayed the whole stream, and one that began at {@code next} skipped
 * everything appended while it was away.
 *
 * <p>A recovered stream subscription therefore starts at the oldest entry it was given and never
 * settled, or just after the newest it settled: what a queue would redeliver, and nothing it
 * would not. The same rule as the .NET, Go and Ruby libraries.
 */
final class StreamPosition {

    static final String ARGUMENT = "x-stream-offset";

    /** The arguments the client recovers the consumer with; rewritten just before it does. */
    private final Map<String, Object> arguments;

    /** Offsets handed to the handler and not yet settled, with how many copies are out. */
    private TreeMap<Long, Integer> pending = new TreeMap<>();

    private long settled = -1L;

    /** Which connection's deliveries count; moved on by every recovery. */
    private int generation;

    StreamPosition(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    /** A delivery at this offset was handed to the handler. Returns the token to settle it with. */
    synchronized int delivered(long offset) {
        pending.merge(offset, 1, Integer::sum);
        return generation;
    }

    /** The delivery at this offset was acknowledged or rejected. */
    synchronized void settle(long offset, int token) {
        // A copy from before the last recovery. Its offset is being delivered again, and
        // letting it move the position could carry a later recovery past entries the new copies
        // have not reached yet.
        if (token != generation) {
            return;
        }
        pending.computeIfPresent(offset, (key, copies) -> copies <= 1 ? null : copies - 1);
        settled = Math.max(settled, offset);
    }

    /**
     * The offset a recovered subscription starts at, or null to keep its own starting point
     * because nothing was delivered.
     *
     * <p>The pending deliveries belonged to the channel that has gone and are delivered again by
     * the new one, so they are forgotten here, and a handler still finishing an old copy no
     * longer moves the position.
     */
    synchronized @Nullable Long resume() {
        Long oldest = pending.isEmpty() ? null : pending.firstKey();
        pending = new TreeMap<>();
        generation++;
        if (oldest != null) {
            return oldest;
        }
        return settled >= 0 ? settled + 1 : null;
    }

    /** Moves the recovery arguments to {@link #resume()}, if anything was delivered. */
    void rewind() {
        Long resume = resume();
        if (resume != null) {
            arguments.put(ARGUMENT, resume);
        }
    }

    /** The offset RabbitMQ stamps on every stream delivery, if present. */
    static @Nullable Long offsetOf(@Nullable Map<String, Object> headers) {
        Object value = headers == null ? null : headers.get(ARGUMENT);
        return value instanceof Number ? ((Number) value).longValue() : null;
    }
}
