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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

/**
 * Where a message is going, carried by the message.
 *
 * <p>A pipeline has no coordinator. Each step reads the slip, does its work, advances the
 * position and publishes to whatever comes next; when the position runs off the end, the run is
 * over. That is choreography rather than orchestration, and the difference is worth the two
 * headers it costs:
 *
 * <ul>
 *   <li>There is no flow engine to run, page somebody about, or upgrade — nothing to be down.
 *   <li>A consumer in Go reads three headers. The Java library is not privileged.
 *   <li>The state survives a restart, because it is in the message rather than in a
 *       coordinator's memory.
 *   <li>A message dead-lettered at step two keeps its slip, so replaying it <em>resumes</em> the
 *       run instead of starting it again.
 * </ul>
 *
 * <p>The cost, and it is a real one: <strong>the route is fixed when the message is first
 * sent.</strong> Changing a pipeline does not change messages already travelling, which finish
 * along the route they were given. During a deploy that is usually what anyone would want, and
 * it is occasionally surprising.
 *
 * <p>Steps are named rather than numbered on the wire, so a slip stays readable in a management
 * console: {@code validate,enrich,dispatch} with a position of {@code 1} says exactly where a
 * message is without anybody decoding anything.
 */
public final class RoutingSlip {

    /** The ordered step names, comma-separated. */
    public static final String ROUTE = AceHeaders.PREFIX + "route";

    /** Which step is next, counting from zero. */
    public static final String POSITION = AceHeaders.PREFIX + "route-position";

    /** Identifies one run through a pipeline, across every hop. */
    public static final String RUN_ID = AceHeaders.PREFIX + "route-id";

    private final List<String> steps;
    private final int position;
    private final String runId;

    private RoutingSlip(List<String> steps, int position, String runId) {
        this.steps = Collections.unmodifiableList(new ArrayList<>(steps));
        this.position = position;
        this.runId = runId;
    }

    /**
     * @param steps the step names, in order
     * @return a slip at the beginning of that route, with a fresh run identifier
     */
    public static RoutingSlip startOf(List<String> steps) {
        Objects.requireNonNull(steps, "steps");
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("a route needs at least one step");
        }
        return new RoutingSlip(steps, 0, UUID.randomUUID().toString());
    }

    /**
     * Reads a slip off a message's headers.
     *
     * @param headers the message headers
     * @return the slip, or empty when the message is not travelling a route
     */
    public static Optional<RoutingSlip> from(Map<String, Object> headers) {
        Object route = headers.get(ROUTE);
        if (route == null) {
            return Optional.empty();
        }
        List<String> steps = new ArrayList<>();
        for (String step : route.toString().split(",")) {
            String trimmed = step.trim();
            if (!trimmed.isEmpty()) {
                steps.add(trimmed);
            }
        }
        if (steps.isEmpty()) {
            return Optional.empty();
        }
        Object rawPosition = headers.get(POSITION);
        int at = rawPosition instanceof Number ? ((Number) rawPosition).intValue() : parse(rawPosition);
        Object id = headers.get(RUN_ID);
        return Optional.of(new RoutingSlip(steps, at, id == null ? UUID.randomUUID().toString() : id.toString()));
    }

    private static int parse(@Nullable Object raw) {
        try {
            return raw == null ? 0 : Integer.parseInt(raw.toString().trim());
        } catch (NumberFormatException e) {
            // A slip whose position is unreadable is worse than one with none: it would send
            // the message to an arbitrary step. Starting over is the only defensible reading.
            return 0;
        }
    }

    /** @return the step this message is for now, or empty when the route is finished */
    public Optional<String> current() {
        return position >= 0 && position < steps.size() ? Optional.of(steps.get(position)) : Optional.empty();
    }

    /** @return the step after the current one, or empty when this is the last */
    public Optional<String> next() {
        return position + 1 < steps.size() ? Optional.of(steps.get(position + 1)) : Optional.empty();
    }

    /**
     * @param position which step to start at
     * @return a slip positioned there, for a message replayed straight into a queue with no slip
     *     of its own
     */
    public RoutingSlip advanceTo(int position) {
        return new RoutingSlip(steps, position, runId);
    }

    /** @return a slip advanced by one step */
    public RoutingSlip advance() {
        return new RoutingSlip(steps, position + 1, runId);
    }

    /** @return whether every step has been handled */
    public boolean isFinished() {
        return position >= steps.size();
    }

    /** @return the steps, in order */
    public List<String> steps() {
        return steps;
    }

    /** @return which step is next, counting from zero */
    public int position() {
        return position;
    }

    /** @return the identifier of this run, stable across every hop, a dead-letter and a replay */
    public String runId() {
        return runId;
    }

    /**
     * @return the headers to publish this slip with; merge them into an envelope's own
     */
    public Map<String, Object> toHeaders() {
        Map<String, Object> headers = new java.util.LinkedHashMap<>();
        headers.put(ROUTE, String.join(",", steps));
        headers.put(POSITION, position);
        headers.put(RUN_ID, runId);
        return headers;
    }

    /**
     * @param steps step names
     * @return a slip at the start of that route
     */
    public static RoutingSlip startOf(String... steps) {
        return startOf(Arrays.asList(steps));
    }

    @Override
    public String toString() {
        return "RoutingSlip{" + String.join(",", steps) + " at " + position + ", run=" + runId + "}";
    }
}
