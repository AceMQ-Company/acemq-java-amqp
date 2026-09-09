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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.AceHeaders;
import org.jspecify.annotations.Nullable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * An itinerary a message carries with it, naming every stop rather than every step.
 *
 * <p>The second of the two routing slips this library speaks, and the one for when there is no
 * declaration to resolve against. {@link org.acemq.amqp.api.RoutingSlip} writes step
 * <em>names</em> — {@code validate,enrich,dispatch} — which a {@link Pipeline} turns into
 * exchanges and routing keys because it knows what those steps are. That is the better model
 * when the pipeline is known up front: the wire stays readable in a management console, and a
 * route is one short header rather than a document.
 *
 * <p>It is no model at all when the itinerary is decided per message and the consumer has never
 * heard of it. This one carries the exchange and the routing key of every stop, so a service
 * that knows nothing but how to read JSON can send the message onwards:
 *
 * <pre>{@code
 * Itinerary itinerary = Itinerary.empty()
 *         .then("orders-events", "order.validate", "validate")
 *         .then("orders-events", "order.charge", "charge")
 *         .then("orders-events", "order.ship", "ship");
 * }</pre>
 *
 * <p>The shape on the wire is the one Go, Python and Ruby write, field for field, because those
 * three agreed on it first and a slip is worth nothing if only one library can read it:
 *
 * <pre>{@code
 * {"steps":[{"exchange":"orders-events","routingKey":"order.charge","name":"charge"}],
 *  "done":[{"exchange":"orders-events","routingKey":"order.validate","name":"validate",
 *           "completedAt":"2026-09-09T14:03:11Z"}]}
 * }</pre>
 *
 * <p>{@code done} is what has already happened, oldest first, so a slip that fails halfway says
 * how far it got. It is omitted entirely while it is empty, which is what the Go and Ruby
 * libraries do and what the Python one reads.
 *
 * <p>Immutable: {@link #then} and {@link #advance} return new instances. The Go and Ruby
 * libraries mutate while a slip is being assembled and copy on advance; this one copies at both
 * ends, because an itinerary held in a field and sent twice must not have been changed by the
 * first send.
 */
public final class Itinerary {

    /** The header the itinerary travels in: {@code acemq-routing-slip}. */
    public static final String HEADER = AceHeaders.ROUTING_SLIP;

    /**
     * Second precision, in UTC, with a {@code Z}.
     *
     * <p>What Go's {@code time.RFC3339} and Ruby's {@code %Y-%m-%dT%H:%M:%SZ} produce. Python
     * writes microseconds and an explicit {@code +00:00} offset; all four read all of it,
     * because the field is a timestamp for a human reading a stuck message rather than
     * something anything computes with.
     */
    private static final DateTimeFormatter COMPLETED_AT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneOffset.UTC);

    private static final ObjectMapper JSON = new ObjectMapper();

    private final List<Stop> steps;
    private final List<Stop> done;

    private Itinerary(List<Stop> steps, List<Stop> done) {
        this.steps = Collections.unmodifiableList(new ArrayList<>(steps));
        this.done = Collections.unmodifiableList(new ArrayList<>(done));
    }

    /** @return an itinerary with no stops on it yet */
    public static Itinerary empty() {
        return new Itinerary(Collections.emptyList(), Collections.emptyList());
    }

    /**
     * @param stops the stops, in order
     * @return an itinerary of those stops, with nothing done yet
     */
    public static Itinerary of(Stop... stops) {
        return new Itinerary(Arrays.asList(stops), Collections.emptyList());
    }

    /**
     * Adds a stop to the end.
     *
     * @param exchange where that stop's message goes, empty for the default exchange
     * @param routingKey the routing key, which is the queue name when publishing without an
     *     exchange
     * @return a new itinerary with the stop appended
     */
    public Itinerary then(String exchange, String routingKey) {
        return then(exchange, routingKey, "");
    }

    /**
     * @param exchange where that stop's message goes
     * @param routingKey the routing key
     * @param name what to call it in a log, which need not be anything
     * @return a new itinerary with the stop appended
     */
    public Itinerary then(String exchange, String routingKey, String name) {
        List<Stop> extended = new ArrayList<>(steps);
        extended.add(new Stop(exchange, routingKey, name, ""));
        return new Itinerary(extended, done);
    }

    /** @return the stop this message is going to, or empty when the itinerary is finished */
    public Optional<Stop> next() {
        return steps.isEmpty() ? Optional.empty() : Optional.of(steps.get(0));
    }

    /**
     * @return an itinerary with the first stop moved to {@link #done}, stamped with the clock
     */
    public Itinerary advance() {
        if (steps.isEmpty()) {
            return this;
        }
        List<Stop> remaining = new ArrayList<>(steps.subList(1, steps.size()));
        List<Stop> finished = new ArrayList<>(done);
        finished.add(steps.get(0).completedAt(COMPLETED_AT.format(Instant.now())));
        return new Itinerary(remaining, finished);
    }

    /** @return whether every stop has been made */
    public boolean isFinished() {
        return steps.isEmpty();
    }

    /** @return the stops still to make, in order */
    public List<Stop> steps() {
        return steps;
    }

    /** @return the stops already made, oldest first */
    public List<Stop> done() {
        return done;
    }

    /**
     * Reads an itinerary off a message's headers.
     *
     * @param headers the headers as the handler was given them
     * @return the itinerary, or empty when the message is not carrying one
     * @throws AceFatalException when the header is there and cannot be read. Fatal rather than
     *     retryable, and deliberately so: a slip that will not parse will not parse on the next
     *     attempt either, and a message going round the broker while nothing can tell where it
     *     is meant to go is the worst of both outcomes.
     */
    public static Optional<Itinerary> from(Map<String, Object> headers) {
        Object raw = headers == null ? null : headers.get(HEADER);
        if (raw == null) {
            return Optional.empty();
        }
        return Optional.of(parse(raw.toString()));
    }

    /**
     * @param json the itinerary as it travels
     * @return the itinerary
     * @throws AceFatalException when the text is not an itinerary
     */
    public static Itinerary parse(String json) {
        JsonNode root;
        try {
            root = JSON.readTree(json);
        } catch (Exception e) {
            throw new AceFatalException("cannot read the routing slip on this message: " + e.getMessage(), e);
        }
        if (root == null || !root.isObject()) {
            throw new AceFatalException("a routing slip has to be a JSON object, and this one is: " + json);
        }
        return new Itinerary(stops(root.get("steps")), stops(root.get("done")));
    }

    private static List<Stop> stops(@Nullable JsonNode array) {
        List<Stop> read = new ArrayList<>();
        if (array == null || array.isNull()) {
            return read;
        }
        if (!array.isArray()) {
            throw new AceFatalException("the steps of a routing slip have to be a JSON array, and these are: "
                    + array);
        }
        for (JsonNode element : array) {
            read.add(new Stop(
                    text(element, "exchange"),
                    text(element, "routingKey"),
                    text(element, "name"),
                    text(element, "completedAt")));
        }
        return read;
    }

    private static String text(JsonNode step, String field) {
        JsonNode value = step.get(field);
        return value == null || value.isNull() ? "" : value.asText();
    }

    /**
     * @return the itinerary as it goes on the wire, to publish under {@link #HEADER}
     */
    public String toHeader() {
        Map<String, Object> written = new LinkedHashMap<>();
        written.put("steps", wire(steps));
        if (!done.isEmpty()) {
            // Omitted while empty, which is what Go and Ruby write. A slip that has been
            // nowhere yet says so by not having the field, rather than by having an empty one.
            written.put("done", wire(done));
        }
        try {
            return JSON.writeValueAsString(written);
        } catch (Exception e) {
            // The map holds strings and lists of strings, so this cannot happen; it is not
            // swallowed, because a slip that silently did not go out would strand the message.
            throw new AceFatalException("cannot write the routing slip: " + e.getMessage(), e);
        }
    }

    private static List<Map<String, Object>> wire(List<Stop> stops) {
        List<Map<String, Object>> written = new ArrayList<>(stops.size());
        for (Stop stop : stops) {
            written.add(stop.toWire());
        }
        return written;
    }

    @Override
    public boolean equals(@Nullable Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Itinerary)) {
            return false;
        }
        Itinerary that = (Itinerary) other;
        return steps.equals(that.steps) && done.equals(that.done);
    }

    @Override
    public int hashCode() {
        return Objects.hash(steps, done);
    }

    @Override
    public String toString() {
        return "Itinerary[done: " + join(done) + " | next: " + join(steps) + "]";
    }

    private static String join(List<Stop> stops) {
        List<String> rendered = new ArrayList<>(stops.size());
        for (Stop stop : stops) {
            rendered.add(stop.toString());
        }
        return String.join(" -> ", rendered);
    }

    /** One stop on an itinerary. */
    public static final class Stop {

        private final String exchange;
        private final String routingKey;
        private final String name;
        private final String completedAt;

        Stop(@Nullable String exchange, @Nullable String routingKey, @Nullable String name,
                @Nullable String completedAt) {
            this.exchange = exchange == null ? "" : exchange;
            this.routingKey = routingKey == null ? "" : routingKey;
            this.name = name == null ? "" : name;
            this.completedAt = completedAt == null ? "" : completedAt;
        }

        /**
         * @param exchange where this stop's message goes
         * @param routingKey the routing key
         * @return a stop, to build an itinerary out of
         */
        public static Stop at(String exchange, String routingKey) {
            return new Stop(exchange, routingKey, "", "");
        }

        /**
         * @param exchange where this stop's message goes
         * @param routingKey the routing key
         * @param name what to call it in a log
         * @return a named stop
         */
        public static Stop at(String exchange, String routingKey, String name) {
            return new Stop(exchange, routingKey, name, "");
        }

        /** @return the exchange, empty for the default one */
        public String exchange() {
            return exchange;
        }

        /** @return the routing key */
        public String routingKey() {
            return routingKey;
        }

        /** @return the name, or empty when the stop was never given one */
        public String name() {
            return name;
        }

        /** @return when this stop was completed, or empty while it has not been */
        public String completedAt() {
            return completedAt;
        }

        Stop completedAt(String at) {
            return new Stop(exchange, routingKey, name, at);
        }

        Map<String, Object> toWire() {
            // Insertion order is the order the other three write, and it is kept so that a slip
            // written here and a slip written in Go are the same bytes rather than merely the
            // same meaning.
            Map<String, Object> written = new LinkedHashMap<>();
            written.put("exchange", exchange);
            written.put("routingKey", routingKey);
            if (!name.isEmpty()) {
                written.put("name", name);
            }
            if (!completedAt.isEmpty()) {
                written.put("completedAt", completedAt);
            }
            return written;
        }

        @Override
        public boolean equals(@Nullable Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Stop)) {
                return false;
            }
            Stop that = (Stop) other;
            return exchange.equals(that.exchange)
                    && routingKey.equals(that.routingKey)
                    && name.equals(that.name)
                    && completedAt.equals(that.completedAt);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exchange, routingKey, name, completedAt);
        }

        @Override
        public String toString() {
            return name.isEmpty() ? exchange + "/" + routingKey : name;
        }
    }
}
