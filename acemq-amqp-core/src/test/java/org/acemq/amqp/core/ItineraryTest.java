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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.acemq.amqp.api.AceFatalException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The routing slip that travels with the message, in the shape three other libraries write.
 *
 * <p>The literals in this file are not invented. They were produced by running the Go and Ruby
 * libraries against the same three stops and copying what came out, which is the only way this
 * test means anything: a shape agreed by reading the source and then written down here would
 * agree with the reading rather than with the software.
 *
 * <pre>
 * go:   patterns.NewRoutingSlip().Then("orders-events", "order.validate", "validate")...
 * ruby: Patterns::RoutingSlip.new.step("orders-events", "order.validate", name: "validate")...
 * </pre>
 *
 * <p>Python writes the same document with spaces after its separators and an empty {@code done}
 * where the other two omit it. That is one JSON document written by two encoders rather than two
 * formats, and the parse cases below cover it.
 */
@DisplayName("an itinerary")
class ItineraryTest {

    /** What Go and Ruby both print for three named stops, byte for byte. */
    private static final String FRESH = "{\"steps\":["
            + "{\"exchange\":\"orders-events\",\"routingKey\":\"order.validate\",\"name\":\"validate\"},"
            + "{\"exchange\":\"orders-events\",\"routingKey\":\"order.charge\",\"name\":\"charge\"},"
            + "{\"exchange\":\"orders-events\",\"routingKey\":\"order.ship\",\"name\":\"ship\"}]}";

    /** The same slip after Go's {@code Advance()}, with the first stop moved to {@code done}. */
    private static final String ADVANCED = "{\"steps\":["
            + "{\"exchange\":\"orders-events\",\"routingKey\":\"order.charge\",\"name\":\"charge\"},"
            + "{\"exchange\":\"orders-events\",\"routingKey\":\"order.ship\",\"name\":\"ship\"}],"
            + "\"done\":[{\"exchange\":\"orders-events\",\"routingKey\":\"order.validate\","
            + "\"name\":\"validate\",\"completedAt\":\"2026-09-09T16:18:09Z\"}]}";

    /** Python's encoder: spaces after the separators, and {@code done} present but empty. */
    private static final String PYTHON_FRESH = "{\"steps\": ["
            + "{\"exchange\": \"orders-events\", \"routingKey\": \"order.validate\", \"name\": \"validate\"}, "
            + "{\"exchange\": \"orders-events\", \"routingKey\": \"order.charge\", \"name\": \"charge\"}, "
            + "{\"exchange\": \"orders-events\", \"routingKey\": \"order.ship\", \"name\": \"ship\"}], "
            + "\"done\": []}";

    private static Itinerary threeStops() {
        return Itinerary.empty()
                .then("orders-events", "order.validate", "validate")
                .then("orders-events", "order.charge", "charge")
                .then("orders-events", "order.ship", "ship");
    }

    @Nested
    @DisplayName("written")
    class Written {

        @Test
        @DisplayName("is byte for byte what Go and Ruby write")
        void matches_the_other_libraries() {
            assertThat(threeStops().toHeader()).isEqualTo(FRESH);
        }

        @Test
        @DisplayName("omits done until there is something in it")
        void omits_an_empty_done() {
            // Go leaves it out through omitempty and Ruby leaves it out by hand. Writing an
            // empty array instead would still parse everywhere, and would still be a different
            // document from the one the other two produce for the same slip.
            assertThat(threeStops().toHeader()).doesNotContain("done");

            assertThat(threeStops().advance().toHeader())
                    .contains("\"done\":[{\"exchange\":\"orders-events\","
                            + "\"routingKey\":\"order.validate\",\"name\":\"validate\",\"completedAt\":\"");
        }

        @Test
        @DisplayName("stamps a completed stop the way Go and Ruby stamp it")
        void stamps_rfc_3339_seconds_in_utc() {
            Itinerary advanced = threeStops().advance();

            assertThat(advanced.done()).hasSize(1);
            assertThat(advanced.done().get(0).completedAt())
                    .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z");
        }

        @Test
        @DisplayName("leaves an unnamed stop unnamed rather than writing an empty name")
        void omits_an_empty_name() {
            assertThat(Itinerary.empty().then("", "orders.new").toHeader())
                    .isEqualTo("{\"steps\":[{\"exchange\":\"\",\"routingKey\":\"orders.new\"}]}");
        }
    }

    @Nested
    @DisplayName("read")
    class Read {

        @Test
        @DisplayName("follows a slip a Go service wrote")
        void reads_the_go_shape() {
            Itinerary slip = Itinerary.parse(FRESH);

            assertThat(slip.isFinished()).isFalse();
            assertThat(slip.next().get().exchange()).isEqualTo("orders-events");
            assertThat(slip.next().get().routingKey()).isEqualTo("order.validate");
            assertThat(slip.next().get().name()).isEqualTo("validate");
            assertThat(slip.steps()).hasSize(3);
            assertThat(slip.done()).isEmpty();

            // Read and written back unchanged, which is what lets a Java hop sit in the middle
            // of a route without the hops on either side noticing.
            assertThat(slip.toHeader()).isEqualTo(FRESH);
        }

        @Test
        @DisplayName("keeps the done list, including when each stop was completed")
        void reads_the_done_list() {
            Itinerary slip = Itinerary.parse(ADVANCED);

            assertThat(slip.done()).hasSize(1);
            assertThat(slip.done().get(0).name()).isEqualTo("validate");
            assertThat(slip.done().get(0).routingKey()).isEqualTo("order.validate");
            assertThat(slip.done().get(0).completedAt()).isEqualTo("2026-09-09T16:18:09Z");
            assertThat(slip.next().get().name()).isEqualTo("charge");

            assertThat(slip.toHeader()).isEqualTo(ADVANCED);
        }

        @Test
        @DisplayName("reads Python's encoder, which spaces its separators and always writes done")
        void reads_the_python_shape() {
            Itinerary slip = Itinerary.parse(PYTHON_FRESH);

            assertThat(slip.steps()).hasSize(3);
            assertThat(slip.done()).isEmpty();
            // Normalised on the way out: the same slip, in the shape Go and Ruby write.
            assertThat(slip.toHeader()).isEqualTo(FRESH);
        }

        @Test
        @DisplayName("comes off a header, and is absent when the message carries none")
        void reads_a_header() {
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put(Itinerary.HEADER, ADVANCED);

            assertThat(Itinerary.from(headers)).isPresent();
            assertThat(Itinerary.from(Collections.emptyMap())).isEmpty();
            assertThat(Itinerary.HEADER).isEqualTo("acemq-routing-slip");
        }

        @Test
        @DisplayName("is fatal when it cannot be read, because it will not read next time either")
        void refuses_a_slip_that_will_not_parse() {
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put(Itinerary.HEADER, "{not json");

            assertThatThrownBy(() -> Itinerary.from(headers))
                    .isInstanceOf(AceFatalException.class)
                    .hasMessageContaining("routing slip");

            assertThatThrownBy(() -> Itinerary.parse("[\"a list is not a slip\"]"))
                    .isInstanceOf(AceFatalException.class);
            assertThatThrownBy(() -> Itinerary.parse("{\"steps\":\"not an array\"}"))
                    .isInstanceOf(AceFatalException.class);
        }

        @Test
        @DisplayName("tolerates a stop with nothing but an exchange and a key")
        void reads_an_unnamed_stop() {
            Itinerary slip = Itinerary.parse("{\"steps\":[{\"exchange\":\"\",\"routingKey\":\"orders.new\"}]}");

            assertThat(slip.next().get().name()).isEmpty();
            assertThat(slip.next().get().completedAt()).isEmpty();
            assertThat(slip.next().get().toString()).isEqualTo("/orders.new");
        }
    }

    @Nested
    @DisplayName("advanced")
    class Advanced {

        @Test
        @DisplayName("moves one stop at a time and finishes when there are none left")
        void walks_the_route() {
            Itinerary slip = threeStops();

            slip = slip.advance();
            assertThat(slip.next().get().name()).isEqualTo("charge");
            slip = slip.advance();
            assertThat(slip.next().get().name()).isEqualTo("ship");
            slip = slip.advance();

            assertThat(slip.isFinished()).isTrue();
            assertThat(slip.next()).isEmpty();
            assertThat(slip.done()).hasSize(3);
            // Advancing past the end is not an error. The last hop asks for the next stop and
            // is told there is none, which is how a run ends.
            assertThat(slip.advance()).isEqualTo(slip);
        }

        @Test
        @DisplayName("leaves the itinerary it was called on alone")
        void does_not_mutate() {
            Itinerary slip = threeStops();
            Itinerary advanced = slip.advance();

            assertThat(slip.steps()).hasSize(3);
            assertThat(slip.done()).isEmpty();
            assertThat(advanced.steps()).hasSize(2);
            assertThat(slip.toHeader()).isEqualTo(FRESH);
        }
    }
}
