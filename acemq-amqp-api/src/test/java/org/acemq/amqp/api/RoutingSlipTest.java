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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("a routing slip")
class RoutingSlipTest {

    @Nested
    @DisplayName("travelling")
    class Travelling {

        @Test
        void starts_at_the_first_step() {
            RoutingSlip slip = RoutingSlip.startOf("validate", "enrich", "dispatch");

            assertThat(slip.position()).isZero();
            assertThat(slip.current()).contains("validate");
            assertThat(slip.next()).contains("enrich");
            assertThat(slip.isFinished()).isFalse();
        }

        @Test
        void advances_one_step_at_a_time_and_then_finishes() {
            RoutingSlip slip = RoutingSlip.startOf("validate", "enrich");

            RoutingSlip second = slip.advance();
            assertThat(second.current()).contains("enrich");
            assertThat(second.next()).isEmpty();

            RoutingSlip past = second.advance();
            assertThat(past.isFinished()).isTrue();
            assertThat(past.current()).isEmpty();
        }

        @Test
        void keeps_the_same_run_identifier_across_every_hop() {
            RoutingSlip slip = RoutingSlip.startOf("a", "b", "c");

            // The run identifier is what ties three hops together in a trace, and what still
            // identifies the run after a dead-letter and a replay days later.
            assertThat(slip.advance().runId()).isEqualTo(slip.runId());
            assertThat(slip.advance().advance().runId()).isEqualTo(slip.runId());
        }

        @Test
        void two_runs_are_told_apart() {
            assertThat(RoutingSlip.startOf("a").runId()).isNotEqualTo(RoutingSlip.startOf("a").runId());
        }

        @Test
        void a_route_needs_somewhere_to_go() {
            assertThatThrownBy(() -> RoutingSlip.startOf(Collections.emptyList()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("at least one step");
            assertThatThrownBy(() -> RoutingSlip.startOf((java.util.List<String>) null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("on the wire")
    class OnTheWire {

        @Test
        void writes_headers_a_person_can_read_in_a_console() {
            Map<String, Object> headers = RoutingSlip.startOf("validate", "enrich", "dispatch")
                    .advance()
                    .toHeaders();

            // Names rather than numbers, so an operator looking at a stuck message can see
            // where it is without decoding anything.
            assertThat(headers.get(RoutingSlip.ROUTE)).isEqualTo("validate,enrich,dispatch");
            assertThat(headers.get(RoutingSlip.POSITION)).isEqualTo(1);
            assertThat(headers.get(RoutingSlip.RUN_ID)).isNotNull();
        }

        @Test
        void reads_back_exactly_what_it_wrote() {
            RoutingSlip original = RoutingSlip.startOf("validate", "enrich").advance();

            RoutingSlip read = RoutingSlip.from(original.toHeaders()).orElseThrow(AssertionError::new);

            assertThat(read.steps()).containsExactly("validate", "enrich");
            assertThat(read.position()).isEqualTo(1);
            assertThat(read.runId()).isEqualTo(original.runId());
            assertThat(read.current()).contains("enrich");
        }

        @Test
        void a_message_with_no_slip_is_not_travelling_a_route() {
            assertThat(RoutingSlip.from(Collections.emptyMap())).isEmpty();

            Map<String, Object> blank = new LinkedHashMap<>();
            blank.put(RoutingSlip.ROUTE, "  ,  ");
            assertThat(RoutingSlip.from(blank)).isEmpty();
        }

        @Test
        void tolerates_a_position_written_as_text() {
            // Header types are not guaranteed across brokers and languages; a Go publisher may
            // well write the position as a string.
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put(RoutingSlip.ROUTE, "a,b,c");
            headers.put(RoutingSlip.POSITION, "2");

            assertThat(RoutingSlip.from(headers).orElseThrow(AssertionError::new).current()).contains("c");
        }

        @Test
        void starts_over_rather_than_guessing_when_the_position_is_unreadable() {
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put(RoutingSlip.ROUTE, "a,b,c");
            headers.put(RoutingSlip.POSITION, "not a number");

            // Sending the message to an arbitrary step would be worse than repeating the route
            // from its beginning, which is at least a defined outcome.
            assertThat(RoutingSlip.from(headers).orElseThrow(AssertionError::new).position()).isZero();
        }

        @Test
        void gives_a_slip_with_no_run_identifier_a_new_one() {
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put(RoutingSlip.ROUTE, "a,b");
            headers.put(RoutingSlip.POSITION, 0);

            assertThat(RoutingSlip.from(headers).orElseThrow(AssertionError::new).runId()).isNotBlank();
        }

        @Test
        void ignores_whitespace_around_step_names() {
            Map<String, Object> headers = new LinkedHashMap<>();
            headers.put(RoutingSlip.ROUTE, " validate , enrich ,dispatch ");

            assertThat(RoutingSlip.from(headers).orElseThrow(AssertionError::new).steps())
                    .containsExactly("validate", "enrich", "dispatch");
        }
    }

    @Nested
    @DisplayName("being replayed")
    class Replayed {

        @Test
        void can_be_positioned_at_a_step_directly() {
            RoutingSlip slip = RoutingSlip.startOf("validate", "enrich", "dispatch").advanceTo(2);

            // A message replayed straight into the third queue resumes from there rather than
            // repeating the two steps that already succeeded.
            assertThat(slip.current()).contains("dispatch");
            assertThat(slip.next()).isEmpty();
        }

        @Test
        void says_what_it_is_in_its_own_words() {
            assertThat(RoutingSlip.startOf("a", "b").advance().toString())
                    .contains("a,b")
                    .contains("at 1");
        }
    }
}
