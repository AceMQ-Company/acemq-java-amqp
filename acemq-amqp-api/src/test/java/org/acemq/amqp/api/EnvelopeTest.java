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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class EnvelopeTest {

    @Nested
    @DisplayName("defaults")
    class Defaults {

        @Test
        void generates_an_identifier_when_none_is_given() {
            Envelope one = Envelope.of("order.placed").build();
            Envelope two = Envelope.of("order.placed").build();

            assertThat(one.id()).isNotBlank();
            assertThat(one.id()).isNotEqualTo(two.id());
        }

        @Test
        void correlates_to_itself_when_no_correlation_is_given() {
            Envelope envelope = Envelope.of("order.placed").build();

            assertThat(envelope.correlationId()).isEqualTo(envelope.id());
        }

        @Test
        void starts_at_version_one_and_attempt_one() {
            Envelope envelope = Envelope.of("order.placed").build();

            assertThat(envelope.version()).isEqualTo(1);
            assertThat(envelope.attempt()).isEqualTo(1);
        }

        @Test
        void has_no_causation_or_origin_by_default() {
            Envelope envelope = Envelope.of("order.placed").build();

            assertThat(envelope.causationId()).isEmpty();
            assertThat(envelope.origin()).isEmpty();
        }
    }

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        void rejects_a_missing_type() {
            assertThatThrownBy(() -> new Envelope.Builder().build())
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("type");
        }

        @Test
        void rejects_a_version_below_one() {
            assertThatThrownBy(() -> Envelope.of("order.placed").version(0).build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("version");
        }

        @Test
        void rejects_an_attempt_below_one() {
            assertThatThrownBy(() -> Envelope.of("order.placed").attempt(0).build())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("attempt");
        }

        @Test
        void refuses_to_let_callers_set_acemq_owned_headers() {
            assertThatThrownBy(() -> Envelope.of("order.placed").header(AceHeaders.ATTEMPT, 7))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("owned by AceMQ");
        }

        @Test
        void silently_drops_acemq_owned_headers_supplied_in_bulk() {
            Map<String, Object> incoming = new HashMap<>();
            incoming.put("tenant", "acme");
            incoming.put(AceHeaders.ATTEMPT, 7);

            Envelope envelope = Envelope.of("order.placed").headers(incoming).build();

            assertThat(envelope.headers()).containsOnlyKeys("tenant");
        }
    }

    @Nested
    @DisplayName("immutability")
    class Immutability {

        @Test
        void exposes_headers_as_an_unmodifiable_map() {
            Envelope envelope = Envelope.of("order.placed").header("tenant", "acme").build();

            assertThatThrownBy(() -> envelope.headers().put("other", "value"))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void is_not_affected_by_later_changes_to_the_source_map() {
            Map<String, Object> incoming = new HashMap<>();
            incoming.put("tenant", "acme");

            Envelope envelope = Envelope.of("order.placed").headers(incoming).build();
            incoming.put("added.later", "value");

            assertThat(envelope.headers()).containsOnlyKeys("tenant");
        }
    }

    @Nested
    @DisplayName("retry accounting")
    class RetryAccounting {

        @Test
        void next_attempt_increments_the_counter_and_keeps_identity() {
            Envelope first = Envelope.of("order.placed")
                    .correlationId("flow-1")
                    .header("tenant", "acme")
                    .build();

            Envelope second = first.nextAttempt();

            assertThat(second.attempt()).isEqualTo(2);
            assertThat(second.id()).isEqualTo(first.id());
            assertThat(second.correlationId()).isEqualTo("flow-1");
            assertThat(second.headers()).containsEntry("tenant", "acme");
        }

        @Test
        void next_attempt_measures_age_from_the_original_publish() {
            Instant published = Instant.now().minus(30, ChronoUnit.MINUTES);
            Envelope first = Envelope.of("order.placed").firstSeen(published).build();

            Envelope third = first.nextAttempt().nextAttempt();

            assertThat(third.firstSeen()).isEqualTo(published);
            assertThat(third.age()).isGreaterThan(java.time.Duration.ofMinutes(29));
        }
    }

    @Nested
    @DisplayName("causation")
    class Causation {

        @Test
        void a_caused_message_carries_the_flow_and_records_its_parent() {
            Envelope cause = Envelope.of("order.placed")
                    .correlationId("flow-1")
                    .origin("orders@host-1")
                    .build();

            Envelope effect = cause.causing("inventory.reserve").build();

            assertThat(effect.correlationId()).isEqualTo("flow-1");
            assertThat(effect.causationId()).contains(cause.id());
            assertThat(effect.origin()).contains("orders@host-1");
            assertThat(effect.id()).isNotEqualTo(cause.id());
        }

        @Test
        void a_caused_message_starts_its_own_attempt_count() {
            Envelope cause = Envelope.of("order.placed").attempt(4).build();

            Envelope effect = cause.causing("inventory.reserve").build();

            assertThat(effect.attempt()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("value semantics")
    class ValueSemantics {

        @Test
        void envelopes_with_the_same_values_are_equal() {
            Instant when = Instant.now();
            Envelope one = Envelope.of("order.placed").id("m-1").firstSeen(when).build();
            Envelope two = Envelope.of("order.placed").id("m-1").firstSeen(when).build();

            assertThat(one).isEqualTo(two).hasSameHashCodeAs(two);
        }

        @Test
        void to_builder_round_trips_every_field() {
            Envelope original = Envelope.of("order.placed")
                    .id("m-1")
                    .version(3)
                    .correlationId("flow-1")
                    .causationId("m-0")
                    .attempt(2)
                    .origin("orders@host-1")
                    .header("tenant", "acme")
                    .build();

            assertThat(original.toBuilder().build()).isEqualTo(original);
        }
    }
}
