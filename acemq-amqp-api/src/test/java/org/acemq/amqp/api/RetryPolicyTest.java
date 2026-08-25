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

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

    @Nested
    @DisplayName("schedules")
    class Schedules {

        @Test
        void exponential_grows_by_the_multiplier_and_stops_at_the_ceiling() {
            RetryPolicy policy = RetryPolicy.exponential(6, Duration.ofSeconds(1), 5.0, Duration.ofMinutes(1))
                    .withJitter(0);

            assertThat(policy.schedule())
                    .containsExactly(
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(5),
                            Duration.ofSeconds(25),
                            // 125s would exceed the one-minute ceiling, so it is clamped, and
                            // stays clamped for every later attempt.
                            Duration.ofMinutes(1),
                            Duration.ofMinutes(1));
        }

        @Test
        void a_schedule_has_one_entry_fewer_than_the_attempt_count() {
            // Five attempts means four waits: nothing is waited before the first delivery.
            assertThat(RetryPolicy.exponential(5, Duration.ofSeconds(1), Duration.ofMinutes(5))
                    .schedule())
                    .hasSize(4);
        }

        @Test
        void fixed_repeats_the_same_delay() {
            assertThat(RetryPolicy.fixed(4, Duration.ofSeconds(10)).schedule())
                    .containsExactly(Duration.ofSeconds(10), Duration.ofSeconds(10), Duration.ofSeconds(10));
        }

        @Test
        void none_never_retries() {
            RetryPolicy policy = RetryPolicy.none();

            assertThat(policy.maxAttempts()).isEqualTo(1);
            assertThat(policy.schedule()).isEmpty();
            assertThat(policy.nextDelay(1, Duration.ZERO)).isEmpty();
        }
    }

    @Nested
    @DisplayName("when to stop")
    class WhenToStop {

        @Test
        void gives_up_once_the_attempts_are_used() {
            RetryPolicy policy = RetryPolicy.exponential(3, Duration.ofSeconds(1), Duration.ofMinutes(1)).withJitter(0);

            assertThat(policy.nextDelay(1, Duration.ZERO)).contains(Duration.ofSeconds(1));
            assertThat(policy.nextDelay(2, Duration.ZERO)).contains(Duration.ofSeconds(5));
            assertThat(policy.nextDelay(3, Duration.ZERO)).isEmpty();
        }

        @Test
        void gives_up_on_a_message_that_has_grown_too_old_even_with_attempts_left() {
            RetryPolicy policy = RetryPolicy.exponential(10, Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .giveUpAfter(Duration.ofMinutes(30));

            assertThat(policy.nextDelay(2, Duration.ofMinutes(29))).isPresent();
            // Nine attempts remain, but the message is older than the limit. Without this,
            // an outage produces messages that keep circulating long after anyone cares.
            assertThat(policy.nextDelay(2, Duration.ofMinutes(31))).isEmpty();
        }

        @Test
        void treats_an_unknown_age_as_young_enough() {
            RetryPolicy policy = RetryPolicy.exponential(3, Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .giveUpAfter(Duration.ofSeconds(1));

            assertThat(policy.nextDelay(1, null)).isPresent();
        }
    }

    @Nested
    @DisplayName("jitter")
    class Jitter {

        @Test
        void spreads_a_delay_without_wandering_far_from_it() {
            RetryPolicy policy = RetryPolicy.exponential(5, Duration.ofSeconds(10), Duration.ofMinutes(5))
                    .withJitter(0.10);

            for (int i = 0; i < 200; i++) {
                Duration delay = policy.nextDelay(1, Duration.ZERO).orElseThrow(AssertionError::new);
                assertThat(delay).isBetween(Duration.ofSeconds(9), Duration.ofSeconds(11));
            }
        }

        @Test
        void is_on_by_default_for_exponential_policies() {
            // A thousand messages failing at once must not all retry on the same tick.
            assertThat(RetryPolicy.exponential(3, Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .jitterFactor())
                    .isGreaterThan(0.0);
        }

        @Test
        void never_produces_a_delay_of_zero() {
            RetryPolicy policy = RetryPolicy.fixed(5, Duration.ofMillis(1)).withJitter(1.0);

            for (int i = 0; i < 200; i++) {
                assertThat(policy.nextDelay(1, Duration.ZERO).orElseThrow(AssertionError::new))
                        .isGreaterThanOrEqualTo(Duration.ofMillis(1));
            }
        }
    }

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        void rejects_fewer_than_one_attempt_and_explains_the_alternative() {
            assertThatThrownBy(() -> RetryPolicy.fixed(0, Duration.ofSeconds(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("RetryPolicy.none()");
        }

        @Test
        void rejects_a_multiplier_below_one() {
            assertThatThrownBy(
                    () -> RetryPolicy.exponential(3, Duration.ofSeconds(1), 0.5, Duration.ofMinutes(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("multiplier");
        }

        @Test
        void rejects_a_non_positive_initial_delay() {
            assertThatThrownBy(() -> RetryPolicy.exponential(3, Duration.ZERO, Duration.ofMinutes(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("initialDelay");
        }

        @Test
        void rejects_jitter_outside_zero_to_one() {
            assertThatThrownBy(() -> RetryPolicy.none().withJitter(1.5))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("jitterFactor");
        }

        @Test
        void rejects_an_attempt_number_below_one() {
            assertThatThrownBy(() -> RetryPolicy.none().nextDelay(0, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
