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
        void the_schedule_doubles_and_is_the_same_in_every_language() {
            // The same four numbers Go, .NET, Python and Ruby produce for this policy. A message
            // retried by a Java consumer and then by a Go one must not wait different amounts
            // for the same attempt, so these are a contract rather than a preference.
            assertThat(RetryPolicy.exponential(5, Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .schedule())
                    .containsExactly(
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(2),
                            Duration.ofSeconds(4),
                            Duration.ofSeconds(8));
        }

        @Test
        void the_ceiling_holds_for_every_attempt_after_it_is_reached() {
            assertThat(RetryPolicy.exponential(6, Duration.ofSeconds(1), Duration.ofSeconds(4))
                    .schedule())
                    .containsExactly(
                            Duration.ofSeconds(1),
                            Duration.ofSeconds(2),
                            Duration.ofSeconds(4),
                            Duration.ofSeconds(4),
                            Duration.ofSeconds(4));
        }

        @Test
        void exponential_grows_by_an_explicit_multiplier_and_stops_at_the_ceiling() {
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
            assertThat(policy.brokerRungs()).isEmpty();
        }
    }

    @Nested
    @DisplayName("where the waiting happens")
    class WhereTheWaitingHappens {

        @Test
        void a_short_wait_is_spent_in_the_consumer() {
            // Below the threshold the seconds a restart loses are only seconds, and a held
            // prefetch slot is cheaper than a queue nobody asked for.
            RetryPolicy.Wait wait = RetryPolicy.fixed(2, Duration.ofSeconds(5))
                    .nextWait(1, Duration.ZERO)
                    .orElseThrow(AssertionError::new);

            assertThat(wait.isInBroker()).isFalse();
        }

        @Test
        void a_long_wait_is_spent_in_the_broker() {
            // A consumer sleeping on a five-minute backoff loses the whole wait when it
            // restarts: the broker redelivers the unacknowledged message at once, so a
            // five-minute policy delivers in none.
            RetryPolicy.Wait wait = RetryPolicy.fixed(2, Duration.ofMinutes(5))
                    .nextWait(1, Duration.ZERO)
                    .orElseThrow(AssertionError::new);

            assertThat(wait.isInBroker()).isTrue();
            assertThat(wait.delay()).isEqualTo(Duration.ofMinutes(5));
        }

        @Test
        void the_threshold_is_reached_rather_than_passed() {
            assertThat(RetryPolicy.fixed(2, Duration.ofSeconds(30))
                    .nextWait(1, Duration.ZERO)
                    .orElseThrow(AssertionError::new)
                    .isInBroker())
                    .isTrue();
            assertThat(RetryPolicy.fixed(2, Duration.ofSeconds(29))
                    .nextWait(1, Duration.ZERO)
                    .orElseThrow(AssertionError::new)
                    .isInBroker())
                    .isFalse();
        }

        @Test
        void thirty_seconds_is_the_default_everywhere() {
            assertThat(RetryPolicy.DEFAULT_BROKER_WAIT_THRESHOLD).isEqualTo(Duration.ofSeconds(30));
            assertThat(RetryPolicy.fixed(2, Duration.ofSeconds(1)).brokerWaitThreshold())
                    .isEqualTo(Duration.ofSeconds(30));
            assertThat(RetryPolicy.exponential(2, Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .brokerWaitThreshold())
                    .isEqualTo(Duration.ofSeconds(30));
        }

        @Test
        void a_threshold_of_zero_keeps_every_wait_in_the_consumer() {
            // The way out for a service that may not declare queues on its broker: nothing is
            // long enough to reach one.
            RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofMinutes(5)).waitInBrokerFrom(Duration.ZERO);
            RetryPolicy.Wait wait = policy.nextWait(1, Duration.ZERO).orElseThrow(AssertionError::new);

            assertThat(wait.isInBroker()).isFalse();
            assertThat(wait.delay()).isEqualTo(Duration.ofMinutes(5));
            assertThat(policy.brokerRungs()).isEmpty();
        }

        @Test
        void the_rungs_are_the_schedule_above_the_threshold_without_repeats() {
            // Finite, because the schedule is. That is what lets the queues be declared with the
            // topology rather than conjured when a consumer first fails.
            RetryPolicy policy = RetryPolicy.exponential(6, Duration.ofSeconds(10), Duration.ofMinutes(10));

            assertThat(policy.schedule())
                    .containsExactly(
                            Duration.ofSeconds(10),
                            Duration.ofSeconds(20),
                            Duration.ofSeconds(40),
                            Duration.ofSeconds(80),
                            Duration.ofSeconds(160));
            assertThat(policy.brokerRungs())
                    .containsExactly(Duration.ofSeconds(40), Duration.ofSeconds(80), Duration.ofSeconds(160));
        }

        @Test
        void a_fixed_policy_that_waits_a_minute_three_times_needs_one_queue_not_three() {
            assertThat(RetryPolicy.fixed(4, Duration.ofMinutes(1)).brokerRungs())
                    .containsExactly(Duration.ofMinutes(1));
        }
    }

    @Nested
    @DisplayName("when to stop")
    class WhenToStop {

        @Test
        void gives_up_once_the_attempts_are_used() {
            RetryPolicy policy = RetryPolicy.exponential(3, Duration.ofSeconds(1), Duration.ofMinutes(1)).withJitter(0);

            assertThat(policy.nextDelay(1, Duration.ZERO)).contains(Duration.ofSeconds(1));
            assertThat(policy.nextDelay(2, Duration.ZERO)).contains(Duration.ofSeconds(2));
            // The third delivery is the last one. Asking for a fourth is how a message is
            // retried for ever by a library that counts wrong.
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
        void moves_both_ways_and_stays_inside_the_factor() {
            RetryPolicy policy = RetryPolicy.exponential(2, Duration.ofSeconds(10), Duration.ofMinutes(5));

            boolean shorter = false;
            boolean longer = false;
            for (int i = 0; i < 200; i++) {
                Duration delay = policy.nextDelay(1, Duration.ZERO).orElseThrow(AssertionError::new);
                // Twenty percent either side of ten seconds, and genuinely either side: jitter
                // that only ever delays turns a thundering herd into a slower thundering herd.
                assertThat(delay).isBetween(Duration.ofSeconds(8), Duration.ofSeconds(12));
                shorter |= delay.compareTo(Duration.ofSeconds(10)) < 0;
                longer |= delay.compareTo(Duration.ofSeconds(10)) > 0;
            }
            assertThat(shorter).isTrue();
            assertThat(longer).isTrue();
        }

        @Test
        void is_twenty_percent_by_default_for_exponential_policies() {
            // A thousand messages failing at once must not all retry on the same tick, and the
            // amount they are spread by is the same in every language.
            assertThat(RetryPolicy.exponential(3, Duration.ofSeconds(1), Duration.ofMinutes(1))
                    .jitterFactor())
                    .isEqualTo(0.20);
        }

        @Test
        void is_off_for_a_fixed_policy() {
            // "Every thirty seconds" is an exact statement, and a library that quietly returned
            // twenty-six would be answering a question nobody asked.
            RetryPolicy policy = RetryPolicy.fixed(3, Duration.ofSeconds(10));

            assertThat(policy.jitterFactor()).isEqualTo(0.0);
            for (int i = 0; i < 50; i++) {
                assertThat(policy.nextDelay(1, Duration.ZERO)).contains(Duration.ofSeconds(10));
            }
        }

        @Test
        void is_never_applied_to_a_wait_the_broker_holds() {
            // A rung queue's time-to-live is fixed at declaration, so a jittered delay would
            // name a queue that does not exist.
            RetryPolicy policy = RetryPolicy.exponential(3, Duration.ofMinutes(5), Duration.ofHours(1));

            for (int i = 0; i < 50; i++) {
                RetryPolicy.Wait wait = policy.nextWait(1, Duration.ZERO).orElseThrow(AssertionError::new);
                assertThat(wait.isInBroker()).isTrue();
                assertThat(wait.delay()).isEqualTo(Duration.ofMinutes(5));
            }
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

        @Test
        void rejects_a_negative_broker_wait_threshold() {
            assertThatThrownBy(() -> RetryPolicy.none().waitInBrokerFrom(Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("threshold");
        }
    }
}
