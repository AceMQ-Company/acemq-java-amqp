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
package org.acemq.amqp.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.acemq.amqp.api.RetryPolicy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Holds this library to the fixture it writes.
 *
 * <p>Generating a fixture is not the same as obeying one. A generator that reads the same wrong
 * constant as the code produces a file that agrees with the bug perfectly, which is exactly how
 * a multiplier of five survived ten releases. So the expectations here are derived a second way
 * wherever a second way exists: the doubling is checked by asking whether each delay is twice the
 * one before it, not by reading the multiplier back; the queue names are rendered by a second
 * implementation written from the rule rather than called from the first; the threshold is
 * recomputed from the sentence that describes it.
 *
 * <p>Two implementations agreeing is not proof, but two implementations disagreeing is proof of
 * a problem, and that is the half worth having.
 */
@DisplayName("what the contract fixtures say Java must do")
class ContractConformanceTest {

    private static JsonNode fixtures;

    @BeforeAll
    static void load() throws IOException {
        try (InputStream in = ContractConformanceTest.class.getResourceAsStream("/fixtures/contract-fixtures.json")) {
            assertThat(in).as("contract-fixtures.json is on the test classpath").isNotNull();
            fixtures = new ObjectMapper().readTree(in);
        }
        assertThat(fixtures.get("generatedBy").asText()).isEqualTo("acemq-java-amqp ContractFixtures");
    }

    private static JsonNode schedule(String name) {
        for (JsonNode candidate : fixtures.get("retrySchedules")) {
            if (name.equals(candidate.get("name").asText())) {
                return candidate;
            }
        }
        throw new AssertionError("no retry schedule named " + name + " in the fixtures");
    }

    private static List<Long> longs(JsonNode array) {
        List<Long> values = new ArrayList<>();
        for (JsonNode element : array) {
            values.add(element.asLong());
        }
        return values;
    }

    @Nested
    @DisplayName("the retry schedules")
    class Schedules {

        @Test
        @DisplayName("an uncapped exponential policy doubles, and the fixture says so")
        void the_uncapped_exponential_doubles() {
            JsonNode entry = schedule("exponential-uncapped");
            List<Long> delays = longs(entry.get("scheduleMillis"));

            // The assertion the multiplier bug would have failed. Written as a relation between
            // neighbours rather than as a literal list, so it stays true if somebody legitimately
            // changes the initial delay and stays false the moment the growth factor moves.
            assertThat(delays).hasSize(entry.get("maxAttempts").asInt() - 1);
            for (int i = 1; i < delays.size(); i++) {
                assertThat(delays.get(i))
                        .as("delay %d should be twice delay %d", i, i - 1)
                        .isEqualTo(delays.get(i - 1) * 2);
            }

            // And the library still produces exactly that, so the file is not describing a
            // version of the code that no longer exists.
            RetryPolicy live = RetryPolicy.exponential(
                    entry.get("maxAttempts").asInt(),
                    Duration.ofMillis(delays.get(0)),
                    Duration.ofHours(24));
            assertThat(millis(live)).isEqualTo(delays);
        }

        @Test
        @DisplayName("a capped exponential policy doubles up to the ceiling and then stops")
        void the_capped_exponential_stops_at_the_ceiling() {
            List<Long> delays = longs(schedule("exponential-capped").get("scheduleMillis"));
            long ceiling = delays.get(delays.size() - 1);

            assertThat(delays).isNotEmpty();
            for (int i = 1; i < delays.size(); i++) {
                long previous = delays.get(i - 1);
                long expected = Math.min(previous * 2, ceiling);
                assertThat(delays.get(i))
                        .as("delay %d doubles unless the ceiling gets in the way", i)
                        .isEqualTo(expected);
            }
            assertThat(delays).allSatisfy(delay -> assertThat(delay).isLessThanOrEqualTo(ceiling));
            assertThat(delays.get(delays.size() - 1))
                    .as("a schedule long enough to reach the ceiling stays there")
                    .isEqualTo(delays.get(delays.size() - 2));
        }

        @Test
        @DisplayName("a fixed policy waits the same amount every time and does not jitter")
        void the_fixed_policy_repeats_itself() {
            JsonNode entry = schedule("fixed");
            List<Long> delays = longs(entry.get("scheduleMillis"));

            assertThat(delays).hasSize(entry.get("maxAttempts").asInt() - 1);
            assertThat(delays).containsOnly(delays.get(0));
            assertThat(entry.get("jitterFactor").asDouble())
                    .as("a caller who says every thirty seconds has said something exact")
                    .isZero();
        }

        @Test
        @DisplayName("the no-retry policy delivers once and never comes back")
        void the_no_retry_policy_never_retries() {
            JsonNode entry = schedule("none");

            assertThat(entry.get("maxAttempts").asInt()).isOne();
            assertThat(entry.get("scheduleMillis")).isEmpty();
            assertThat(entry.get("brokerRungDelaysMillis")).isEmpty();
            for (JsonNode decision : entry.get("decisions")) {
                assertThat(decision.get("retries").asBoolean())
                        .as("no attempt of a no-retry policy retries")
                        .isFalse();
            }
        }

        @Test
        @DisplayName("an age limit ends the retries even when attempts are left")
        void the_age_limit_ends_the_retries() {
            JsonNode entry = schedule("exponential-give-up-on-age");
            long limit = entry.get("maxMessageAgeMillis").asLong();

            assertThat(limit)
                    .as("the age limit is the point of this policy and must be shorter than a year")
                    .isLessThan(Duration.ofDays(365).toMillis());

            boolean sawYoungAndRetrying = false;
            boolean sawOldAndFinished = false;
            for (JsonNode decision : entry.get("decisions")) {
                long age = decision.get("messageAgeMillis").asLong();
                boolean retries = decision.get("retries").asBoolean();
                if (age >= limit) {
                    assertThat(retries).as("a message at or past the age limit is abandoned").isFalse();
                    sawOldAndFinished = true;
                }
                if (decision.get("attempt").asInt() == 1 && age == 0) {
                    assertThat(retries).as("a young message on its first failure retries").isTrue();
                    sawYoungAndRetrying = true;
                }
            }
            assertThat(sawYoungAndRetrying && sawOldAndFinished)
                    .as("the fixture covers both sides of the age limit")
                    .isTrue();
        }

        private List<Long> millis(RetryPolicy policy) {
            List<Long> values = new ArrayList<>();
            for (Duration delay : policy.schedule()) {
                values.add(delay.toMillis());
            }
            return values;
        }
    }

    @Nested
    @DisplayName("the jitter bounds")
    class Jitter {

        @Test
        @DisplayName("the bounds in the fixture are the bounds the library keeps to")
        void a_consumer_wait_lands_inside_the_bounds() {
            JsonNode jitter = fixtures.get("jitter");
            double factor = jitter.get("factor").asDouble();
            double min = jitter.get("minMultiplier").asDouble();
            double max = jitter.get("maxMultiplier").asDouble();

            assertThat(factor).isPositive();
            assertThat(min).isEqualTo(1 - factor);
            assertThat(max).isEqualTo(1 + factor);
            assertThat(jitter.get("appliesInBothDirections").asBoolean()).isTrue();

            // The half the fixture cannot prove on its own: that the live library stays inside
            // the range it publishes. A port asserting the same thing about its own delays is
            // asserting the same contract.
            Duration base = Duration.ofSeconds(1);
            RetryPolicy policy = RetryPolicy.exponential(5, base, Duration.ofHours(24));
            long floor = jitter.get("flooredAtMillis").asLong();
            boolean below = false;
            boolean above = false;
            for (int i = 0; i < 5_000; i++) {
                RetryPolicy.Wait wait = policy.nextWait(1, Duration.ZERO).orElseThrow(AssertionError::new);
                long delay = wait.delay().toMillis();
                assertThat(wait.isInBroker()).as("a one-second wait is held in the consumer").isFalse();
                assertThat(delay).isBetween((long) (base.toMillis() * min), (long) (base.toMillis() * max));
                assertThat(delay).isGreaterThanOrEqualTo(floor);
                below |= delay < base.toMillis();
                above |= delay > base.toMillis();
            }
            assertThat(below && above).as("jitter moves the delay in both directions").isTrue();
        }

        @Test
        @DisplayName("a wait the broker holds is the scheduled delay exactly")
        void a_broker_wait_is_never_jittered() {
            assertThat(fixtures.get("jitter").get("appliesToBrokerWaits").asBoolean()).isFalse();

            // Because it names a rung queue whose time-to-live was fixed at declaration. A
            // jittered delay would name a queue nobody declared.
            RetryPolicy policy = RetryPolicy.fixed(2, Duration.ofMinutes(1)).withJitter(0.5);
            for (int i = 0; i < 500; i++) {
                RetryPolicy.Wait wait = policy.nextWait(1, Duration.ZERO).orElseThrow(AssertionError::new);
                assertThat(wait.isInBroker()).isTrue();
                assertThat(wait.delay()).isEqualTo(Duration.ofMinutes(1));
            }
        }
    }

    @Nested
    @DisplayName("the line between waiting here and waiting there")
    class Threshold {

        @Test
        @DisplayName("every row follows the rule, recomputed rather than re-read")
        void the_threshold_table_follows_its_own_rule() {
            JsonNode section = fixtures.get("brokerWaitThreshold");
            assertThat(section.get("defaultMillis").asLong())
                    .as("thirty seconds, the same in all five libraries")
                    .isEqualTo(30_000L);

            int rows = 0;
            for (JsonNode row : section.get("cases")) {
                long threshold = row.get("thresholdMillis").asLong();
                long delay = row.get("delayMillis").asLong();
                // Written out from the sentence in the fixture rather than by calling the
                // library, so that a change to waitsInBroker has to disagree with something.
                boolean expected = threshold > 0 && delay > 0 && delay >= threshold;
                assertThat(row.get("waitsInBroker").asBoolean())
                        .as("a %dms wait against a %dms threshold", delay, threshold)
                        .isEqualTo(expected);
                rows++;
            }
            assertThat(rows).as("the table covers something").isGreaterThan(20);
        }

        @Test
        @DisplayName("a threshold of zero keeps every wait in the consumer")
        void a_zero_threshold_declares_no_rung() {
            RetryPolicy nothing = RetryPolicy.exponential(6, Duration.ofSeconds(10), Duration.ofMinutes(5))
                    .waitInBrokerFrom(Duration.ZERO);

            assertThat(nothing.brokerRungs()).isEmpty();
            assertThat(nothing.waitsInBroker(Duration.ofHours(1))).isFalse();
        }
    }

    @Nested
    @DisplayName("the queue names")
    class Naming {

        /**
         * A second implementation of the rendering rule, written from the rule rather than
         * called from the code that generated the fixture.
         *
         * <p>Deliberately duplicated. A test that calls the same method the generator called
         * proves the two calls agree with each other and nothing else.
         */
        private String render(long millis) {
            if (millis >= 3_600_000 && millis % 3_600_000 == 0) {
                return millis / 3_600_000 + "h";
            }
            if (millis >= 60_000 && millis % 60_000 == 0) {
                return millis / 60_000 + "m";
            }
            if (millis >= 1_000 && millis % 1_000 == 0) {
                return millis / 1_000 + "s";
            }
            return millis + "ms";
        }

        @Test
        @DisplayName("the dead-letter and parking queues are the source queue plus a suffix")
        void the_dead_letter_names_are_derived_from_the_queue() {
            JsonNode naming = fixtures.get("naming");
            String queue = naming.get("queue").asText();

            assertThat(naming.get("deadLetterQueue").asText()).isEqualTo(queue + ".dlq");
            assertThat(naming.get("parkedQueue").asText()).isEqualTo(queue + ".parked");
        }

        @Test
        @DisplayName("a rung is named for the delay it holds, awkward delays included")
        void the_rung_names_render_the_delay() {
            JsonNode naming = fixtures.get("naming");
            String queue = naming.get("queue").asText();

            List<Long> covered = new ArrayList<>();
            for (JsonNode rung : naming.get("retryQueues")) {
                long delay = rung.get("delayMillis").asLong();
                assertThat(rung.get("queue").asText())
                        .as("the rung holding %dms", delay)
                        .isEqualTo(queue + ".retry." + render(delay));
                covered.add(delay);
            }

            // The delays that render awkwardly are the ones worth having, so insist they are
            // still in the file rather than trusting whoever edits it next.
            assertThat(covered).contains(500L, 1_000L, 30_000L, 90_000L, 300_000L, 7_200_000L);
        }

        @Test
        @DisplayName("the sub-second name Java produces is recorded, not quietly normalised")
        void the_sub_second_disagreement_is_written_down() {
            JsonNode naming = fixtures.get("naming");
            String queue = naming.get("queue").asText();

            // Java renders 500ms as 500ms where Go, Python and Ruby render 0s. Recorded here as
            // a disagreement rather than resolved by this repository alone: whichever way it is
            // settled, it has to be settled in five places at once.
            boolean found = false;
            for (JsonNode rung : naming.get("retryQueues")) {
                if (rung.get("delayMillis").asLong() == 500L) {
                    assertThat(rung.get("queue").asText()).isEqualTo(queue + ".retry.500ms");
                    found = true;
                }
            }
            assertThat(found).as("the sub-second case is covered").isTrue();
            assertThat(naming.get("subSecondDisagreement").asText())
                    .as("and the divergence is spelled out for whoever reads the fixture next")
                    .contains("0s");
        }
    }

    @Nested
    @DisplayName("the rung argument table")
    class RungArguments {

        @Test
        @DisplayName("exactly three arguments, and the third points home")
        void a_rung_carries_the_three_arguments_and_no_others() {
            String retryExchange = fixtures.get("retryExchange").asText();
            String source = fixtures.get("naming").get("queue").asText();

            assertThat(retryExchange).isEqualTo("acemq.retry");
            assertThat(fixtures.get("rungArguments")).isNotEmpty();

            for (JsonNode rung : fixtures.get("rungArguments")) {
                JsonNode arguments = rung.get("arguments");
                List<String> names = new ArrayList<>();
                for (Iterator<String> it = arguments.fieldNames(); it.hasNext();) {
                    names.add(it.next());
                }

                assertThat(names)
                        .as("a fourth argument makes the second consumer's declaration fail")
                        .containsExactlyInAnyOrder(
                                "x-message-ttl", "x-dead-letter-exchange", "x-dead-letter-routing-key");
                assertThat(arguments.get("x-message-ttl").asLong())
                        .as("the time-to-live is the wait itself")
                        .isEqualTo(rung.get("delayMillis").asLong());
                assertThat(arguments.get("x-dead-letter-exchange").asText()).isEqualTo(retryExchange);
                assertThat(arguments.get("x-dead-letter-routing-key").asText())
                        .as("an expired message goes back to the queue it came from")
                        .isEqualTo(source);
            }
        }
    }

    @Nested
    @DisplayName("the declared topology")
    class DeclaredTopology {

        private JsonNode topology() {
            return fixtures.get("topology");
        }

        private JsonNode queue(String name) {
            for (JsonNode candidate : topology().get("queues")) {
                if (name.equals(candidate.get("name").asText())) {
                    return candidate;
                }
            }
            throw new AssertionError("no queue named " + name + " in the declared topology");
        }

        @Test
        @DisplayName("the source queue is quorum and says where its dead letters go")
        void the_source_queue_is_quorum_and_dead_letters() {
            String source = topology().get("sourceQueue").asText();
            JsonNode queue = queue(source);

            assertThat(queue.get("type").asText())
                    .as("the queue that holds the work is replicated")
                    .isEqualTo("quorum");
            assertThat(queue.get("durable").asBoolean()).isTrue();
            assertThat(queue.get("arguments").get("x-dead-letter-exchange").asText())
                    .isEqualTo(fixtures.get("deadLetterExchange").asText());
            assertThat(queue.get("arguments").get("x-dead-letter-routing-key").asText())
                    .as("without the routing key the message matches no binding and is dropped")
                    .isEqualTo(source + ".dlq");
        }

        @Test
        @DisplayName("everything that is not the source queue is classic")
        void the_supporting_queues_are_classic() {
            String source = topology().get("sourceQueue").asText();

            for (JsonNode queue : topology().get("queues")) {
                if (queue.get("name").asText().equals(source)) {
                    continue;
                }
                assertThat(queue.get("type").asText())
                        .as("%s is a holding pen, not a workload", queue.get("name").asText())
                        .isEqualTo("classic");
                assertThat(queue.get("durable").asBoolean()).isTrue();
            }
        }

        @Test
        @DisplayName("neither the dead-letter queue nor the parking lot dead-letters in turn")
        void the_dead_letter_queues_do_not_loop() {
            String source = topology().get("sourceQueue").asText();

            for (String name : new String[]{source + ".dlq", source + ".parked"}) {
                assertThat(queue(name).get("arguments"))
                        .as("%s must not dead-letter: a loop is how a poison message becomes an outage", name)
                        .isEmpty();
            }
        }

        @Test
        @DisplayName("a rung exists for every long delay in the policy and for no short one")
        void the_rungs_match_the_policy() {
            String source = topology().get("sourceQueue").asText();
            List<Long> rungDelays = longs(topology().get("policyRungDelaysMillis"));
            List<Long> schedule = longs(topology().get("policyScheduleMillis"));
            long threshold = fixtures.get("defaultBrokerWaitThresholdMillis").asLong();

            // Derived from the schedule and the threshold rather than read back: the rungs are
            // the distinct delays at or above the line, in the order the schedule reaches them.
            List<Long> expected = new ArrayList<>();
            for (long delay : schedule) {
                if (delay >= threshold && !expected.contains(delay)) {
                    expected.add(delay);
                }
            }
            assertThat(rungDelays).isEqualTo(expected);

            List<String> rungQueues = new ArrayList<>();
            for (JsonNode queue : topology().get("queues")) {
                if (queue.get("name").asText().startsWith(source + ".retry.")) {
                    rungQueues.add(queue.get("name").asText());
                }
            }
            assertThat(rungQueues)
                    .as("one queue per distinct long delay, and not one per remaining attempt")
                    .hasSize(rungDelays.size());
        }

        @Test
        @DisplayName("both exchanges are declared, and the rungs route home through one of them")
        void the_exchanges_and_the_bindings_are_all_there() {
            String source = topology().get("sourceQueue").asText();
            String dlx = fixtures.get("deadLetterExchange").asText();
            String retry = fixtures.get("retryExchange").asText();

            List<String> exchanges = new ArrayList<>();
            for (JsonNode exchange : topology().get("exchanges")) {
                exchanges.add(exchange.get("name").asText());
                if (exchange.get("name").asText().equals(dlx) || exchange.get("name").asText().equals(retry)) {
                    assertThat(exchange.get("type").asText())
                            .as("%s is matched on an exact queue name", exchange.get("name").asText())
                            .isEqualTo("direct");
                    assertThat(exchange.get("durable").asBoolean()).isTrue();
                }
            }
            assertThat(exchanges).contains(dlx, retry);

            List<String> bindings = new ArrayList<>();
            for (JsonNode binding : topology().get("bindings")) {
                bindings.add(binding.get("queue").asText() + " <- " + binding.get("exchange").asText()
                        + " [" + binding.get("routingKey").asText() + "]");
            }
            assertThat(bindings)
                    .as("an expired rung message has to reach the queue it came from")
                    .contains(source + " <- " + retry + " [" + source + "]");
            assertThat(bindings).contains(
                    source + ".dlq <- " + dlx + " [" + source + ".dlq]",
                    source + ".parked <- " + dlx + " [" + source + ".parked]");
        }

        @Test
        @DisplayName("the topology names which half of it the consumer declares")
        void the_two_halves_are_labelled() {
            List<String> declarers = new ArrayList<>();
            for (JsonNode queue : topology().get("queues")) {
                declarers.add(queue.get("declaredBy").asText());
            }
            assertThat(declarers)
                    .as("a port that implements only the operator's half has a broker that drops messages")
                    .contains("topology", "consumer");
        }
    }

    @Nested
    @DisplayName("the queue type defaults")
    class QueueTypes {

        @Test
        @DisplayName("a queue that holds work is quorum and a queue that holds failures is classic")
        void the_defaults_are_what_the_fixture_says() {
            JsonNode defaults = fixtures.get("queueTypeDefaults");

            assertThat(defaults.get("sourceQueue").asText()).isEqualTo("quorum");
            assertThat(defaults.get("sourceQueueWithDeadLetter").asText()).isEqualTo("quorum");
            assertThat(defaults.get("explicitClassicQueue").asText()).isEqualTo("classic");
            assertThat(defaults.get("explicitClassicQueueWithDeadLetter").asText()).isEqualTo("classic");
            assertThat(defaults.get("deadLetterQueue").asText()).isEqualTo("classic");
            assertThat(defaults.get("parkedQueue").asText()).isEqualTo("classic");
            assertThat(defaults.get("retryRung").asText()).isEqualTo("classic");
            assertThat(defaults.get("durability").get("everyDeclaredQueueIsDurable").asBoolean()).isTrue();
        }

        @Test
        @DisplayName("Java cannot be asked for an exclusive, auto-delete or transient queue")
        void the_flags_the_other_libraries_have_are_recorded_as_absent() {
            // Not a shortcoming to paper over in the fixture. The ports that offer those flags
            // must keep such a queue classic, and a fixture that pretended Java had an opinion
            // would be inventing one.
            assertThat(fixtures.get("queueTypeDefaults").get("durability").get("exclusiveAutoDeleteOrTransient")
                    .asText())
                    .contains("not declarable through the Java API");
        }
    }
}
