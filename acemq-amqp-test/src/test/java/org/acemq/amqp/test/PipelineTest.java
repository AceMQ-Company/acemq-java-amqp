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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.RoutingSlip;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Pipeline;
import org.acemq.amqp.patterns.InMemoryIdempotencyStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("a pipeline")
class PipelineTest {

    private AceMq mq;

    private AceMq connect(String broker) {
        mq = AceMq.connect("memory://" + broker, Telemetry.NONE);
        return mq;
    }

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Nested
    @DisplayName("running a chain")
    class Chain {

        @Test
        @Timeout(60)
        void passes_a_message_through_every_step_in_order() {
            connect("pipeline-order");
            List<String> visited = new CopyOnWriteArrayList<>();

            try (Pipeline<String> fulfilment = mq.pipeline("fulfilment", String.class)
                    .step("validate", String.class, message -> {
                        visited.add("validate:" + message.payload());
                        return message.payload() + "|validated";
                    })
                    .step("enrich", String.class, message -> {
                        visited.add("enrich:" + message.payload());
                        return message.payload() + "|enriched";
                    })
                    .step("dispatch", String.class, message -> {
                        visited.add("dispatch:" + message.payload());
                        return null;
                    })
                    .build()) {

                fulfilment.send("order-1");

                await().atMost(Duration.ofSeconds(30)).until(() -> visited.size() == 3);

                // Each step sees what the one before it produced, which is the whole point of
                // threading the types through the builder.
                assertThat(visited).containsExactly(
                        "validate:order-1",
                        "enrich:order-1|validated",
                        "dispatch:order-1|validated|enriched");
            }
        }

        @Test
        @Timeout(60)
        void changes_the_payload_type_between_steps() {
            connect("pipeline-types");
            AtomicReference<Integer> received = new AtomicReference<>();

            try (Pipeline<String> counting = mq.pipeline("counting", String.class)
                    .step("measure", Integer.class, message -> message.payload().length())
                    .step("record", Void.class, message -> {
                        received.set(message.payload());
                        return null;
                    })
                    .build()) {

                counting.send("twelve chars");

                await().atMost(Duration.ofSeconds(30)).until(() -> received.get() != null);
                assertThat(received.get()).isEqualTo(12);
            }
        }

        @Test
        @Timeout(60)
        void declares_one_queue_per_step() {
            connect("pipeline-topology");

            try (Pipeline<String> fulfilment = mq.pipeline("fulfilment", String.class)
                    .step("validate", String.class, m -> m.payload())
                    .step("enrich", String.class, m -> m.payload())
                    .build()) {

                assertThat(fulfilment.stepNames()).containsExactly("validate", "enrich");
                assertThat(fulfilment.queueFor("validate")).isEqualTo("fulfilment.validate");
                assertThat(fulfilment.name()).isEqualTo("fulfilment");
                assertThat(fulfilment.toString()).contains("validate | enrich");
            }
        }

        @Test
        @Timeout(60)
        void counts_what_went_in_and_what_finished() {
            connect("pipeline-counters");

            try (Pipeline<String> pipeline = mq.pipeline("counted", String.class)
                    .step("first", String.class, m -> m.payload())
                    .step("last", String.class, m -> m.payload())
                    .build()) {

                pipeline.send("a");
                pipeline.send("b");

                await().atMost(Duration.ofSeconds(30)).until(() -> pipeline.completed() == 2);
                assertThat(pipeline.entered()).isEqualTo(2);
                assertThat(pipeline.endedEarly()).isZero();
            }
        }
    }

    @Nested
    @DisplayName("the routing slip")
    class Slip {

        @Test
        @Timeout(60)
        void travels_with_the_message_and_says_where_it_is() {
            connect("pipeline-slip");
            List<String> seen = new CopyOnWriteArrayList<>();

            try (Pipeline<String> pipeline = mq.pipeline("slipped", String.class)
                    .step("one", String.class, message -> {
                        RoutingSlip slip = message.envelope().route().orElseThrow(AssertionError::new);
                        seen.add(slip.current().orElse("?") + "@" + slip.position());
                        return message.payload();
                    })
                    .step("two", String.class, message -> {
                        RoutingSlip slip = message.envelope().route().orElseThrow(AssertionError::new);
                        seen.add(slip.current().orElse("?") + "@" + slip.position());
                        return null;
                    })
                    .build()) {

                pipeline.send("x");

                await().atMost(Duration.ofSeconds(30)).until(() -> seen.size() == 2);
                // No coordinator anywhere: the message itself knows where it is.
                assertThat(seen).containsExactly("one@0", "two@1");
            }
        }

        @Test
        @Timeout(60)
        void keeps_one_run_identifier_across_every_hop() {
            connect("pipeline-runid");
            List<String> runIds = new CopyOnWriteArrayList<>();

            try (Pipeline<String> pipeline = mq.pipeline("tracked", String.class)
                    .step("one", String.class, message -> {
                        runIds.add(message.envelope().route().orElseThrow(AssertionError::new).runId());
                        return message.payload();
                    })
                    .step("two", String.class, message -> {
                        runIds.add(message.envelope().route().orElseThrow(AssertionError::new).runId());
                        return null;
                    })
                    .build()) {

                String runId = pipeline.send("x");

                await().atMost(Duration.ofSeconds(30)).until(() -> runIds.size() == 2);
                assertThat(runIds).containsExactly(runId, runId);
            }
        }
    }

    @Nested
    @DisplayName("a step that returns nothing")
    class EndingEarly {

        @Test
        @Timeout(60)
        void stops_the_route_without_failing() {
            connect("pipeline-filter");
            AtomicInteger reachedSecond = new AtomicInteger();

            try (Pipeline<String> pipeline = mq.pipeline("filtered", String.class)
                    .step("filter", String.class,
                            message -> message.payload().startsWith("keep") ? message.payload() : null)
                    .step("handle", Void.class, message -> {
                        reachedSecond.incrementAndGet();
                        return null;
                    })
                    .build()) {

                pipeline.send("keep-me");
                pipeline.send("drop-me");
                pipeline.send("drop-me-too");

                await().atMost(Duration.ofSeconds(30)).until(() -> reachedSecond.get() == 1);

                // Two were filtered at the first step; the one that survived reached the end.
                // Filtering is a decision rather than a failure, and is counted apart from both.
                await().atMost(Duration.ofSeconds(20)).until(() -> pipeline.endedEarly() == 2);
                assertThat(pipeline.completed()).isEqualTo(1);
                assertThat(reachedSecond).hasValue(1);
            }
        }
    }

    @Nested
    @DisplayName("failure at one step")
    class Failure {

        @Test
        @Timeout(90)
        void retries_that_step_without_repeating_the_ones_before_it() {
            connect("pipeline-retry");
            AtomicInteger firstStepRuns = new AtomicInteger();
            AtomicInteger secondStepAttempts = new AtomicInteger();
            AtomicInteger finished = new AtomicInteger();

            try (Pipeline<String> pipeline = mq.pipeline("retried", String.class)
                    .step("first", String.class, message -> {
                        firstStepRuns.incrementAndGet();
                        return message.payload();
                    })
                    .step("second", String.class, message -> {
                        if (secondStepAttempts.incrementAndGet() < 3) {
                            throw new IllegalStateException("not yet");
                        }
                        finished.incrementAndGet();
                        return null;
                    })
                    .withRetry(RetryPolicy.fixed(5, Duration.ofMillis(100)).withJitter(0))
                    .build()) {

                pipeline.send("x");

                await().atMost(Duration.ofSeconds(60)).until(() -> finished.get() == 1);

                // The step before never ran again. That is the property that makes a
                // non-idempotent earlier step survivable when a later one is flaky.
                assertThat(firstStepRuns).hasValue(1);
                assertThat(secondStepAttempts).hasValue(3);
            }
        }

        @Test
        @Timeout(60)
        void leaves_later_steps_untouched() {
            connect("pipeline-blocked");
            AtomicInteger reachedThird = new AtomicInteger();

            try (Pipeline<String> pipeline = mq.pipeline("blocked", String.class)
                    .step("one", String.class, m -> m.payload())
                    .step("two", String.class, message -> {
                        throw new IllegalStateException("this step is broken");
                    })
                    .step("three", Void.class, message -> {
                        reachedThird.incrementAndGet();
                        return null;
                    })
                    .build()) {

                pipeline.send("x");
                Thread.sleep(1200);

                // Nothing downstream saw the message, and nothing downstream will until the
                // failing step passes.
                assertThat(reachedThird).hasValue(0);
                assertThat(pipeline.completed()).isZero();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Nested
    @DisplayName("per-step settings")
    class PerStep {

        @Test
        @Timeout(60)
        void give_one_step_more_consumers_than_another() {
            connect("pipeline-scaling");

            try (Pipeline<String> pipeline = mq.pipeline("scaled", String.class)
                    .step("fast", String.class, m -> m.payload())
                    .concurrency(1)
                    .step("slow", Void.class, m -> null)
                    .concurrency(4)
                    .build()) {

                assertThat(pipeline.step("fast").size()).isEqualTo(1);
                assertThat(pipeline.step("slow").size()).isEqualTo(4);

                // The operational argument for queues between steps: the slowest one can be
                // given capacity without touching its neighbours.
                pipeline.step("slow").scaleTo(8);
                assertThat(pipeline.step("slow").size()).isEqualTo(8);
                assertThat(pipeline.step("fast").size()).isEqualTo(1);
            }
        }

        @Test
        @Timeout(60)
        void handle_a_duplicate_once_where_it_matters() {
            connect("pipeline-idempotent");

            try (Pipeline<String> pipeline = mq.pipeline("deduped", String.class)
                    .step("charge", Void.class, m -> null)
                    .idempotent(InMemoryIdempotencyStore.forOneDay())
                    .build()) {

                pipeline.send("x");
                await().atMost(Duration.ofSeconds(30)).until(() -> pipeline.completed() == 1);
            }
        }

        @Test
        @Timeout(60)
        void encode_one_step_s_output_in_another_format() {
            connect("pipeline-encoding");
            List<String> reached = new CopyOnWriteArrayList<>();

            try (Pipeline<String> pipeline = mq.pipeline("encoded", String.class)
                    .step("first", String.class, message -> {
                        reached.add("first");
                        return message.payload();
                    })
                    .step("middle", String.class, message -> {
                        reached.add("middle");
                        return message.payload();
                    })
                    .encodedAs(org.acemq.amqp.core.Codecs.byName("bytes"))
                    .step("last", Void.class, message -> {
                        reached.add("last");
                        return null;
                    })
                    .build()) {

                pipeline.send("x");

                // The bytes codec refuses a String, so configuring it on 'middle' must break
                // the publish from middle to last and nothing earlier. That is what makes this
                // decisive: were the codec applied to the destination instead of the sender,
                // the failure would land one hop earlier and 'middle' would never run.
                await().atMost(Duration.ofSeconds(30)).until(() -> reached.contains("middle"));
                Thread.sleep(800);

                assertThat(reached).containsExactly("first", "middle");
                assertThat(pipeline.completed()).isZero();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Test
        @Timeout(30)
        void say_which_step_is_meant_when_the_name_is_wrong() {
            connect("pipeline-unknown-step");

            try (Pipeline<String> pipeline = mq.pipeline("known", String.class)
                    .step("one", Void.class, m -> null)
                    .build()) {

                assertThatThrownBy(() -> pipeline.step("two"))
                        .isInstanceOf(AceMqException.class)
                        .hasMessageContaining("no running step called 'two'")
                        .hasMessageContaining("one");
            }
        }
    }

    @Nested
    @DisplayName("declaring one")
    class Declaring {

        @Test
        @Timeout(30)
        void needs_at_least_one_step() {
            connect("pipeline-empty");

            assertThatThrownBy(() -> mq.pipeline("empty", String.class).build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no steps");
        }

        @Test
        @Timeout(30)
        void refuses_two_steps_with_the_same_name() {
            connect("pipeline-duplicate");

            // Step names are the routing slip, so two the same would make a slip ambiguous.
            assertThatThrownBy(() -> mq.pipeline("dup", String.class)
                    .step("same", String.class, m -> m.payload())
                    .step("same", Void.class, m -> null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("already has a step called 'same'");
        }

        @Test
        @Timeout(30)
        void refuses_a_name_that_cannot_be_a_routing_key() {
            connect("pipeline-badname");

            // A comma would split the slip; a space is not a routing key.
            assertThatThrownBy(() -> mq.pipeline("bad", String.class)
                    .step("one,two", Void.class, m -> null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("no commas");
            assertThatThrownBy(() -> mq.pipeline("bad", String.class)
                    .step("", Void.class, m -> null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @Timeout(30)
        void refuses_settings_before_there_is_a_step_to_settle() {
            connect("pipeline-early");

            assertThatThrownBy(() -> mq.pipeline("early", String.class).concurrency(4))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("call step(...) first");
        }

        @Test
        @Timeout(30)
        void refuses_a_closed_pipeline() {
            connect("pipeline-closed");
            Pipeline<String> pipeline = mq.pipeline("closed", String.class)
                    .step("one", Void.class, m -> null)
                    .build();
            pipeline.close();

            assertThatThrownBy(() -> pipeline.send("x")).isInstanceOf(AceMqException.class);
        }
    }
}
