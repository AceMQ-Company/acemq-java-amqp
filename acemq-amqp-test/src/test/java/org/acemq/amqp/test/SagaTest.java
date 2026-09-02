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

import java.util.ArrayList;
import java.util.List;

import org.acemq.amqp.patterns.Saga;
import org.acemq.amqp.patterns.SagaResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("a saga")
class SagaTest {

    /** What the steps write to, standing in for the world they would change. */
    private final List<String> world = new ArrayList<>();

    @Nested
    @DisplayName("when everything works")
    class HappyPath {

        @Test
        @DisplayName("runs every step in order and compensates nothing")
        void runsInOrder() {
            Saga<String> saga = Saga.<String>named("place-order")
                    .step("take-payment", order -> world.add("charged " + order))
                    .compensateWith(order -> world.add("refunded " + order))
                    .step("reserve-stock", order -> world.add("reserved " + order))
                    .compensateWith(order -> world.add("released " + order))
                    .build();

            SagaResult result = saga.run("o-1");

            assertThat(result.isComplete()).isTrue();
            assertThat(result.compensated()).isFalse();
            assertThat(world).containsExactly("charged o-1", "reserved o-1");
        }
    }

    @Nested
    @DisplayName("when a step fails")
    class Compensating {

        @Test
        @DisplayName("undoes what was done, most recent first")
        void undoesInReverse() {
            Saga<String> saga = Saga.<String>named("place-order")
                    .step("take-payment", order -> world.add("charged " + order))
                    .compensateWith(order -> world.add("refunded " + order))
                    .step("reserve-stock", order -> world.add("reserved " + order))
                    .compensateWith(order -> world.add("released " + order))
                    .step("book-courier", order -> {
                        throw new IllegalStateException("no couriers available");
                    })
                    .build();

            SagaResult result = saga.run("o-1");

            assertThat(result.compensated()).isTrue();
            assertThat(result.failedAt()).contains("book-courier");
            assertThat(result.failure().orElseThrow()).hasMessageContaining("no couriers");

            // Reverse order, because that is the order the world was changed in: the stock is
            // released before the payment is refunded.
            assertThat(world).containsExactly(
                    "charged o-1", "reserved o-1", "released o-1", "refunded o-1");
        }

        @Test
        @DisplayName("does not compensate a step that never ran")
        void onlyCompensatesWhatCompleted() {
            Saga<String> saga = Saga.<String>named("place-order")
                    .step("take-payment", order -> world.add("charged " + order))
                    .compensateWith(order -> world.add("refunded " + order))
                    .step("reserve-stock", order -> {
                        throw new IllegalStateException("out of stock");
                    })
                    .compensateWith(order -> world.add("released " + order))
                    .build();

            SagaResult result = saga.run("o-1");

            // Releasing stock that was never reserved would be a second bug on top of the first.
            assertThat(world).containsExactly("charged o-1", "refunded o-1");
            assertThat(result.completed()).containsExactly("take-payment");
        }

        @Test
        @DisplayName("a step with no compensation is skipped rather than failing")
        void stepsWithoutCompensation() {
            Saga<String> saga = Saga.<String>named("place-order")
                    .step("look-up-customer", order -> world.add("looked up " + order))
                    .step("take-payment", order -> world.add("charged " + order))
                    .compensateWith(order -> world.add("refunded " + order))
                    .step("book-courier", order -> {
                        throw new IllegalStateException("no couriers");
                    })
                    .build();

            SagaResult result = saga.run("o-1");

            // The lookup changed nothing, so there is nothing to undo. Legitimate, and the
            // reason a missing compensation is not an error.
            assertThat(world).containsExactly("looked up o-1", "charged o-1", "refunded o-1");
            assertThat(result.hasUnresolved()).isFalse();
        }
    }

    @Nested
    @DisplayName("when compensation itself fails")
    class UnresolvedCompensation {

        @Test
        @DisplayName("the remaining compensations still run, and the failure is reported")
        void carriesOnAndReports() {
            Saga<String> saga = Saga.<String>named("place-order")
                    .step("take-payment", order -> world.add("charged " + order))
                    .compensateWith(order -> world.add("refunded " + order))
                    .step("reserve-stock", order -> world.add("reserved " + order))
                    .compensateWith(order -> {
                        throw new IllegalStateException("the warehouse is offline");
                    })
                    .step("book-courier", order -> {
                        throw new IllegalStateException("no couriers");
                    })
                    .build();

            SagaResult result = saga.run("o-1");

            // Stopping at the failed compensation would have left the payment taken as well.
            assertThat(world).containsExactly("charged o-1", "reserved o-1", "refunded o-1");

            // This is the list to alert on: a real-world effect that happened, was meant to be
            // undone, and was not. No retry resolves it; a person does.
            assertThat(result.unresolved()).containsExactly("reserve-stock");
            assertThat(result.hasUnresolved()).isTrue();
            assertThat(result.toString()).contains("UNRESOLVED");
        }
    }

    @Nested
    @DisplayName("building one")
    class Building {

        @Test
        @DisplayName("refuses two steps with the same name")
        void refusesDuplicateNames() {
            assertThatThrownBy(() -> Saga.<String>named("s")
                    .step("charge", order -> {
                    })
                    .step("charge", order -> {
                    }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("already has a step called");
        }

        @Test
        @DisplayName("refuses a compensation with no step to attach it to")
        void refusesOrphanCompensation() {
            assertThatThrownBy(() -> Saga.<String>named("s").compensateWith(order -> {
            }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no step to compensate");
        }

        @Test
        @DisplayName("refuses a saga with no steps")
        void refusesEmpty() {
            assertThatThrownBy(() -> Saga.<String>named("s").build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no steps");
        }
    }
}
