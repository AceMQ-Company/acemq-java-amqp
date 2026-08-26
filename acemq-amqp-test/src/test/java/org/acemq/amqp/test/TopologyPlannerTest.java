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

import java.util.Collections;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.PublishFailedException;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.api.TopologyPlan;
import org.acemq.amqp.core.AceMq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("the topology planner")
class TopologyPlannerTest {

    private AceMq mq;

    private Topology orders() {
        return Topology.define()
                .exchange("orders", "topic")
                .classicQueue("orders.new", Collections.emptyMap())
                .bind("orders.new", "orders", "order.*")
                .build();
    }

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

    @Test
    @Timeout(10)
    void plans_creations_for_a_topology_that_does_not_exist_yet() {
        connect("planner-new");

        TopologyPlan plan = mq.topology().plan(orders());

        assertThat(plan.hasChanges()).isTrue();
        assertThat(plan.render()).contains("create", "queue orders.new", "exchange orders");
    }

    @Test
    @Timeout(10)
    void a_dry_run_changes_nothing() {
        connect("planner-dry");

        mq.topology().apply(orders(), ApplyMode.DRY_RUN);

        // The proof that nothing was created is that publishing has nowhere to go: an
        // apply that quietly did the work would make this succeed.
        assertThatThrownBy(() -> mq.publisher("orders", "order.placed").send("payload"))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    @Timeout(10)
    void applying_creates_the_topology_and_the_second_apply_reports_it_present() {
        connect("planner-apply");

        TopologyPlan first = mq.topology().apply(orders(), ApplyMode.CREATE_ONLY);
        assertThat(first.hasChanges()).isTrue();

        mq.publisher("orders", "order.placed").send("payload");

        // Re-planning the same topology now finds the queue already there, which is what
        // makes a start-up apply safe to run on every boot.
        TopologyPlan second = mq.topology().plan(orders());
        assertThat(second.actions())
                .filteredOn(action -> action.description().startsWith("queue "))
                .allSatisfy(action -> assertThat(action.kind()).isEqualTo(TopologyPlan.Kind.PRESENT));
        assertThat(second.render()).contains("present");
    }

    @Test
    @Timeout(10)
    void applying_twice_is_harmless() {
        connect("planner-twice");

        mq.topology().apply(orders(), ApplyMode.CREATE_ONLY);
        mq.topology().apply(orders(), ApplyMode.CREATE_ONLY);

        mq.publisher("orders", "order.placed").send("payload");
    }

    @Test
    @Timeout(10)
    void validate_refuses_to_start_when_the_topology_is_missing() {
        connect("planner-validate-missing");

        assertThatThrownBy(() -> mq.topology().apply(orders(), ApplyMode.VALIDATE))
                .isInstanceOf(AceMqException.class)
                .hasMessageContaining("validate mode")
                .hasMessageContaining("orders.new");
    }

    @Test
    @Timeout(10)
    void validate_passes_once_the_topology_is_there() {
        connect("planner-validate-present");

        mq.topology().apply(orders(), ApplyMode.CREATE_ONLY);
        mq.topology().apply(orders(), ApplyMode.VALIDATE);
    }

    @Test
    @Timeout(10)
    void refuses_a_quorum_queue_the_broker_cannot_provide() {
        connect("planner-quorum");

        Topology quorum = Topology.define().queue("orders.quorum").build();

        assertThatThrownBy(() -> mq.topology().apply(quorum, ApplyMode.CREATE_ONLY))
                .isInstanceOf(AceMqException.class)
                .hasMessageContaining("does not support quorum queues");
    }

    @Test
    @Timeout(10)
    void an_empty_topology_plans_nothing() {
        connect("planner-empty");

        TopologyPlan plan = mq.topology().plan(Topology.define().build());

        assertThat(plan.hasChanges()).isFalse();
        assertThat(plan.render()).contains("nothing declared");
    }

    @Test
    @Timeout(10)
    void a_published_message_reaches_a_queue_the_planner_created() {
        connect("planner-roundtrip");
        mq.topology().apply(orders(), ApplyMode.CREATE_ONLY);

        // Not an assertion about the plan but about the broker: the planner's output is only
        // worth anything if the topology it created actually routes.
        assertThat(mq.publisher("orders", "order.placed").send("payload").routed())
                .isTrue();
        assertThatThrownBy(() -> mq.publisher("orders", "unmatched.key").send("payload"))
                .isInstanceOf(PublishFailedException.class);
    }
}
