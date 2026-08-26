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

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("the topology model")
class TopologyTest {

    @Test
    void describes_what_should_exist() {
        Topology topology = Topology.define()
                .exchange("orders", "topic")
                .queue("orders.new")
                .classicQueue("orders.audit", Collections.singletonMap("x-message-ttl", 1000))
                .bind("orders.new", "orders", "order.*")
                .build();

        assertThat(topology.exchanges()).hasSize(1);
        assertThat(topology.exchanges().get(0).type()).isEqualTo("topic");
        assertThat(topology.exchanges().get(0).durable()).isTrue();
        assertThat(topology.queues()).hasSize(2);
        assertThat(topology.bindings()).hasSize(1);
        assertThat(topology.bindings().get(0).routingKey()).isEqualTo("order.*");
        assertThat(topology.toString()).contains("exchanges=1", "queues=2");
    }

    @Test
    void defaults_a_queue_to_quorum_and_durable() {
        // The default is the one almost everyone wants and almost nobody remembers to ask for.
        Topology.QueueSpec queue = Topology.define().queue("orders.new").build().queues().get(0);

        assertThat(queue.quorum()).isTrue();
        assertThat(queue.durable()).isTrue();
        assertThat(queue.arguments()).isEmpty();
    }

    @Test
    void keeps_the_arguments_of_a_classic_queue() {
        Topology.QueueSpec queue = Topology.define()
                .classicQueue("orders.retry", Collections.singletonMap("x-message-ttl", 5000))
                .build()
                .queues()
                .get(0);

        assertThat(queue.quorum()).isFalse();
        assertThat(queue.arguments()).containsEntry("x-message-ttl", 5000);
    }

    @Test
    void is_immutable_once_built() {
        Topology topology = Topology.define().queue("orders.new").build();

        assertThatThrownBy(() -> topology.queues().add(null)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> topology.queues().get(0).arguments().put("x", 1))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejects_an_unnamed_queue_or_exchange() {
        assertThatThrownBy(() -> Topology.define().queue(null).build()).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> Topology.define().exchange("orders", null).build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void treats_a_missing_routing_key_as_the_empty_one() {
        // Fanout bindings carry no routing key, and null would reach the broker as the string
        // "null" if it were not normalised here.
        Topology.BindingSpec binding = Topology.define().bind("audit", "events", null).build().bindings().get(0);

        assertThat(binding.routingKey()).isEmpty();
    }

    @Test
    void a_plan_separates_what_would_change_from_what_is_already_there() {
        TopologyPlan plan = TopologyPlan.of(Arrays.asList(
                TopologyPlan.Action.of(TopologyPlan.Kind.CREATE, "queue orders.new (quorum)"),
                TopologyPlan.Action.of(TopologyPlan.Kind.PRESENT, "queue orders.old (quorum)")));

        assertThat(plan.actions()).hasSize(2);
        assertThat(plan.changes()).hasSize(1);
        assertThat(plan.hasChanges()).isTrue();
        assertThat(plan.render()).contains("create ", "present", "orders.new", "orders.old");
        assertThat(plan.toString()).contains("1 change(s) of 2 item(s)");
    }

    @Test
    void a_plan_with_nothing_to_do_says_so() {
        TopologyPlan plan = TopologyPlan.of(Collections.emptyList());

        assertThat(plan.hasChanges()).isFalse();
        assertThat(plan.render()).contains("nothing declared");
    }

    @Test
    void a_plan_of_only_existing_items_proposes_no_change() {
        TopologyPlan plan = TopologyPlan.of(
                Collections.singletonList(TopologyPlan.Action.of(TopologyPlan.Kind.PRESENT, "queue orders.new")));

        assertThat(plan.hasChanges()).isFalse();
        assertThat(plan.actions().get(0).toString()).contains("PRESENT");
    }

    @Test
    void apply_modes_are_named_for_what_they_permit() {
        assertThat(ApplyMode.values())
                .containsExactly(ApplyMode.DRY_RUN, ApplyMode.CREATE_ONLY, ApplyMode.VALIDATE);
    }
}
