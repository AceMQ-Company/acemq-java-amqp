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
package org.acemq.amqp.transport.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.api.TopologyPlan;
import org.acemq.amqp.core.AceMq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Drift detection against a real broker.
 *
 * <p>The in-memory version compares two maps, which proves the planner reports what it is told
 * and nothing about AMQP. Everything interesting here is in the protocol: whether the broker
 * refuses a declare that disagrees with an existing queue, whether the refusal is a 406 with the
 * argument named in it, and whether that refusal can be read back out of the client's exception
 * rather than merely closing a channel. A mistake in any of the three shows up as a queue
 * reported healthy right up until a deployment fails.
 */
@Testcontainers
@DisplayName("topology drift against a real RabbitMQ")
class TopologyDriftIT {

    @Container
    private static final RabbitMQContainer BROKER = new RabbitMQContainer(BrokerImage.current());

    private AceMq mq;

    @BeforeEach
    void connect() {
        mq = AceMq.connect(BROKER.getAmqpUrl());
    }

    @AfterEach
    void disconnect() {
        if (mq != null) {
            try {
                mq.deleteQueue("drift.orders");
            } catch (RuntimeException e) {
                // The queue may never have been created; the connection still has to close.
            }
            mq.close();
        }
    }

    private static Topology withTtl(long millis) {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-message-ttl", (int) millis);
        return Topology.define().classicQueue("drift.orders", arguments).build();
    }

    @Test
    @Timeout(120)
    @DisplayName("a changed argument is reported, in the broker's own words")
    void driftIsDetected() {
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        TopologyPlan plan = mq.topology().plan(withTtl(30_000));

        assertThat(plan.hasDrift()).isTrue();
        // RabbitMQ's own text, which names the argument and both values. Rewriting it into
        // something tidier would drop the two numbers, which are the only part anyone needs.
        assertThat(plan.render()).contains("DRIFT", "inequivalent arg", "x-message-ttl");
        assertThat(plan.render()).contains("60000").contains("30000");
    }

    @Test
    @Timeout(120)
    @DisplayName("a matching queue is present, and checking it leaves the connection usable")
    void matchingQueueStaysUsable() {
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        assertThat(mq.topology().plan(withTtl(60_000)).hasDrift()).isFalse();

        // The check declares on a channel of its own precisely so a mismatch cannot take
        // anything else with it. Publishing afterwards is how we know it did not.
        assertThat(mq.topology().plan(withTtl(30_000)).hasDrift()).isTrue();
        mq.publisher("", "drift.orders", String.class).send("still working");
        assertThat(mq.messageCount("drift.orders")).isEqualTo(1);
    }

    @Test
    @Timeout(120)
    @DisplayName("checking a queue does not create one")
    void checkingDoesNotCreate() {
        // The check ends in a declare, and a declare creates. Asking passively first is what
        // stops a dry run from provisioning the topology it was only supposed to report on.
        assertThat(mq.topology().plan(withTtl(60_000)).hasChanges()).isTrue();

        assertThat(mq.topology().plan(withTtl(60_000)).render()).contains("create");
    }

    @Test
    @Timeout(120)
    @DisplayName("applying refuses before it declares anything")
    void applyRefusesOnDrift() {
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        assertThatThrownBy(() -> mq.topology().apply(withTtl(30_000), ApplyMode.CREATE_ONLY))
                .isInstanceOf(AceMqException.class)
                .hasMessageContaining("x-message-ttl");

        // And the queue is untouched: refusing early is only worth anything if it is early.
        assertThat(mq.topology().plan(withTtl(60_000)).hasDrift()).isFalse();
    }
}
