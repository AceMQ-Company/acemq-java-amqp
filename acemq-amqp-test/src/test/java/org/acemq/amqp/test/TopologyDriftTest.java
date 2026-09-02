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
import java.util.HashMap;
import java.util.Map;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.ApplyMode;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.api.Topology;
import org.acemq.amqp.api.TopologyPlan;
import org.acemq.amqp.core.AceMq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("topology drift")
class TopologyDriftTest {

    private AceMq mq;

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    private AceMq connect(String broker) {
        mq = AceMq.connect("memory://" + broker, Telemetry.NONE);
        return mq;
    }

    private static Map<String, Object> ttl(long millis) {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-message-ttl", millis);
        return arguments;
    }

    private static Topology withTtl(long millis) {
        return Topology.define()
                .exchange("orders", "topic")
                .classicQueue("orders.new", ttl(millis))
                .bind("orders.new", "orders", "order.*")
                .build();
    }

    @Test
    @Timeout(10)
    @DisplayName("a queue whose argument was changed is reported, not called present")
    void changedArgumentIsDrift() {
        connect("drift-argument");
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        // The next release wants a different time to live. AMQP will not change one in place.
        TopologyPlan plan = mq.topology().plan(withTtl(30_000));

        assertThat(plan.hasDrift()).isTrue();
        assertThat(plan.drift()).hasSize(1);
        // Both values, because "the ttl is wrong" is not enough to decide what to do about it.
        assertThat(plan.render()).contains("DRIFT", "x-message-ttl", "60000", "30000");
    }

    @Test
    @Timeout(10)
    @DisplayName("an argument that appeared since the queue was created is drift too")
    void addedArgumentIsDrift() {
        connect("drift-added");
        mq.topology()
                .apply(
                        Topology.define()
                                .classicQueue("orders.new", Collections.emptyMap())
                                .build(),
                        ApplyMode.CREATE_ONLY);

        TopologyPlan plan = mq.topology().plan(withTtl(30_000));

        assertThat(plan.hasDrift()).isTrue();
        assertThat(plan.render()).contains("x-message-ttl", "none");
    }

    @Test
    @Timeout(10)
    @DisplayName("a queue created as classic where the topology asks for quorum")
    void changedQueueTypeIsDrift() {
        connect("drift-type");
        mq.topology()
                .apply(
                        Topology.define()
                                .classicQueue("orders.new", Collections.emptyMap())
                                .build(),
                        ApplyMode.CREATE_ONLY);

        TopologyPlan plan = mq.topology()
                .plan(Topology.define().queue("orders.new").build());

        assertThat(plan.hasDrift()).isTrue();
        assertThat(plan.render()).contains("queue type is CLASSIC", "QUORUM");
    }

    @Test
    @Timeout(10)
    @DisplayName("a queue that matches is present, and is not drift")
    void matchingQueueIsPresent() {
        connect("drift-none");
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        TopologyPlan plan = mq.topology().plan(withTtl(60_000));

        assertThat(plan.hasDrift()).isFalse();
        assertThat(plan.render()).contains("present", "queue orders.new");
    }

    @Test
    @Timeout(10)
    @DisplayName("applying refuses before touching anything, rather than failing partway")
    void applyRefusesOnDrift() {
        connect("drift-apply");
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        assertThatThrownBy(() -> mq.topology().apply(withTtl(30_000), ApplyMode.CREATE_ONLY))
                .isInstanceOf(AceMqException.class)
                .hasMessageContaining("does not allow changing them in place")
                .hasMessageContaining("x-message-ttl")
                // The two things a person can actually do about it.
                .hasMessageContaining("change the topology")
                .hasMessageContaining("migrate the queue");
    }

    @Test
    @Timeout(10)
    @DisplayName("a dry run reports drift without refusing, because it changes nothing")
    void dryRunReportsWithoutThrowing() {
        connect("drift-dry");
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        // A dry run is how somebody finds out. Throwing here would mean the only way to see the
        // report is to trigger the failure it is warning about.
        TopologyPlan plan = mq.topology().apply(withTtl(30_000), ApplyMode.DRY_RUN);

        assertThat(plan.hasDrift()).isTrue();
    }

    @Test
    @Timeout(10)
    @DisplayName("validate mode refuses on drift as well as on absence")
    void validateRefusesOnDrift() {
        connect("drift-validate");
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        assertThatThrownBy(() -> mq.topology().apply(withTtl(30_000), ApplyMode.VALIDATE))
                .isInstanceOf(AceMqException.class)
                .hasMessageContaining("x-message-ttl");
    }

    @Test
    @Timeout(10)
    @DisplayName("redeclaring does not quietly rewrite what the queue was created with")
    void redeclaringDoesNotEraseDrift() {
        connect("drift-sticky");
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        // A real broker keeps the settings a queue was created with. If the fake let the latest
        // declare win, drift would vanish the moment anything looked for it -- and the topology
        // would pass here and fail in production, which is worse than not checking at all.
        InMemoryTransport.reset();
        connect("drift-sticky-2");
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);
        mq.topology().apply(withTtl(60_000), ApplyMode.CREATE_ONLY);

        assertThat(mq.topology().plan(withTtl(30_000)).hasDrift()).isTrue();
    }
}
