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
import java.util.Map;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Capability;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.PublishOptions;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A capability is a promise that <em>this library</em> can do the thing.
 *
 * <p>Not that the broker could if somebody wrote the code. Three capabilities were claimed with
 * no API behind them at all, so {@code supports(...)} returned true and left the caller with
 * nothing to call — and the portability example told readers to branch on exactly that.
 */
@DisplayName("what a capability claims")
class CapabilityHonestyTest {

    @AfterEach
    void tearDown() {
        InMemoryTransport.reset();
    }

    @Test
    void priority_is_reachable_through_publish_options() {
        // The gap this closes: PRIORITY was claimed and PublishOptions had no way to set one.
        PublishOptions urgent = PublishOptions.defaults().withPriority(9);

        assertThat(urgent.priority()).hasValue(9);
        assertThat(PublishOptions.defaults().priority()).isEmpty();
    }

    @Test
    void a_negative_priority_is_refused_where_it_is_written_rather_than_at_the_broker() {
        assertThatThrownBy(() -> PublishOptions.defaults().withPriority(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("priority");
    }

    @Test
    void the_in_memory_transport_refuses_a_priority_it_cannot_honour() {
        try (AceMq mq = AceMq.connect("memory://priority", Telemetry.NONE)) {
            assertThat(mq.supports(Capability.PRIORITY)).isFalse();
            mq.declareQueue("work", QueueType.CLASSIC, Collections.emptyMap());

            // Ignoring it would reorder nothing here and reorder everything against a real
            // broker: a test that passes and a production that does not.
            assertThatThrownBy(() -> mq.publisher("", "work", String.class)
                    .with(PublishOptions.defaults().withPriority(5))
                    .asText()
                    .send("urgent"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("does not support priority");
        }
    }

    @Test
    void a_publish_without_a_priority_is_unaffected() {
        try (AceMq mq = AceMq.connect("memory://priority-none", Telemetry.NONE)) {
            mq.declareQueue("work", QueueType.CLASSIC, Collections.emptyMap());
            mq.publisher("", "work", String.class).asText().send("ordinary");

            assertThat(mq.messageCount("work")).isEqualTo(1);
        }
    }

    @Test
    void nothing_claims_a_capability_the_library_cannot_express() {
        try (AceMq mq = AceMq.connect("memory://claims", Telemetry.NONE)) {
            // TRANSACTIONS has no API on any transport. It is no longer claimed by either,
            // which is the honest answer until multi-message transactions exist.
            assertThat(mq.supports(Capability.TRANSACTIONS)).isFalse();
        }
    }

    @Test
    void single_active_consumer_is_a_queue_argument_and_is_declared_like_one() {
        try (AceMq mq = AceMq.connect("memory://single-active", Telemetry.NONE)) {
            // Reachable today, which is why it stays claimed on RabbitMQ: it is a queue
            // argument rather than a method, and the in-memory transport takes the
            // declaration without pretending to honour the ordering guarantee.
            mq.declareQueue("ordered", QueueType.CLASSIC, Map.of("x-single-active-consumer", true));

            assertThat(mq.messageCount("ordered")).isZero();
        }
    }
}
