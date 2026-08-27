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

import java.time.Duration;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Capability;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("a transport that has no streams")
class StreamCapabilityTest {

    private AceMq mq;

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Test
    @Timeout(10)
    void says_so_rather_than_quietly_making_an_ordinary_queue() {
        mq = AceMq.connect("memory://streams-unsupported", Telemetry.NONE);

        // Capability declared, not assumed. Falling back to a classic queue would look like it
        // worked, and would lose replay, retention and every consumer's independent position —
        // which is everything a stream was chosen for.
        assertThat(mq.supports(Capability.STREAMS)).isFalse();

        assertThatThrownBy(() -> mq.stream("orders.log", String.class))
                .isInstanceOf(AceMqException.class)
                .hasMessageContaining("does not support streams")
                .hasMessageContaining("orders.log");

        assertThatThrownBy(() -> mq.declareStream("orders.log", Duration.ofHours(1), null))
                .isInstanceOf(AceMqException.class)
                .hasMessageContaining("does not support streams");
    }
}
