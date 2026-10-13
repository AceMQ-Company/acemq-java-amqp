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
package org.acemq.amqp.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("a received message")
class ReceivedMessageTest {

    @Test
    void carries_the_brokers_redelivered_flag_through_a_change_of_payload() {
        Message<String> message = new ReceivedMessage<>(
                "body", Envelope.of("t").build(), "q", "k", Instant.now(), null, null, true);

        assertThat(message.redelivered()).isTrue();
        assertThat(message.withPayload(1).redelivered()).isTrue();
        assertThat(new ReceivedMessage<>("body", Envelope.of("t").build(), "q", "k", Instant.now()).redelivered())
                .isFalse();
    }
}
