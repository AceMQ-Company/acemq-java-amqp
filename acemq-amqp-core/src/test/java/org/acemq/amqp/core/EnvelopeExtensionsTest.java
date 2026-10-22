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

import java.util.LinkedHashMap;
import java.util.Map;

import org.acemq.amqp.api.Envelope;
import org.junit.jupiter.api.Test;

/** AMQP-ENV-014: an x-acemq- header this version has no field for survives a read and a write. */
class EnvelopeExtensionsTest {

    @Test
    void an_unknown_acemq_header_is_read_as_an_extension_and_written_back() {
        Map<String, Object> wire = new LinkedHashMap<>();
        wire.put("x-acemq-id", "m1");
        wire.put("x-acemq-route", "a,b");
        wire.put("x-acemq-future", "kept");
        wire.put("x-tenant", "acme");

        Envelope envelope = EnvelopeHeaders.fromHeaders(wire, null, "fallback");

        assertThat(envelope.extensions()).containsOnlyKeys("x-acemq-future");
        assertThat(envelope.headers()).containsOnlyKeys("x-tenant");
        assertThat(EnvelopeHeaders.toHeaders(envelope.toBuilder().build()))
                .containsEntry("x-acemq-future", "kept")
                .containsEntry("x-tenant", "acme")
                .containsEntry("x-acemq-id", "m1");
    }

    @Test
    void a_field_wins_over_an_extension_of_the_same_name() {
        Envelope envelope = Envelope.of("t").id("real").extensions(Map.of("x-acemq-id", "stale", "x-tenant", "no"))
                .build();

        assertThat(envelope.extensions()).containsOnlyKeys("x-acemq-id");
        assertThat(EnvelopeHeaders.toHeaders(envelope)).containsEntry("x-acemq-id", "real")
                .doesNotContainKey("x-tenant");
    }
}
