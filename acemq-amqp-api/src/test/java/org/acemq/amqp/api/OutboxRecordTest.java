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

import java.time.Instant;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("an outbox record")
class OutboxRecordTest {

    private static Envelope envelope() {
        return Envelope.of("order.placed")
                .id("o-1")
                .correlationId("checkout-9")
                .causationId("cmd-3")
                .build();
    }

    @Test
    void takes_its_identity_from_the_envelope_it_will_be_published_with() {
        OutboxRecord record = OutboxRecord.of("orders", "order.placed", envelope(), "{}");

        // The record's identifier is the message's identifier rather than a key of its own. That
        // is what lets a consumer recognise the second copy of a message the relay sent twice.
        assertThat(record.id()).isEqualTo("o-1");
        assertThat(record.type()).isEqualTo("order.placed");
        assertThat(record.correlationId()).contains("checkout-9");
        assertThat(record.causationId()).contains("cmd-3");
        assertThat(record.attempts()).isZero();
        assertThat(record.lastError()).isEmpty();
        assertThat(record.createdAt()).isBeforeOrEqualTo(Instant.now());
    }

    @Test
    void rebuilds_the_same_envelope_it_was_made_from() {
        Envelope rebuilt = OutboxRecord.of("orders", "order.placed", envelope(), "{}").envelope();

        assertThat(rebuilt.id()).isEqualTo("o-1");
        assertThat(rebuilt.type()).isEqualTo("order.placed");
        assertThat(rebuilt.correlationId()).isEqualTo("checkout-9");
        assertThat(rebuilt.causationId()).contains("cmd-3");
    }

    @Test
    void keeps_the_destination_it_was_given() {
        OutboxRecord record = OutboxRecord.of("", "orders.direct", envelope(), "{\"total\":42}");

        assertThat(record.exchange()).isEmpty();
        assertThat(record.routingKey()).isEqualTo("orders.direct");
        assertThat(record.payload()).isEqualTo("{\"total\":42}");
    }

    @Test
    void reports_the_attempts_and_the_reason_it_was_read_back_with() {
        OutboxRecord read = new OutboxRecord(
                "o-1", "orders", "order.placed", "order.placed", "{}", null, null, Instant.now(), 3, "unroutable");

        assertThat(read.attempts()).isEqualTo(3);
        assertThat(read.lastError()).contains("unroutable");
        assertThat(read.correlationId()).isEmpty();
        assertThat(read.causationId()).isEmpty();
    }

    @Test
    void refuses_to_be_built_without_what_it_needs() {
        Instant now = Instant.now();

        assertThatThrownBy(() -> new OutboxRecord(null, "e", "r", "t", "p", null, null, now, 0, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new OutboxRecord("i", "e", "r", "t", null, null, null, now, 0, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> OutboxRecord.of("e", "r", null, "p")).isInstanceOf(NullPointerException.class);
    }

    @Test
    void two_records_are_the_same_when_they_are_the_same_message_at_the_same_attempt() {
        Instant now = Instant.now();
        OutboxRecord first = new OutboxRecord("o-1", "e", "r", "t", "p", null, null, now, 0, null);
        OutboxRecord same = new OutboxRecord("o-1", "e", "r", "t", "p", null, null, now, 0, null);
        OutboxRecord retried = new OutboxRecord("o-1", "e", "r", "t", "p", null, null, now, 1, "failed");
        OutboxRecord other = new OutboxRecord("o-2", "e", "r", "t", "p", null, null, now, 0, null);

        assertThat(first).isEqualTo(first).isEqualTo(same).isNotEqualTo(retried).isNotEqualTo(other);
        assertThat(first).isNotEqualTo("not a record");
        assertThat(first).hasSameHashCodeAs(same);
    }

    @Test
    void says_enough_in_its_own_words_to_be_useful_in_a_log() {
        OutboxRecord record = OutboxRecord.of("orders", "order.placed", envelope(), "{}");

        assertThat(record.toString()).contains("o-1", "orders/order.placed", "order.placed", "attempts=0");
    }
}
