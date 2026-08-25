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

import java.time.Duration;

import org.junit.jupiter.api.Test;

class AckTest {

    @Test
    void accept_reports_only_itself() {
        Ack ack = Ack.accept();

        assertThat(ack.isAccept()).isTrue();
        assertThat(ack.isRetry()).isFalse();
        assertThat(ack.isDeadLetter()).isFalse();
        assertThat(ack.isRelease()).isFalse();
        assertThat(ack.delay()).isEmpty();
        assertThat(ack.reason()).isEmpty();
    }

    @Test
    void retry_carries_its_delay_and_reason() {
        Ack ack = Ack.retry(Duration.ofSeconds(5), "inventory service timed out");

        assertThat(ack.isRetry()).isTrue();
        assertThat(ack.delay()).contains(Duration.ofSeconds(5));
        assertThat(ack.reason()).contains("inventory service timed out");
    }

    @Test
    void retry_rejects_a_negative_delay() {
        assertThatThrownBy(() -> Ack.retry(Duration.ofSeconds(-1), "why"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
    }

    @Test
    void retry_rejects_a_missing_delay() {
        assertThatThrownBy(() -> Ack.retry(null, "why")).isInstanceOf(NullPointerException.class);
    }

    @Test
    void retry_allows_no_delay_at_all() {
        Ack ack = Ack.retry(Duration.ZERO, null);

        assertThat(ack.delay()).contains(Duration.ZERO);
        assertThat(ack.reason()).isEmpty();
    }

    @Test
    void dead_letter_carries_a_reason_but_no_delay() {
        Ack ack = Ack.deadLetter("payload failed schema validation");

        assertThat(ack.isDeadLetter()).isTrue();
        assertThat(ack.reason()).contains("payload failed schema validation");
        assertThat(ack.delay()).isEmpty();
    }

    @Test
    void release_reports_only_itself() {
        Ack ack = Ack.release();

        assertThat(ack.isRelease()).isTrue();
        assertThat(ack.isAccept()).isFalse();
        assertThat(ack.delay()).isEmpty();
    }

    @Test
    void stateless_outcomes_are_shared_instances() {
        assertThat(Ack.accept()).isSameAs(Ack.accept());
        assertThat(Ack.release()).isSameAs(Ack.release());
    }

    @Test
    void every_outcome_describes_itself_for_logs() {
        assertThat(Ack.accept().toString()).isEqualTo("Ack.accept");
        assertThat(Ack.release().toString()).isEqualTo("Ack.release");
        assertThat(Ack.retry(Duration.ofSeconds(1), "busy").toString()).contains("retry", "busy");
        assertThat(Ack.deadLetter("bad").toString()).contains("deadLetter", "bad");
    }
}
