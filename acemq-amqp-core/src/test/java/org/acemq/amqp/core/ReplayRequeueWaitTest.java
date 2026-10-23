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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** I5: the wait for passed-over messages is configurable; the default stays 5 seconds. */
@DisplayName("the replay requeue wait")
class ReplayRequeueWaitTest {

    private final Replay replay = new Replay(null, "orders", "orders.dlq");

    @Test
    @DisplayName("defaults to 5 seconds and survives parked() and keepingAttempts()")
    void default_and_carried() {
        assertThat(replay.requeueWait()).isEqualTo(Duration.ofSeconds(5));
        Replay waiting = replay.waitingForRequeue(Duration.ofSeconds(30));
        assertThat(waiting.requeueWait()).isEqualTo(Duration.ofSeconds(30));
        assertThat(waiting.parked().keepingAttempts().requeueWait()).isEqualTo(Duration.ofSeconds(30));
        assertThat(waiting.parked().from()).isEqualTo("orders.parked");
        assertThat(replay.waitingForRequeue(Duration.ZERO).requeueWait()).isZero();
    }

    @Test
    @DisplayName("refuses a negative wait")
    void negative_refused() {
        assertThatThrownBy(() -> replay.waitingForRequeue(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
