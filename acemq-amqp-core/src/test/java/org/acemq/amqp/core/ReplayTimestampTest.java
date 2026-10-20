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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** AMQP-ENV-013: a replay timestamp is millisecond ISO-8601 in UTC, whatever the clock holds. */
@DisplayName("the replay timestamp")
class ReplayTimestampTest {

    @Test
    @DisplayName("has exactly three fraction digits and a Z, on a whole second and on a fine clock")
    void millisecond_precision_always() {
        assertThat(EnvelopeHeaders.rfc3339(Instant.parse("2026-02-03T04:05:06Z")))
                .isEqualTo("2026-02-03T04:05:06.000Z");
        assertThat(EnvelopeHeaders.rfc3339(Instant.parse("2026-02-03T04:05:06.789123456Z")))
                .isEqualTo("2026-02-03T04:05:06.789Z");
    }
}
