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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class StreamPositionTest {

    private final Map<String, Object> arguments = new HashMap<>(Map.of(StreamPosition.ARGUMENT, "first"));
    private final StreamPosition position = new StreamPosition(arguments);

    @Test
    void keeps_its_own_start_when_nothing_was_delivered() {
        position.rewind();
        assertThat(arguments).containsEntry(StreamPosition.ARGUMENT, "first");
    }

    @Test
    void resumes_one_past_the_newest_settled() {
        for (long offset = 0; offset < 5; offset++) {
            position.settle(offset, position.delivered(offset));
        }
        position.rewind();
        assertThat(arguments).containsEntry(StreamPosition.ARGUMENT, 5L);
    }

    @Test
    void resumes_at_the_oldest_unsettled() {
        int t3 = position.delivered(3);
        position.delivered(4);
        int t5 = position.delivered(5);
        position.settle(3, t3);
        position.settle(5, t5);
        position.rewind();
        assertThat(arguments).containsEntry(StreamPosition.ARGUMENT, 4L);
    }

    @Test
    void a_late_settle_from_before_the_recovery_does_not_move_it() {
        int old = position.delivered(10);
        position.rewind();
        assertThat(arguments).containsEntry(StreamPosition.ARGUMENT, 10L);

        // The old copy finishes after the recovery; the new copy of 10 is still out.
        position.delivered(10);
        position.settle(10, old);
        position.rewind();
        assertThat(arguments).containsEntry(StreamPosition.ARGUMENT, 10L);
    }

    @Test
    void reads_the_offset_the_broker_stamps() {
        assertThat(StreamPosition.offsetOf(Map.of("x-stream-offset", 42L))).isEqualTo(42L);
        assertThat(StreamPosition.offsetOf(Map.of())).isNull();
        assertThat(StreamPosition.offsetOf(null)).isNull();
    }
}
