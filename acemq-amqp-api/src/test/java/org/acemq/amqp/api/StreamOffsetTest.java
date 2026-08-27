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
import java.time.Instant;
import java.util.Date;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("a stream offset")
class StreamOffsetTest {

    @Test
    void the_keywords_go_to_the_broker_as_the_words_it_knows() {
        assertThat(StreamOffset.first().toConsumerArgument()).isEqualTo("first");
        assertThat(StreamOffset.last().toConsumerArgument()).isEqualTo("last");
        assertThat(StreamOffset.next().toConsumerArgument()).isEqualTo("next");
    }

    @Test
    void an_absolute_position_goes_as_a_number_and_not_as_its_text() {
        Object argument = StreamOffset.at(4200).toConsumerArgument();

        // The type is the meaning here. The broker reads a string as a keyword and a number as
        // an offset, so "4200" would not be rejected — it would be a keyword it does not know.
        assertThat(argument).isInstanceOf(Long.class).isEqualTo(4200L);
    }

    @Test
    void a_moment_in_time_goes_as_a_date() {
        Instant when = Instant.parse("2026-01-02T03:04:05Z");

        assertThat(StreamOffset.from(when).toConsumerArgument()).isEqualTo(Date.from(when));
    }

    @Test
    void a_period_goes_as_seconds_however_it_was_expressed() {
        // Rounding to days or hours would quietly move where reading starts.
        assertThat(StreamOffset.lastly(Duration.ofHours(1)).toConsumerArgument()).isEqualTo("3600s");
        assertThat(StreamOffset.lastly(Duration.ofMinutes(90)).toConsumerArgument()).isEqualTo("5400s");
        assertThat(StreamOffset.lastly(Duration.ofSeconds(30)).toConsumerArgument()).isEqualTo("30s");
    }

    @Test
    void refuses_positions_that_cannot_mean_anything() {
        assertThatThrownBy(() -> StreamOffset.at(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be negative");
        assertThatThrownBy(() -> StreamOffset.lastly(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be positive");
        assertThatThrownBy(() -> StreamOffset.lastly(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> StreamOffset.from(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void two_of_the_same_position_are_the_same_offset() {
        Instant when = Instant.parse("2026-01-02T03:04:05Z");

        assertThat(StreamOffset.at(7)).isEqualTo(StreamOffset.at(7)).hasSameHashCodeAs(StreamOffset.at(7));
        assertThat(StreamOffset.from(when)).isEqualTo(StreamOffset.from(when));
        assertThat(StreamOffset.first()).isEqualTo(StreamOffset.first()).isNotEqualTo(StreamOffset.next());
        assertThat(StreamOffset.at(7)).isNotEqualTo(StreamOffset.at(8)).isNotEqualTo("not an offset");
        assertThat(StreamOffset.lastly(Duration.ofHours(1)))
                .isEqualTo(StreamOffset.lastly(Duration.ofMinutes(60)));
    }

    @Test
    void says_where_it_points_in_its_own_words() {
        assertThat(StreamOffset.first()).hasToString("StreamOffset{first}");
        assertThat(StreamOffset.at(9)).hasToString("StreamOffset{9}");
    }
}
