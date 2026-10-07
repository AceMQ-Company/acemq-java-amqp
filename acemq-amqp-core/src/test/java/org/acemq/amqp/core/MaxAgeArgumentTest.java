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

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("x-max-age is spelled the way Go, Python and Ruby spell it")
class MaxAgeArgumentTest {

    @Test
    void the_largest_exact_unit_wins() {
        assertThat(AceMq.maxAgeArgument(Duration.ofHours(1))).isEqualTo("1h");
        assertThat(AceMq.maxAgeArgument(Duration.ofMinutes(90))).isEqualTo("90m");
        assertThat(AceMq.maxAgeArgument(Duration.ofDays(2))).isEqualTo("2D");
        assertThat(AceMq.maxAgeArgument(Duration.ofSeconds(90))).isEqualTo("90s");
        assertThat(AceMq.maxAgeArgument(Duration.ofHours(36))).isEqualTo("36h");
    }

    @Test
    void a_fraction_of_a_second_is_dropped() {
        assertThat(AceMq.maxAgeArgument(Duration.ofMillis(90_500))).isEqualTo("90s");
    }
}
