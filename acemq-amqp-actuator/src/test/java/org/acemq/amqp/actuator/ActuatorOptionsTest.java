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
package org.acemq.amqp.actuator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** The settings, and the mistakes they refuse to be built with. */
class ActuatorOptionsTest {

    @Test
    void theDefaultsAreTheOnesEveryLibraryInTheFamilyUses() {
        ActuatorOptions options = ActuatorOptions.builder().build();

        assertThat(options.port()).isEqualTo(9464);
        assertThat(options.metricsPath()).isEqualTo("/acemq-metrics");
        assertThat(options.healthPath()).isEqualTo("/acemq-health");
        assertThat(options.infoPath()).isEqualTo("/acemq-info");
        // Loopback, because these endpoints are unauthenticated and name queues.
        assertThat(options.bindAddress()).isEqualTo("127.0.0.1");
        assertThat(options.attachToGlobalRegistry()).isTrue();
        assertThat(options.connection()).isNull();
        assertThat(options.registry()).isNull();
    }

    @Test
    void aPathWithoutALeadingSlashIsRefusedRatherThanQuietlyNeverMatched() {
        assertThatThrownBy(() -> ActuatorOptions.builder().metricsPath("acemq-metrics"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must start with a slash");
    }

    @Test
    void twoPathsTheSameWouldShadowOneEndpointWithAnother() {
        assertThatThrownBy(() -> ActuatorOptions.builder().healthPath("/acemq-metrics").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("the three paths must differ");
    }

    @Test
    void aPortOutsideTheRangeIsRefusedHereRatherThanByTheSocket() {
        assertThatThrownBy(() -> ActuatorOptions.builder().port(70000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("between 0 and 65535");
    }

    @Test
    void whatTheApplicationCallsItselfIsCarriedThrough() {
        ActuatorOptions options = ActuatorOptions.builder().application("orders", "1.4.2").build();

        assertThat(options.applicationName()).isEqualTo("orders");
        assertThat(options.applicationVersion()).isEqualTo("1.4.2");
    }
}
