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
package org.acemq.amqp.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.codec.toml.TomlCodec;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("TOML")
class TomlCodecTest {

    /** Configuration is the case TOML is for, so the test payload is some. */
    public static final class FeatureFlags {

        private String service;
        private boolean enabled;
        private int rollout;

        public FeatureFlags() {
            // for the codec
        }

        FeatureFlags(String service, boolean enabled, int rollout) {
            this.service = service;
            this.enabled = enabled;
            this.rollout = rollout;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getRollout() {
            return rollout;
        }

        public void setRollout(int rollout) {
            this.rollout = rollout;
        }
    }

    private AceMq mq;

    @AfterEach
    void tearDown() {
        if (mq != null && mq.isOpen()) {
            mq.close();
        }
        InMemoryTransport.reset();
    }

    @Nested
    @DisplayName("the round trip")
    class RoundTrip {

        @Test
        void carries_an_object_out_and_back() {
            Codec codec = new TomlCodec();

            byte[] encoded = codec.encode(new FeatureFlags("pricing", true, 25));
            FeatureFlags decoded = codec.decode(encoded, FeatureFlags.class);

            assertThat(decoded.getService()).isEqualTo("pricing");
            assertThat(decoded.isEnabled()).isTrue();
            assertThat(decoded.getRollout()).isEqualTo(25);
        }

        @Test
        void writes_something_a_person_can_read_and_edit() {
            String written = new String(new TomlCodec().encode(new FeatureFlags("pricing", true, 25)),
                    StandardCharsets.UTF_8);

            // The entire reason to choose TOML over JSON for this kind of message.
            assertThat(written).contains("service = 'pricing'").contains("enabled = true");
        }

        @Test
        void is_reachable_by_name_like_every_other_format() {
            assertThat(Codecs.byName("toml")).isInstanceOf(TomlCodec.class);
            assertThat(Codecs.names()).contains("toml");
        }

        @Test
        void tolerates_a_key_it_has_never_heard_of() {
            // A person edited the file and added something, or a newer publisher sent a field
            // this consumer predates. Failing here would make TOML useless for the one job it
            // is good at.
            byte[] extra = "service = 'pricing'\nenabled = true\nrollout = 25\nowner = 'ada'\n"
                    .getBytes(StandardCharsets.UTF_8);

            FeatureFlags decoded = new TomlCodec().decode(extra, FeatureFlags.class);

            assertThat(decoded.getService()).isEqualTo("pricing");
        }
    }

    @Nested
    @DisplayName("what it will not do")
    class Limits {

        @Test
        void refuses_a_top_level_list_rather_than_writing_something_unreadable() {
            // Jackson does not fail on this, which is why the codec checks. Left to it, a list
            // is written as " = ['a', 'b']" -- a key-less assignment that is not TOML, and that
            // Jackson's own parser then refuses with "Got KEY_VAL_SEP, expected key or table".
            // Publishing it produces a message nothing can read, found by the consumer rather
            // than the publisher.
            assertThatThrownBy(() -> new TomlCodec().encode(Arrays.asList("a", "b")))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("top level has to be an object");
        }

        @Test
        void refuses_a_bare_string_and_a_bare_number_for_the_same_reason() {
            assertThatThrownBy(() -> new TomlCodec().encode("just a string"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("has no TOML representation");
            assertThatThrownBy(() -> new TomlCodec().encode(42))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("has no TOML representation");
        }

        @Test
        void everything_it_does_write_can_be_read_back() {
            // The property the guard exists to preserve, stated as a test rather than trusted:
            // whatever this codec emits, it can parse.
            byte[] written = new TomlCodec().encode(new FeatureFlags("pricing", true, 25));

            assertThat(new TomlCodec().decode(written, FeatureFlags.class).getService())
                    .isEqualTo("pricing");
        }

        @Test
        void does_not_answer_for_a_message_that_named_no_format() {
            // JSON is what an unlabelled message almost always is. Answering would be wrong
            // about the format while possibly right about the value, which is the worst case.
            assertThat(new TomlCodec().canDecode(null)).isFalse();
        }

        @Test
        void claims_only_content_types_that_say_toml() {
            Codec codec = new TomlCodec();

            assertThat(codec.contentType()).isEqualTo("application/toml");
            assertThat(codec.canDecode("application/toml")).isTrue();
            assertThat(codec.canDecode("text/toml")).isTrue();
            assertThat(codec.canDecode("application/vnd.acme.settings+toml")).isTrue();
            assertThat(codec.canDecode("application/json")).isFalse();
            assertThat(codec.canDecode("application/yaml")).isFalse();
        }
    }

    @Nested
    @DisplayName("through a broker")
    class OverTheWire {

        @Test
        @Timeout(30)
        void a_publisher_and_a_consumer_agree_on_it() {
            mq = AceMq.connect("memory://toml", Telemetry.NONE);
            mq.declareExchange("config", "topic");
            mq.declareQueue("config.changes", QueueType.CLASSIC, Collections.emptyMap());
            mq.bind("config.changes", "config", "config.*");

            List<String> received = new CopyOnWriteArrayList<>();
            try (MessageConsumer consumer = mq.consume("config.changes", FeatureFlags.class,
                    ConsumerOptions.defaults().as(Codecs.byName("toml")),
                    message -> received.add(message.payload().getService() + "="
                            + message.payload().getRollout()))) {

                mq.publisher("config", "config.changed", FeatureFlags.class)
                        .as(Codecs.byName("toml"))
                        .send(new FeatureFlags("pricing", true, 25));

                await().atMost(Duration.ofSeconds(10)).until(() -> received.size() == 1);
                assertThat(received).containsExactly("pricing=25");
                assertThat(consumer.rejected()).isZero();
            }
        }
    }
}
