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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.crypto.SecretKey;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.codec.json.JsonCodec;
import org.acemq.amqp.crypto.EncryptedCodec;
import org.acemq.amqp.crypto.Keyring;
import org.acemq.amqp.crypto.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("payload encryption")
class EncryptedCodecTest {

    private SecretKey key;
    private Keyring keyring;
    private EncryptedCodec codec;

    @BeforeEach
    void setUp() {
        key = Keys.generate();
        keyring = Keyring.of("orders-2026-09", key);
        codec = EncryptedCodec.wrapping(new JsonCodec(), keyring);
    }

    /** Java 11 target, so a plain class rather than a record. */
    public static final class Card {

        private String holder;
        private String number;

        public Card() {
        }

        Card(String holder, String number) {
            this.holder = holder;
            this.number = number;
        }

        public String getHolder() {
            return holder;
        }

        public void setHolder(String holder) {
            this.holder = holder;
        }

        public String getNumber() {
            return number;
        }

        public void setNumber(String number) {
            this.number = number;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Card
                    && Objects.equals(holder, ((Card) other).holder)
                    && Objects.equals(number, ((Card) other).number);
        }

        @Override
        public int hashCode() {
            return Objects.hash(holder, number);
        }
    }

    private static Card card() {
        return new Card("A Person", "4111111111111111");
    }

    @Nested
    @DisplayName("round trip")
    class RoundTrip {

        @Test
        @DisplayName("gives back what went in")
        void roundTrips() {
            assertThat(codec.decode(codec.encode(card()), Card.class)).isEqualTo(card());
        }

        @Test
        @DisplayName("leaves nothing readable on the wire")
        void nothingReadable() {
            byte[] wire = codec.encode(card());

            String asText = new String(wire, StandardCharsets.UTF_8);
            assertThat(asText).doesNotContain("4111111111111111").doesNotContain("A Person");
            // The key identifier is there on purpose, and is the only thing that is.
            assertThat(asText).contains("orders-2026-09");
        }

        @Test
        @DisplayName("says it is ciphertext, not the format underneath")
        void contentTypeIsNotTheDelegates() {
            // A "+json" suffix would make the JSON codec volunteer to parse ciphertext, so the
            // failure would surface in a parser instead of in a codec that knows it cannot help.
            assertThat(codec.contentType()).isEqualTo("application/vnd.acemq.encrypted");
            assertThat(codec.canDecode("application/json")).isFalse();
            assertThat(codec.canDecode(codec.contentType())).isTrue();
            assertThat(codec.canDecode(null)).isFalse();
        }

        @Test
        @DisplayName("encrypts the same payload differently every time")
        void nonceIsFresh() {
            // Equal ciphertexts would mean a reused nonce, which under GCM does not weaken the
            // encryption but forfeits it -- and would also leak that two messages are the same.
            Set<String> seen = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                seen.add(Arrays.toString(codec.encode(card())));
            }

            assertThat(seen).hasSize(20);
        }

        @Test
        @DisplayName("wraps any codec, not only JSON")
        void wrapsAnyDelegate() {
            Codec bytes = EncryptedCodec.wrapping(new PassthroughCodec(), keyring);

            byte[] wire = bytes.encode("plain text".getBytes(StandardCharsets.UTF_8));

            assertThat(new String(bytes.decode(wire, byte[].class), StandardCharsets.UTF_8))
                    .isEqualTo("plain text");
        }
    }

    @Nested
    @DisplayName("key rotation")
    class KeyRotation {

        @Test
        @DisplayName("a message written with the old key is still readable")
        void oldMessagesStillRead() {
            SecretKey newer = Keys.generate();
            byte[] writtenInJune = codec.encode(card());

            // September's service writes with September's key and holds June's for what is
            // still queued. This is the whole of rotation, and the reason the identifier is in
            // the message rather than assumed.
            EncryptedCodec rotated = EncryptedCodec.wrapping(
                    new JsonCodec(),
                    Keyring.builder().add("orders-2026-09", key).current("orders-2026-12", newer).build());

            assertThat(rotated.decode(writtenInJune, Card.class)).isEqualTo(card());
            assertThat(EncryptedCodec.keyIdOf(rotated.encode(card()))).isEqualTo("orders-2026-12");
        }

        @Test
        @DisplayName("a retired key is named, along with what is held instead")
        void retiredKeyIsNamed() {
            byte[] wire = codec.encode(card());

            EncryptedCodec withoutIt = EncryptedCodec.wrapping(
                    new JsonCodec(), Keyring.of("orders-2026-12", Keys.generate()));

            assertThatThrownBy(() -> withoutIt.decode(wire, Card.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("orders-2026-09")
                    .hasMessageContaining("orders-2026-12")
                    .hasMessageContaining("retired");
        }

        @Test
        @DisplayName("an operator can read which key a message needs without holding it")
        void keyIdIsReadableWithoutTheKey() {
            // The question in front of a dead-letter queue nobody can decrypt is almost always
            // "which key does this need", and answering it must not require the key.
            assertThat(EncryptedCodec.keyIdOf(codec.encode(card()))).isEqualTo("orders-2026-09");
        }

        @Test
        @DisplayName("and reads nothing at all from a message written by something else")
        void keyIdOfForeignBytes() {
            assertThat(EncryptedCodec.keyIdOf("{\"holder\":\"A Person\"}".getBytes(StandardCharsets.UTF_8)))
                    .isNull();
            assertThat(EncryptedCodec.keyIdOf(new byte[0])).isNull();
        }

        @Test
        @DisplayName("a keyring with no current key is refused at build time")
        void currentKeyIsRequired() {
            // Rather than at the first publish, which in a service that mostly consumes could be
            // days later and somewhere else entirely.
            assertThatThrownBy(() -> Keyring.builder().add("only-for-reading", key).build())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no current key");
        }
    }

    @Nested
    @DisplayName("refusing")
    class Refusing {

        @Test
        @DisplayName("a message that was not encrypted")
        void plaintextIsRefused() {
            byte[] plainJson = new JsonCodec().encode(card());

            assertThatThrownBy(() -> codec.decode(plainJson, Card.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("not written by EncryptedCodec")
                    .hasMessageContaining("plaintext");
        }

        @Test
        @DisplayName("a message whose ciphertext was altered")
        void tamperingIsRefused() {
            byte[] wire = codec.encode(card());
            wire[wire.length - 1] ^= 0x01;

            assertThatThrownBy(() -> codec.decode(wire, Card.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("altered after it was written");
        }

        @Test
        @DisplayName("a message whose key identifier was altered")
        void alteredHeaderIsRefused() {
            // The header is authenticated as associated data, so swapping the identifier for one
            // the reader does hold gets a failure rather than a decryption under the wrong key.
            SecretKey other = Keys.generate();
            EncryptedCodec twoKeys = EncryptedCodec.wrapping(
                    new JsonCodec(),
                    Keyring.builder().add("key-b", other).current("key-a", key).build());
            byte[] wire = twoKeys.encode(card());
            assertThat(EncryptedCodec.keyIdOf(wire)).isEqualTo("key-a");

            // Rewritten to name the *other* key this reader holds, which is the attack worth
            // testing: not an unknown identifier, which fails on lookup, but a real one, which
            // would decrypt under the wrong key if the header were not authenticated.
            wire[3 + "key-".length()] = 'b';
            assertThat(EncryptedCodec.keyIdOf(wire)).isEqualTo("key-b");

            assertThatThrownBy(() -> twoKeys.decode(wire, Card.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("altered after it was written");
        }

        @Test
        @DisplayName("a message too short to hold a nonce and a tag")
        void truncatedIsRefused() {
            byte[] wire = codec.encode(card());

            byte[] truncated = Arrays.copyOf(wire, 3 + "orders-2026-09".length() + 4);

            assertThatThrownBy(() -> codec.decode(truncated, Card.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("truncated");
        }

        @Test
        @DisplayName("a key identifier too long to fit in the framing")
        void oversizedKeyIdIsRefused() {
            StringBuilder tooLong = new StringBuilder();
            for (int i = 0; i < 256; i++) {
                tooLong.append('k');
            }

            assertThatThrownBy(() -> Keyring.of(tooLong.toString(), key))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("255");
        }
    }

    @Nested
    @DisplayName("keys")
    class TheKeys {

        @Test
        @DisplayName("survive a trip through Base64, which is how they arrive")
        void base64RoundTrips() {
            SecretKey restored = Keys.fromBase64(Keys.toBase64(key));

            byte[] wire = codec.encode(card());
            EncryptedCodec fromConfig = EncryptedCodec.wrapping(
                    new JsonCodec(), Keyring.of("orders-2026-09", restored));

            assertThat(fromConfig.decode(wire, Card.class)).isEqualTo(card());
        }

        @Test
        @DisplayName("of the wrong length are refused, with the reason")
        void wrongLengthIsRefused() {
            assertThatThrownBy(() -> Keys.fromBytes("hunter2".getBytes(StandardCharsets.UTF_8)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("key derivation function");
        }

        @Test
        @DisplayName("never appear in a toString")
        void neverPrinted() {
            String printed = keyring.toString() + " " + codec + " " + keyring.current();

            assertThat(printed).contains("orders-2026-09");
            assertThat(printed).doesNotContain(Keys.toBase64(key));
        }
    }

    /** Hands bytes through untouched, to show the wrapping is not JSON-specific. */
    private static final class PassthroughCodec implements Codec {

        @Override
        public String contentType() {
            return "application/octet-stream";
        }

        @Override
        public byte[] encode(Object payload) {
            return (byte[]) payload;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> T decode(byte[] body, Class<T> target) {
            return (T) body;
        }

        @Override
        public boolean canDecode(String contentType) {
            return true;
        }
    }
}
