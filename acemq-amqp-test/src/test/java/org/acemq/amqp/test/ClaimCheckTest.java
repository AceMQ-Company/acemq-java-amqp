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

import java.nio.file.Path;
import java.util.Arrays;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.ClaimCheckStore;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.codec.json.JsonCodec;
import org.acemq.amqp.patterns.ClaimCheckCodec;
import org.acemq.amqp.patterns.FilesystemClaimCheckStore;
import org.acemq.amqp.patterns.InMemoryClaimCheckStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("the claim check")
class ClaimCheckTest {

    private InMemoryClaimCheckStore store;
    private Codec json;

    @BeforeEach
    void setUp() {
        store = new InMemoryClaimCheckStore();
        json = new JsonCodec();
    }

    /** Java 11 target across the library, so a plain class rather than a record. */
    public static final class Document {

        private String id;
        private String body;

        public Document() {
        }

        Document(String id, String body) {
            this.id = id;
            this.body = body;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }
    }

    private static Document large(int bytes) {
        char[] filler = new char[bytes];
        Arrays.fill(filler, 'x');
        return new Document("d-1", new String(filler));
    }

    @Nested
    @DisplayName("above the threshold")
    class Offloaded {

        @Test
        @DisplayName("the payload goes to the store and the key goes on the wire")
        void offloadsLargePayloads() {
            Codec checked = ClaimCheckCodec.wrapping(json, store, 1024);

            byte[] wire = checked.encode(large(8192));

            // The message is a few dozen bytes whatever the document weighs.
            assertThat(wire.length).isLessThan(100);
            assertThat(store.size()).isEqualTo(1);
            assertThat(checked.decode(wire, Document.class).getBody()).hasSize(8192);
        }

        @Test
        @DisplayName("an operator can read which object a message needs, without fetching it")
        void keyIsReadableFromTheMessage() {
            Codec checked = ClaimCheckCodec.wrapping(json, store, 1024);

            byte[] wire = checked.encode(large(8192));

            // The question in front of a dead-letter queue: which object does this need, and is
            // it still there? Answerable from the message alone.
            String key = ClaimCheckCodec.keyOf(wire);
            assertThat(key).isNotNull();
            assertThat(store.get(key)).isPresent();
        }

        @Test
        @DisplayName("a payload the store has lost says so, and says why")
        void missingPayloadExplainsItself() {
            Codec checked = ClaimCheckCodec.wrapping(json, store, 1024);
            byte[] wire = checked.encode(large(8192));

            // Retention expired underneath a message that outlived it, which is the failure
            // this pattern actually has in production.
            store.clear();

            assertThatThrownBy(() -> checked.decode(wire, Document.class))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("not in the store")
                    .hasMessageContaining("retention");
        }
    }

    @Nested
    @DisplayName("below the threshold")
    class Inline {

        @Test
        @DisplayName("the payload travels inline and the store is untouched")
        void staysInline() {
            Codec checked = ClaimCheckCodec.wrapping(json, store, 1024);

            byte[] wire = checked.encode(new Document("d-1", "small"));

            // Offloading a small message would turn one round trip into two, making the common
            // case slower to fix the rare one.
            assertThat(store.size()).isZero();
            assertThat(ClaimCheckCodec.keyOf(wire)).isNull();
            assertThat(checked.decode(wire, Document.class).getBody()).isEqualTo("small");
        }

        @Test
        @DisplayName("a consumer reads both kinds without being told which it is")
        void readsBothKinds() {
            Codec checked = ClaimCheckCodec.wrapping(json, store, 1024);

            byte[] small = checked.encode(new Document("d-1", "small"));
            byte[] big = checked.encode(large(4096));

            // This is what lets the threshold change, or the codec be introduced, without a
            // flag day.
            assertThat(checked.decode(small, Document.class).getBody()).isEqualTo("small");
            assertThat(checked.decode(big, Document.class).getBody()).hasSize(4096);
        }

        @Test
        @DisplayName("a message written before this codec existed is still readable")
        void readsUnframedMessages() {
            Codec checked = ClaimCheckCodec.wrapping(json, store, 1024);

            // Written by a plain JSON publisher: no framing at all. Adding a claim check to a
            // live queue has to be safe for what is already in it.
            byte[] plain = json.encode(new Document("d-1", "written before"));

            assertThat(checked.decode(plain, Document.class).getBody()).isEqualTo("written before");
        }

        @Test
        @DisplayName("the content type is the delegate's, because the message is still a document")
        void contentTypeIsUnchanged() {
            Codec checked = ClaimCheckCodec.wrapping(json, store, 1024);

            // Unlike encryption, where the bytes really are something else. A claim-checked
            // document is a document that is somewhere else.
            assertThat(checked.contentType()).isEqualTo(json.contentType());
            assertThat(checked.canDecode(json.contentType())).isTrue();
        }
    }

    @Nested
    @DisplayName("the filesystem store")
    class Filesystem {

        @TempDir
        Path directory;

        @Test
        @DisplayName("round trips through a directory")
        void roundTrips() {
            ClaimCheckStore files = new FilesystemClaimCheckStore(directory.resolve("payloads"));
            Codec checked = ClaimCheckCodec.wrapping(json, files, 1024);

            byte[] wire = checked.encode(large(4096));

            assertThat(checked.decode(wire, Document.class).getBody()).hasSize(4096);
        }

        @Test
        @DisplayName("refuses a key that would escape the directory")
        void refusesTraversal() {
            ClaimCheckStore files = new FilesystemClaimCheckStore(directory);

            // A key arrives from a message, so it is whatever a publisher put there. This one
            // is a path, and resolving it would read a file outside the store.
            assertThatThrownBy(() -> files.get("../../etc/passwd"))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("not a key this store issued");
        }

        @Test
        @DisplayName("a key the store never held is absent rather than an error")
        void missingKeyIsEmpty() {
            ClaimCheckStore files = new FilesystemClaimCheckStore(directory);

            assertThat(files.get("11111111-2222-3333-4444-555555555555")).isEmpty();
        }
    }
}
