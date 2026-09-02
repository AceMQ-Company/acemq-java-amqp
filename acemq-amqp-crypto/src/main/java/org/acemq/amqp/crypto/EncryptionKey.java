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
package org.acemq.amqp.crypto;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import javax.crypto.SecretKey;

/**
 * A key and the name messages will refer to it by.
 *
 * <p>The identifier is the part that makes rotation possible. It travels in every message, so a
 * message written last month can still be read by a service whose current key is a different
 * one, without anybody having to know when the change happened.
 *
 * <p><strong>Identifiers are public.</strong> They sit in the clear in front of the ciphertext
 * where anyone holding the message can read them, which is the point — the reader has to know
 * what to ask the key store for before it can decrypt anything. Name them for the key, not for
 * what they protect: {@code orders-2026-09} rather than {@code customer-card-numbers}.
 */
public final class EncryptionKey {

    /**
     * The identifier goes in the framing behind a single length byte, so this is what fits.
     * Long enough for a name and a date; short enough that it is not a place to put a comment.
     */
    static final int MAX_ID_BYTES = 255;

    private final String id;
    private final SecretKey key;

    /**
     * @param id what messages written with this key will name, in ASCII
     * @param key an AES key; 256-bit unless there is a reason
     */
    public EncryptionKey(String id, SecretKey key) {
        this.id = Objects.requireNonNull(id, "id");
        this.key = Objects.requireNonNull(key, "key");
        if (id.isEmpty()) {
            throw new IllegalArgumentException("a key identifier cannot be empty: it is what a"
                    + " reader looks the key up by");
        }
        int length = id.getBytes(StandardCharsets.UTF_8).length;
        if (length > MAX_ID_BYTES) {
            throw new IllegalArgumentException("a key identifier is at most " + MAX_ID_BYTES
                    + " bytes and '" + id + "' is " + length + ". It travels in front of every"
                    + " message, so it is a name rather than a description.");
        }
    }

    /** @return the identifier written into every message this key encrypts */
    public String id() {
        return id;
    }

    /** @return the key itself */
    public SecretKey key() {
        return key;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof EncryptionKey)) {
            return false;
        }
        EncryptionKey that = (EncryptionKey) other;
        return id.equals(that.id) && key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, key);
    }

    /** @return the identifier and nothing else. The key must not appear in a log */
    @Override
    public String toString() {
        return "EncryptionKey{id=" + id + "}";
    }
}
