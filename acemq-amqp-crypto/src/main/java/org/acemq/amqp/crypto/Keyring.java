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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.crypto.SecretKey;

import org.acemq.amqp.api.AceMqException;

/**
 * The keys a service can write with and the keys it can read with.
 *
 * <p>Those are not the same set, and that asymmetry is the whole of key rotation. A service
 * writes with exactly one key — the current one — and must be able to read with every key any
 * message still in flight was written with. Rotating means adding a key and making it current,
 * while the old ones stay readable until nothing written with them is left.
 *
 * <pre>{@code
 * Keyring keys = Keyring.builder()
 *         .add("orders-2026-06", june)     // still on some queue somewhere
 *         .current("orders-2026-09", now)  // everything new
 *         .build();
 * }</pre>
 *
 * <p>Two methods, so a real one backed by a key management service is a small class. Implement
 * it against AWS KMS, Vault, or whatever already holds your secrets; {@link #builder()} is for
 * keys that arrive as configuration.
 *
 * <p><strong>Implementations should cache.</strong> {@link #keyFor(String)} is called for every
 * message decoded, and a key management service charged per call will notice.
 */
public interface Keyring {

    /**
     * @return the key to write with. Consulted per message, so a keyring that reloads from a
     *     secret store can change what this returns and the next message uses the new key
     */
    EncryptionKey current();

    /**
     * @param keyId an identifier read out of a message
     * @return the key it names
     * @throws AceMqException if this keyring does not hold it. That is the normal shape of "a
     *     key was retired too early", so the message should say which identifier was missing
     */
    SecretKey keyFor(String keyId);

    /**
     * @param id what messages will name the key
     * @param key the key
     * @return a keyring holding one key, which both writes and reads
     */
    static Keyring of(String id, SecretKey key) {
        return builder().current(id, key).build();
    }

    /** @return a builder for a keyring whose keys are known up front */
    static Builder builder() {
        return new Builder();
    }

    /** Collects keys for a {@link Keyring} that does not change while the process runs. */
    final class Builder {

        private final Map<String, SecretKey> keys = new LinkedHashMap<>();
        private String currentId = "";

        private Builder() {
        }

        /**
         * Adds a key that can be read with but not written with.
         *
         * @param id the identifier messages carry
         * @param key the key
         * @return this builder
         */
        public Builder add(String id, SecretKey key) {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(key, "key");
            keys.put(id, key);
            return this;
        }

        /**
         * Adds a key and makes it the one to write with.
         *
         * @param id the identifier messages will carry
         * @param key the key
         * @return this builder
         */
        public Builder current(String id, SecretKey key) {
            add(id, key);
            currentId = id;
            return this;
        }

        /**
         * @return the keyring
         * @throws IllegalStateException if no key was made current, because a keyring that
         *     cannot say what to write with is only half a keyring and the half that is missing
         *     would not be noticed until the first publish
         */
        public Keyring build() {
            if (currentId.isEmpty()) {
                throw new IllegalStateException("no current key: call current(id, key) for the one"
                        + " to write with. add(...) alone builds a keyring that can read and not write.");
            }
            return new StaticKeyring(keys, currentId);
        }
    }

    /** A keyring over a fixed map. */
    final class StaticKeyring implements Keyring {

        private final Map<String, SecretKey> keys;
        private final EncryptionKey current;

        private StaticKeyring(Map<String, SecretKey> keys, String currentId) {
            this.keys = Collections.unmodifiableMap(new LinkedHashMap<>(keys));
            SecretKey key = this.keys.get(currentId);
            if (key == null) {
                throw new IllegalStateException("the current key '" + currentId + "' is not in the keyring");
            }
            this.current = new EncryptionKey(currentId, key);
        }

        @Override
        public EncryptionKey current() {
            return current;
        }

        @Override
        public SecretKey keyFor(String keyId) {
            SecretKey key = keys.get(keyId);
            if (key == null) {
                // Naming what is held is safe -- identifiers travel in the clear anyway -- and it
                // is the difference between "the key was retired too early" and "this message came
                // from somewhere else", which are different problems with different fixes.
                List<String> held = new ArrayList<>(keys.keySet());
                throw new AceMqException("this message was encrypted with key '" + keyId + "', which"
                        + " is not in the keyring. It holds " + held + ". Either the key was retired"
                        + " while messages written with it were still queued, or the message came"
                        + " from a service using a different keyring.");
            }
            return key;
        }

        /** @return the identifiers, never the keys */
        @Override
        public String toString() {
            return "Keyring{current=" + current.id() + ", holds=" + keys.keySet() + "}";
        }
    }
}
