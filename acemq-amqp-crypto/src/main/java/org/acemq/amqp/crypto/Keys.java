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

import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Objects;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/** Getting an AES key from the shapes one usually arrives in. */
public final class Keys {

    private static final String AES = "AES";

    private Keys() {
    }

    /**
     * @return a new 256-bit key from the platform's secure random
     */
    public static SecretKey generate() {
        try {
            KeyGenerator generator = KeyGenerator.getInstance(AES);
            generator.init(256);
            return generator.generateKey();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("AES is required of every JVM and is missing", e);
        }
    }

    /**
     * @param bytes 16, 24 or 32 bytes of key material
     * @return the key
     */
    public static SecretKey fromBytes(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (bytes.length != 16 && bytes.length != 24 && bytes.length != 32) {
            throw new IllegalArgumentException("an AES key is 16, 24 or 32 bytes and this is "
                    + bytes.length + ". If this came from a passphrase, it needs a key derivation"
                    + " function such as PBKDF2 or Argon2 rather than being used as it stands.");
        }
        return new SecretKeySpec(bytes, AES);
    }

    /**
     * The usual way a key arrives from a secret store or an environment variable.
     *
     * @param base64 key material, Base64 encoded
     * @return the key
     */
    public static SecretKey fromBase64(String base64) {
        Objects.requireNonNull(base64, "base64");
        try {
            return fromBytes(Base64.getDecoder().decode(base64.trim()));
        } catch (IllegalArgumentException e) {
            // Without the value: whatever this was, it was meant to be a key, and a key does not
            // belong in an exception message that will be logged.
            throw new IllegalArgumentException("could not read a key from Base64: " + e.getMessage(), e);
        }
    }

    /**
     * For writing a generated key into a secret store.
     *
     * @param key the key
     * @return its material, Base64 encoded
     */
    public static String toBase64(SecretKey key) {
        return Base64.getEncoder().encodeToString(Objects.requireNonNull(key, "key").getEncoded());
    }
}
