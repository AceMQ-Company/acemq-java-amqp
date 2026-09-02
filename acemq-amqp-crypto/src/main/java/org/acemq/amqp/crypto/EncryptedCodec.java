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
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Locale;
import java.util.Objects;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.jspecify.annotations.Nullable;

/**
 * Encrypts whatever another codec produced, so the broker holds ciphertext.
 *
 * <pre>{@code
 * Codec encrypted = EncryptedCodec.wrapping(new JsonCodec(), keyring);
 *
 * mq.publisher("payments", "card.stored", Card.class).as(encrypted);
 * mq.consume("payments.stored", Card.class, encrypted, m -> store(m.payload()));
 * }</pre>
 *
 * <p>It wraps a delegate rather than serialising anything itself, so choosing a format and
 * choosing to encrypt stay independent: JSON in, AES-GCM out, and Avro just as well.
 *
 * <h2>What is on the wire</h2>
 *
 * <pre>
 * 0xAE  0x01  len  key identifier   12-byte nonce   ciphertext + 16-byte tag
 * </pre>
 *
 * <p><strong>The key identifier is in the message, in the clear.</strong> That is deliberate,
 * and it is what makes rotation possible: a consumer reads which key a message needs rather
 * than assuming the current one, so a new key can be introduced while messages written with the
 * old one are still queued. Putting it in an AMQP header instead would have been tidier and
 * would have lost it — headers are dropped by shovels, rewritten by federation, and absent from
 * a message recovered out of a backup, and a ciphertext whose key nobody can name is gone.
 *
 * <p>The header is authenticated but not encrypted: GCM binds it as associated data, so an
 * altered key identifier fails to decrypt rather than quietly decrypting as something else.
 *
 * <h2>What this does not do</h2>
 *
 * <p>The broker can no longer read the message, and neither can the people who operate it.
 * <strong>Decide what they do instead before turning this on</strong>: a dead-letter queue full
 * of ciphertext is a queue nobody can triage, and the answer is usually a small internal tool
 * holding the keyring rather than the management UI. See
 * {@link #keyIdOf(byte[])}, which tells an operator which key a message needs without holding
 * any of them.
 *
 * <p>Encryption is not authorisation. Every service holding the keyring can read every message
 * encrypted with those keys; the granularity is the key, so separate audiences mean separate
 * keys.
 *
 * <p>Nor does it hide the routing. Exchange, routing key, headers and message size stay in the
 * clear, and for many systems the routing key is the sensitive part.
 */
public final class EncryptedCodec implements Codec {

    /**
     * Deliberately not {@code ...+json}, whatever the plaintext underneath is.
     *
     * <p>A {@code +json} suffix is a promise that the bytes on the wire are JSON, and every
     * JSON-aware consumer reads it that way. These bytes are ciphertext. Naming them
     * {@code +json} makes the JSON codec volunteer to decode them, which is how a message ends
     * up failing in a parser rather than being refused by a codec that knows it cannot help.
     */
    public static final String CONTENT_TYPE = "application/vnd.acemq.encrypted";

    /** Marks the framing as this codec's, so a message from elsewhere is refused rather than decrypted. */
    private static final byte MAGIC = (byte) 0xAE;

    /** Version 1. Present so a later framing can be told apart from this one by its first two bytes. */
    private static final byte VERSION = 0x01;

    private static final int NONCE_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final int TAG_BYTES = TAG_BITS / 8;

    private final Codec delegate;
    private final Keyring keyring;
    private final SecureRandom random = new SecureRandom();

    private EncryptedCodec(Codec delegate, Keyring keyring) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.keyring = Objects.requireNonNull(keyring, "keyring");
    }

    /**
     * @param delegate the codec that turns objects into bytes; those bytes are what gets encrypted
     * @param keyring the keys to write with and read with
     * @return a codec that encrypts the delegate's output
     */
    public static EncryptedCodec wrapping(Codec delegate, Keyring keyring) {
        return new EncryptedCodec(delegate, keyring);
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public byte[] encode(Object payload) {
        EncryptionKey key = keyring.current();
        byte[] keyId = key.id().getBytes(StandardCharsets.UTF_8);
        byte[] header = new byte[3 + keyId.length];
        header[0] = MAGIC;
        header[1] = VERSION;
        header[2] = (byte) keyId.length;
        System.arraycopy(keyId, 0, header, 3, keyId.length);

        // A fresh nonce per message. Reusing one under GCM does not weaken the encryption, it
        // forfeits it: two messages under the same key and nonce leak their difference outright.
        byte[] nonce = new byte[NONCE_BYTES];
        random.nextBytes(nonce);

        byte[] plaintext = delegate.encode(payload);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key.key(), new GCMParameterSpec(TAG_BITS, nonce));
            cipher.updateAAD(header);
            byte[] ciphertext = cipher.doFinal(plaintext);

            byte[] framed = new byte[header.length + nonce.length + ciphertext.length];
            System.arraycopy(header, 0, framed, 0, header.length);
            System.arraycopy(nonce, 0, framed, header.length, nonce.length);
            System.arraycopy(ciphertext, 0, framed, header.length + nonce.length, ciphertext.length);
            return framed;
        } catch (GeneralSecurityException e) {
            // Without the payload in the message. An exception that helpfully prints what could
            // not be encrypted writes the plaintext to the log, which is the one place it was
            // never supposed to reach.
            throw new AceMqException("could not encrypt a " + payload.getClass().getName()
                    + " with key '" + key.id() + "'", e);
        }
    }

    @Override
    public <T> T decode(byte[] body, Class<T> target) {
        String keyId = keyIdOf(body);
        if (keyId == null) {
            throw new AceMqException("this message was not written by EncryptedCodec: it does not"
                    + " start with the framing this codec writes. A consumer configured to decrypt"
                    + " has been pointed at a queue carrying plaintext.");
        }
        int headerLength = 3 + keyId.getBytes(StandardCharsets.UTF_8).length;
        if (body.length < headerLength + NONCE_BYTES + TAG_BYTES) {
            throw new AceMqException("this message is too short to hold a nonce and an"
                    + " authentication tag, so it was truncated somewhere between being written"
                    + " and being read.");
        }

        SecretKey key = keyring.keyFor(keyId);
        // The plaintext is not wiped after decoding, deliberately: a delegate such as a bytes
        // codec may hand the array straight back as the payload, and wiping it would blank the
        // message the application is about to read. Keeping plaintext out of memory needs the
        // whole path to cooperate, and this one cannot promise it alone.
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(
                    TAG_BITS, body, headerLength, NONCE_BYTES));
            cipher.updateAAD(body, 0, headerLength);
            int from = headerLength + NONCE_BYTES;
            byte[] plaintext = cipher.doFinal(body, from, body.length - from);
            return delegate.decode(plaintext, target);
        } catch (GeneralSecurityException e) {
            // GCM authenticates as well as encrypts, so this is equally what a tampered message
            // looks like. Both get the same answer: it does not reach the application.
            throw new AceMqException("could not decrypt a message onto " + target.getName()
                    + " with key '" + keyId + "'. Either that is not the key it was written with,"
                    + " or the message was altered after it was written.", e);
        }
    }

    @Override
    public boolean canDecode(@Nullable String contentType) {
        // Only its own. Volunteering for anything else means trying to decrypt plaintext and
        // reporting the failure as a decode error, which sends whoever is debugging it in
        // precisely the wrong direction.
        return contentType != null && contentType.toLowerCase(Locale.ROOT).startsWith(CONTENT_TYPE);
    }

    /**
     * Reads which key a message needs, without needing the key.
     *
     * <p>For the operator looking at a dead-letter queue they can no longer read. The
     * identifier is in the clear in front of the ciphertext, so this answers "which key does
     * this need?" from the bytes alone — which is usually the question, because a queue full of
     * undecryptable messages is normally a key that was retired too early rather than anything
     * wrong with the messages.
     *
     * @param body a message body
     * @return the key identifier, or {@code null} if this was not written by this codec
     */
    public static @Nullable String keyIdOf(byte[] body) {
        if (body == null || body.length < 3 || body[0] != MAGIC || body[1] != VERSION) {
            return null;
        }
        int keyIdLength = body[2] & 0xFF;
        if (keyIdLength == 0 || body.length < 3 + keyIdLength) {
            return null;
        }
        return new String(body, 3, keyIdLength, StandardCharsets.UTF_8);
    }

    /** @return the wrapped codec, whose output is what gets encrypted */
    public Codec delegate() {
        return delegate;
    }

    @Override
    public String toString() {
        return "EncryptedCodec{" + delegate + ", key=" + keyring.current().id() + "}";
    }
}
