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
package org.acemq.amqp.patterns;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.ClaimCheckStore;
import org.acemq.amqp.api.Codec;
import org.jspecify.annotations.Nullable;

/**
 * Keeps large payloads off the broker.
 *
 * <pre>{@code
 * Codec checked = ClaimCheckCodec.wrapping(new JsonCodec(), store);
 *
 * mq.publisher("policies", "document.stored", Document.class).as(checked);
 * mq.consume("policies.documents", Document.class, checked, m -> read(m.payload()));
 * }</pre>
 *
 * <p>A scanned medical report is tens of megabytes. Putting it on a queue is possible and is a
 * mistake: it fills the broker's memory, it is copied to every bound queue, it makes a
 * dead-letter queue impossible to inspect, and it turns a broker into a filesystem with worse
 * tools. What travels instead is a <strong>claim check</strong> — the payload goes to a
 * {@link ClaimCheckStore}, and the message carries the key.
 *
 * <h2>Only when it is worth it</h2>
 *
 * <p>Below {@linkplain #wrapping(Codec, ClaimCheckStore, int) the threshold} the payload travels
 * inline, exactly as it would without this codec. That matters more than it sounds: offloading
 * a two-hundred-byte message turns one broker round trip into a store round trip <em>and</em> a
 * broker round trip, so an unconditional claim check makes the common case slower to fix the
 * rare one.
 *
 * <p>The framing therefore says which of the two it is, and a consumer handles both without
 * being told. That is what allows the threshold to be changed, or this codec to be introduced,
 * without a flag day: messages written before the change are still readable after it.
 *
 * <h2>What is on the wire</h2>
 *
 * <pre>
 * 0xAC  0x01  0x00  payload             inline, and identical to what the delegate wrote
 * 0xAC  0x01  0x01  key                 a claim check
 * </pre>
 *
 * <p>The content type is the delegate's, unchanged — unlike encryption, where the bytes really
 * are something else. A claim-checked message is still a document; it is a document that is
 * somewhere else, and a consumer that lacks the store gets a clear failure rather than a
 * parser error.
 */
public final class ClaimCheckCodec implements Codec {

    /** Marks this codec's framing. */
    private static final byte MAGIC = (byte) 0xAC;

    private static final byte VERSION = 0x01;
    private static final byte INLINE = 0x00;
    private static final byte CHECKED = 0x01;
    private static final int HEADER = 3;

    /**
     * Below this, payloads travel inline.
     *
     * <p>64 KiB: comfortably above an ordinary event and comfortably below the size at which a
     * broker starts to care. RabbitMQ will accept far larger, which is the problem — nothing
     * refuses a 40 MB message, it simply makes everything worse afterwards.
     */
    public static final int DEFAULT_THRESHOLD = 64 * 1024;

    private final Codec delegate;
    private final ClaimCheckStore store;
    private final int threshold;

    private ClaimCheckCodec(Codec delegate, ClaimCheckStore store, int threshold) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.store = Objects.requireNonNull(store, "store");
        if (threshold < 0) {
            throw new IllegalArgumentException("a threshold cannot be negative, was " + threshold);
        }
        this.threshold = threshold;
    }

    /**
     * @param delegate the codec that turns objects into bytes
     * @param store where large payloads go
     * @return a codec offloading anything above {@link #DEFAULT_THRESHOLD}
     */
    public static ClaimCheckCodec wrapping(Codec delegate, ClaimCheckStore store) {
        return new ClaimCheckCodec(delegate, store, DEFAULT_THRESHOLD);
    }

    /**
     * @param delegate the codec that turns objects into bytes
     * @param store where large payloads go
     * @param threshold payloads of at least this many bytes are offloaded; zero offloads
     *     everything, which is occasionally what a store-backed audit trail wants
     * @return a codec offloading anything at or above the threshold
     */
    public static ClaimCheckCodec wrapping(Codec delegate, ClaimCheckStore store, int threshold) {
        return new ClaimCheckCodec(delegate, store, threshold);
    }

    @Override
    public String contentType() {
        // The delegate's. A claim-checked document is still a document.
        return delegate.contentType();
    }

    @Override
    public byte[] encode(Object payload) {
        byte[] encoded = delegate.encode(payload);
        if (encoded.length < threshold) {
            return frame(INLINE, encoded);
        }
        return frame(CHECKED, store.put(encoded).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public <T> T decode(byte[] body, Class<T> target) {
        if (!isFramed(body)) {
            // Written before this codec was introduced, or by a publisher that does not use it.
            // Reading it as the delegate would is the only useful answer, and it is what makes
            // adding a claim check to a live queue safe.
            return delegate.decode(body, target);
        }

        byte[] rest = Arrays.copyOfRange(body, HEADER, body.length);
        if (body[2] == INLINE) {
            return delegate.decode(rest, target);
        }

        String key = new String(rest, StandardCharsets.UTF_8);
        byte[] content = store.get(key).orElseThrow(() -> new AceMqException(
                "the claim check '" + key + "' is not in the store, so this message cannot be read."
                        + " The payload was removed while a message referring to it was still"
                        + " deliverable -- the store's retention has to outlast every queue, every"
                        + " dead-letter queue, and any replay somebody might do by hand."));
        return delegate.decode(content, target);
    }

    @Override
    public boolean canDecode(@Nullable String contentType) {
        // Whatever the delegate accepts. A claim check does not change what the message is.
        return delegate.canDecode(contentType);
    }

    /**
     * Reads the key a message refers to, without fetching it.
     *
     * <p>For the operator looking at a dead-letter queue: which object does this need, and is it
     * still in the store? Answering that from the message alone is the difference between a
     * five-minute check and restoring a backup.
     *
     * @param body a message body
     * @return the key, or {@code null} when the payload travelled inline or this codec did not
     *     write the message
     */
    public static @Nullable String keyOf(byte[] body) {
        if (!isFramed(body) || body[2] != CHECKED) {
            return null;
        }
        return new String(body, HEADER, body.length - HEADER, StandardCharsets.UTF_8);
    }

    /** @return the wrapped codec, whose output is what gets stored or inlined */
    public Codec delegate() {
        return delegate;
    }

    private static boolean isFramed(byte @Nullable [] body) {
        return body != null && body.length >= HEADER && body[0] == MAGIC && body[1] == VERSION
                && (body[2] == INLINE || body[2] == CHECKED);
    }

    private static byte[] frame(byte kind, byte[] rest) {
        byte[] framed = new byte[HEADER + rest.length];
        framed[0] = MAGIC;
        framed[1] = VERSION;
        framed[2] = kind;
        System.arraycopy(rest, 0, framed, HEADER, rest.length);
        return framed;
    }

    @Override
    public String toString() {
        return "ClaimCheckCodec{" + delegate + ", above=" + threshold + " bytes}";
    }
}
