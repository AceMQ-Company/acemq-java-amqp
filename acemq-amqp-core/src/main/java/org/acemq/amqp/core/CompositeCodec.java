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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.jspecify.annotations.Nullable;

/**
 * Writes in one format and reads several.
 *
 * <p>This is what makes changing format survivable. A queue during a cutover holds messages
 * written by the old producers and the new ones at the same time, and a consumer that understands
 * only one of them either loses the others or has to be deployed in lockstep with every producer.
 * Reading both for a while, and writing only the new one, turns a flag day into two ordinary
 * releases: deploy the consumers that read both, then the producers that write the new format,
 * then drop the old codec once the queue has drained.
 *
 * <p>Encoding always uses the first codec, and there is no way to configure otherwise. Choosing
 * an encoding per message would need a rule for choosing, and every such rule turns into a
 * question the caller has to answer at each publish; one format out, several in, is the shape
 * that migrations actually need.
 *
 * <p>A message whose content type matches nothing is offered to every codec in turn, because a
 * sender that set no content type has told us nothing rather than told us it is unreadable, and
 * something on the wire is usually still readable by one of them.
 */
public final class CompositeCodec implements Codec {

    private final List<Codec> codecs;

    private CompositeCodec(List<Codec> codecs) {
        this.codecs = Collections.unmodifiableList(new ArrayList<>(codecs));
    }

    /**
     * @param codecs codecs to read with; the first is also the one written with
     * @return a codec that writes the first format and reads any of them
     */
    public static CompositeCodec of(Codec... codecs) {
        if (codecs == null || codecs.length == 0) {
            throw new IllegalArgumentException("at least one codec is required");
        }
        List<Codec> given = Arrays.asList(codecs);
        if (given.contains(null)) {
            throw new IllegalArgumentException("codecs must not contain null");
        }
        return new CompositeCodec(given);
    }

    @Override
    public String contentType() {
        return codecs.get(0).contentType();
    }

    @Override
    public byte[] encode(Object payload) {
        return codecs.get(0).encode(payload);
    }

    @Override
    public <T> T decode(byte[] body, Class<T> target) {
        return decode(body, target, null);
    }

    @Override
    public <T> T decode(byte[] body, Class<T> target, @Nullable String contentType) {
        List<Codec> candidates = new ArrayList<>();
        for (Codec codec : codecs) {
            if (codec.canDecode(contentType)) {
                candidates.add(codec);
            }
        }
        if (candidates.isEmpty()) {
            throw new AceMqException("no codec here reads '" + contentType + "'; this one has "
                    + describe() + ". The message will not be retried, because it would not decode next"
                    + " time either.");
        }

        List<String> refusals = new ArrayList<>();
        for (Codec codec : candidates) {
            try {
                return codec.decode(body, target);
            } catch (RuntimeException e) {
                // Kept rather than thrown, so that a body claiming one format but written in
                // another is still read by whichever codec can read it. Only when every
                // candidate has refused is anything reported, and then all the reasons are.
                refusals.add(codec + ": " + e.getMessage());
            }
        }
        throw new AceMqException("no codec could decode this message as " + target.getName() + ". Tried "
                + String.join("; ", refusals));
    }

    @Override
    public boolean canDecode(@Nullable String contentType) {
        for (Codec codec : codecs) {
            if (codec.canDecode(contentType)) {
                return true;
            }
        }
        return false;
    }

    /** @return the codecs this one reads with, in the order they are tried */
    public List<Codec> codecs() {
        return codecs;
    }

    private String describe() {
        return codecs.stream().map(Codec::contentType).collect(Collectors.joining(", "));
    }

    @Override
    public String toString() {
        return "CompositeCodec{writes=" + contentType() + ", reads=" + describe() + "}";
    }
}
