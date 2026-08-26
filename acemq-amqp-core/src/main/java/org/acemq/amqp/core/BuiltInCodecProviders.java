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

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.CodecProvider;

/**
 * The two formats that need no library, announced to the registry.
 *
 * <p>Both sit at the back of the order. Text accepts anything that looks like text and would
 * otherwise answer for messages meant for a structured format; bytes accepts anything at all.
 * A format that recognises its own content belongs in front of both.
 */
public final class BuiltInCodecProviders {

    private BuiltInCodecProviders() {
        throw new AssertionError("BuiltInCodecProviders is a holder and must not be instantiated");
    }

    /** Announces {@link StringCodec} as {@code text}. */
    public static final class Text implements CodecProvider {

        @Override
        public String name() {
            return "text";
        }

        @Override
        public Codec create() {
            return new StringCodec();
        }

        @Override
        public int order() {
            // Next to last. It claims every text/* content type and a message with none at all,
            // so anything that can identify its own bytes should get the first look.
            return 900;
        }
    }

    /** Announces {@link BytesCodec} as {@code bytes}. */
    public static final class Bytes implements CodecProvider {

        @Override
        public String name() {
            return "bytes";
        }

        @Override
        public Codec create() {
            return new BytesCodec();
        }

        @Override
        public int order() {
            // Last. It would accept any bytes at all and hand back a byte[] where an object was
            // expected, which is a decode that succeeds and still leaves the consumer wrong.
            return 1000;
        }
    }
}
