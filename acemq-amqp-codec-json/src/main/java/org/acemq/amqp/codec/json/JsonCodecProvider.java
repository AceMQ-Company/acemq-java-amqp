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
package org.acemq.amqp.codec.json;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.CodecProvider;

/** Announces {@link JsonCodec} to the engine as {@code json}. */
public final class JsonCodecProvider implements CodecProvider {

    @Override
    public String name() {
        return "json";
    }

    @Override
    public Codec create() {
        return new JsonCodec();
    }

    @Override
    public int order() {
        // First. JSON bytes are recognisable and the default format, so a message with no
        // content type is most likely to be one of these.
        return 100;
    }
}
