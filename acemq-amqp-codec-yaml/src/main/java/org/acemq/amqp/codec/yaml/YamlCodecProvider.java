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
package org.acemq.amqp.codec.yaml;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.CodecProvider;

/** Announces {@link YamlCodec} to the engine as {@code yaml}. */
public final class YamlCodecProvider implements CodecProvider {

    @Override
    public String name() {
        return "yaml";
    }

    @Override
    public Codec create() {
        return new YamlCodec();
    }

    @Override
    public int order() {
        // Behind JSON, and that matters more here than for the other formats: YAML parses JSON
        // quite happily, so a YAML codec given the first look would answer for JSON messages and
        // be right about the value while wrong about the format.
        return 300;
    }
}
