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
package org.acemq.amqp.codec.toml;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.CodecProvider;

/** Announces {@link TomlCodec} to the engine as {@code toml}. */
public final class TomlCodecProvider implements CodecProvider {

    @Override
    public String name() {
        return "toml";
    }

    @Override
    public Codec create() {
        return new TomlCodec();
    }

    @Override
    public int order() {
        // Behind JSON, XML and YAML. Nothing on this list should ever be picked by accident,
        // and TOML is the least likely of them to be what an unlabelled message contains.
        return 400;
    }
}
