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
package org.acemq.amqp.codec.xml;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.CodecProvider;

/** Announces {@link XmlCodec} to the engine as {@code xml}. */
public final class XmlCodecProvider implements CodecProvider {

    @Override
    public String name() {
        return "xml";
    }

    @Override
    public Codec create() {
        return new XmlCodec();
    }

    @Override
    public int order() {
        // After JSON, before text. It claims only messages that say they are XML, so its
        // position matters little; it is ahead of text so that an XML content type is never
        // answered by the text codec first.
        return 200;
    }
}
