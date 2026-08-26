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
package org.acemq.amqp.api;

/**
 * Announces a codec to the engine, so that a format can be chosen by name.
 *
 * <p>Discovered with {@link java.util.ServiceLoader}, the same way transports are. That is what
 * lets {@code asXml()} exist in the core without the core depending on an XML library: the method
 * asks for a codec called {@code xml}, and either the module providing it is on the classpath or
 * the error says which artifact to add.
 *
 * <p>To add a format, implement this and {@link Codec}, and name the implementation in
 * {@code META-INF/services/org.acemq.amqp.api.CodecProvider}.
 */
public interface CodecProvider {

    /**
     * @return the short name this format is asked for by, such as {@code json} or {@code xml};
     *     lowercase, no spaces
     */
    String name();

    /**
     * @return a codec for this format
     * @throws AceMqException if the format's library is missing or misconfigured
     */
    Codec create();

    /**
     * Where this codec sits when several could read the same message.
     *
     * <p>A consumer reads with every codec on the classpath, tried in this order, because a
     * message whose sender set no content type has told us nothing and something usually still
     * reads it. Lower numbers are tried first. Formats that recognise their own bytes belong
     * near the front; a format that accepts anything belongs at the back, or it will answer for
     * messages that were not meant for it.
     *
     * @return the sort key; {@code 500} is a reasonable default for a new format
     */
    default int order() {
        return 500;
    }
}
