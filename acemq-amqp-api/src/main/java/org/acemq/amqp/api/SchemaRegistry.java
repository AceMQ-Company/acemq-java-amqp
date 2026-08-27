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
 * Turns a schema into a small integer and back.
 *
 * <p>Some formats describe themselves and some do not. Avro's bytes and Protobuf's carry no
 * account of what they are, so a reader must already hold the schema the writer used — that is
 * the trade those formats make for being compact, and it is why a registry exists at all rather
 * than being an operational preference.
 *
 * <p>Format-neutral on purpose. The same schemas have to be reachable from Avro and Protobuf
 * today and from MQTT and STOMP later, so this interface knows about text and identifiers and
 * nothing about serialisation. A registry that spoke one format would have to be rebuilt for the
 * next.
 *
 * <p><strong>The identifier is small on purpose too.</strong> It travels in the header of every
 * single message, so four bytes rather than sixteen is not fastidiousness — and it is the
 * layout Confluent's clients already read, which is what lets messages written here be read by
 * tools nobody here wrote. A registry is free to key its own records however it likes, by UUID
 * or otherwise; what crosses the wire is this integer.
 *
 * <p>Identifiers must be stable forever. A message published today may be read next year by a
 * consumer looking up the schema it was written with, so an implementation that hands out fresh
 * identifiers on restart makes every message written before the restart unreadable.
 */
public interface SchemaRegistry {

    /**
     * @param schema the schema a message is about to be written with
     * @return its identifier, registering it if the registry has not seen it. Registering the
     *     same schema twice must return the same identifier
     */
    int idFor(SchemaDefinition schema);

    /**
     * @param id an identifier read from a message
     * @return the schema it stands for
     * @throws AceMqException if the registry does not know it
     */
    SchemaDefinition schemaFor(int id);
}
