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
package org.acemq.amqp.codec.avro;

import org.apache.avro.Schema;

/**
 * Turns a schema into a small integer and back.
 *
 * <p>Avro's bytes carry no description of themselves. A reader must already hold the schema the
 * writer used, or the bytes are unreadable — that is the trade Avro makes for being compact, and
 * it is why a registry exists at all rather than being an operational preference.
 *
 * <p>Kept to two methods so that a Confluent registry, a database table, or a map filled at start
 * up all fit behind it. AceMQ does not ship a client for anyone's registry: doing so would tie a
 * messaging library to a vendor's HTTP API and its authentication, and that belongs in the
 * application.
 *
 * <p>Identifiers must be stable forever. A message published today may be read next year by a
 * consumer that has to look up the schema it was written with, so an implementation that hands
 * out fresh identifiers on restart makes every message written before the restart unreadable.
 */
public interface SchemaRegistry {

    /**
     * @param schema the schema a message is about to be written with
     * @return its identifier, registering it if the registry has not seen it
     */
    int idFor(Schema schema);

    /**
     * @param id an identifier read from the front of a message
     * @return the schema it stands for
     * @throws org.acemq.amqp.api.AceMqException if the registry does not know it
     */
    Schema schemaFor(int id);
}
