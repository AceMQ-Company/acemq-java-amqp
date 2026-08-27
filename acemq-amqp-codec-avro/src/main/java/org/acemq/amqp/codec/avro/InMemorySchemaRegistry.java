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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.SchemaDefinition;
import org.acemq.amqp.api.SchemaRegistry;

/**
 * A registry held in this process's memory.
 *
 * <p>Format-neutral, and so no longer anything to do with Avro; it lives here only until the
 * registry moves to a repository of its own. For tests, and for the case where every schema is known at start up and registered from
 * resources on the classpath. Not for anything else, and the reason is not squeamishness: the
 * identifiers it hands out are assigned in the order schemas happen to be registered, so two
 * instances of the same service can disagree about what identifier three means, and a restart can
 * make yesterday's messages unreadable. A registry shared between writers and readers, and
 * durable across restarts, is the only kind that works in production.
 *
 * <p>Registering every schema up front, in a fixed order, on every instance, is what makes this
 * safe to use for real. That is a discipline the class cannot enforce, so it is written down
 * here instead.
 */
public final class InMemorySchemaRegistry implements SchemaRegistry {

    private final Map<SchemaDefinition, Integer> ids = new ConcurrentHashMap<>();
    private final Map<Integer, SchemaDefinition> schemas = new ConcurrentHashMap<>();
    private final AtomicInteger next = new AtomicInteger(1);

    /**
     * Registers a schema under an identifier of the caller's choosing.
     *
     * <p>How this class is made deterministic: assign the numbers yourself, from configuration or
     * a constant, and every instance agrees.
     *
     * @param id the identifier to use
     * @param schema the schema it stands for
     * @return this registry, for chaining
     */
    public InMemorySchemaRegistry register(int id, SchemaDefinition schema) {
        SchemaDefinition existing = schemas.putIfAbsent(id, schema);
        if (existing != null && !existing.equals(schema)) {
            // Silently overwriting would make every message already written under this
            // identifier decode as the wrong thing, which is worse than refusing to start.
            throw new AceMqException("schema id " + id + " is already registered to " + existing.subject()
                    + " and cannot be reassigned to " + schema.subject());
        }
        ids.put(schema, id);
        next.updateAndGet(current -> Math.max(current, id + 1));
        return this;
    }

    @Override
    public int idFor(SchemaDefinition schema) {
        return ids.computeIfAbsent(schema, added -> {
            int id = next.getAndIncrement();
            schemas.put(id, added);
            return id;
        });
    }

    @Override
    public SchemaDefinition schemaFor(int id) {
        SchemaDefinition schema = schemas.get(id);
        if (schema == null) {
            throw new AceMqException("this registry does not know schema id " + id
                    + ". A message was written with a schema this process has never registered, which is what"
                    + " an in-memory registry does whenever the writer is another process or another run.");
        }
        return schema;
    }

    /** @return how many schemas are registered */
    public int size() {
        return schemas.size();
    }
}
