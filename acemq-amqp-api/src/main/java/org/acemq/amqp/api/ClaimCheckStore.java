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

import java.util.Optional;

/**
 * Where a payload too large for a broker actually goes.
 *
 * <p>Three methods, so a store backed by S3, Azure Blob Storage, a filesystem or a database
 * table is a small class. Nothing here knows about messaging: the store holds bytes under a key
 * and hands them back, and the codec is what turns that into a claim check on the wire.
 *
 * <h2>Retention is the part that goes wrong</h2>
 *
 * <p>The store and the queue have different lifetimes, and nothing enforces a relationship
 * between them. A message replayed a month later carries a key, and if the store expired that
 * key the replay produces a message nobody can read — <strong>worse than a lost message,
 * because it looks like a message</strong> and fails deep inside a consumer rather than
 * visibly.
 *
 * <p>So the store's retention must exceed every retention that could bring a message back:
 * queue TTLs, dead-letter queues, and however long somebody might sit on a message before
 * replaying it by hand. When in doubt, longer.
 *
 * @implSpec implementations must be safe for use by several threads, because one is shared by
 *     every publisher and consumer on a connection.
 */
public interface ClaimCheckStore {

    /**
     * Stores a payload.
     *
     * @param content the bytes
     * @return the key the message will carry, which must be unique for the life of the store
     */
    String put(byte[] content);

    /**
     * Redeems a claim check.
     *
     * @param key what the message carried
     * @return the payload, or empty when the store no longer holds it — which is retention
     *     having expired underneath a message that outlived it
     */
    Optional<byte[]> get(String key);

    /**
     * Removes a payload.
     *
     * <p>Not called by the codec. Deleting on read would break the second consumer of the same
     * message, and deleting on acknowledgement would break a replay — so when a payload may be
     * removed is a retention decision, and retention decisions belong to whoever owns the data.
     *
     * @param key what to remove
     */
    void delete(String key);
}
