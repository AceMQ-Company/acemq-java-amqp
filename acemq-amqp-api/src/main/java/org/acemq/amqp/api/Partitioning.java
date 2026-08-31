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

import java.nio.charset.StandardCharsets;

/**
 * Decides which partition an ordering key belongs to.
 *
 * <p>This is a wire contract, not an implementation detail. A Java publisher and a Go consumer
 * have to agree about where a key lands or ordering breaks silently across languages, so the
 * hash is specified rather than borrowed: <strong>FNV-1a, 32-bit, over the key's UTF-8 bytes</strong>.
 *
 * <p>{@link String#hashCode()} was the obvious candidate and is the wrong one. It is stable
 * across Java versions but it is Java's, and reimplementing it in Go or Python to keep a fleet in
 * agreement is a strange thing to ask of anybody. FNV-1a is eight lines in any language and has
 * no library behind it.
 *
 * <p>The golden values in the test suite exist for exactly this reason: a port that produces
 * different partitions is a port that reorders messages, and nothing else would catch it.
 *
 * <h2>Changing the partition count is not free</h2>
 *
 * <p>Partition {@code n} for a key is a function of the count, so raising it moves most keys
 * somewhere new. During the change a key can have messages in two partitions at once, and their
 * relative order is not defined. There is no clever fix — consistent hashing reduces how many
 * keys move but does not stop it. Drain first, then change.
 */
public final class Partitioning {

    private static final int FNV_OFFSET_BASIS = 0x811c9dc5;
    private static final int FNV_PRIME = 0x01000193;

    private Partitioning() {
        throw new AssertionError("Partitioning is a utility and must not be instantiated");
    }

    /**
     * @param key the ordering key, such as a customer or account identifier
     * @return the FNV-1a 32-bit hash of its UTF-8 bytes
     */
    public static int hash(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        int hash = FNV_OFFSET_BASIS;
        for (byte b : bytes) {
            // XOR then multiply. FNV-1 does it the other way round and disperses noticeably
            // worse on short keys, which is what identifiers usually are.
            hash ^= b & 0xFF;
            hash *= FNV_PRIME;
        }
        return hash;
    }

    /**
     * @param key the ordering key
     * @param partitions how many partitions exist, at least 1
     * @return the partition this key belongs to, from 0 to {@code partitions - 1}
     */
    public static int partitionFor(String key, int partitions) {
        if (key == null) {
            throw new IllegalArgumentException("an ordering key must not be null. A message with no key has no"
                    + " place in a sequence, so there is nothing sensible to do with it here.");
        }
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be at least 1, was " + partitions);
        }
        // floorMod rather than %, because the hash is signed and % would produce a negative
        // index for roughly half of all keys.
        return Math.floorMod(hash(key), partitions);
    }

    /**
     * @param partition partition number
     * @return the routing key it is bound with, {@code p0}, {@code p1} and so on
     */
    public static String routingKeyFor(int partition) {
        return "p" + partition;
    }
}
