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
package org.acemq.amqp.patterns;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.acemq.amqp.api.ClaimCheckStore;

/**
 * A claim-check store in a map, for tests.
 *
 * <p><strong>Not for production, and the reason is the point of the pattern.</strong> The
 * payloads are held in the publisher's heap — which is where they were going to be anyway, so
 * this removes them from the broker and nothing else. A claim check that does not outlive the
 * process that wrote it is a message nobody else can read, and every consumer in another
 * process gets "the claim check is not in the store".
 *
 * <p>It is genuinely useful for a test, where the publisher and consumer are the same JVM and
 * the point being proved is the framing rather than the storage.
 */
public final class InMemoryClaimCheckStore implements ClaimCheckStore {

    private final Map<String, byte[]> contents = new ConcurrentHashMap<>();

    @Override
    public String put(byte[] content) {
        String key = UUID.randomUUID().toString();
        // Copied, because the caller owns the array it handed over and a codec is entitled to
        // reuse a buffer. A store that keeps somebody else's array is a store whose contents
        // change after they were stored.
        contents.put(key, content.clone());
        return key;
    }

    @Override
    public Optional<byte[]> get(String key) {
        byte[] content = contents.get(key);
        return content == null ? Optional.empty() : Optional.of(content.clone());
    }

    @Override
    public void delete(String key) {
        contents.remove(key);
    }

    /** @return how many payloads are held */
    public int size() {
        return contents.size();
    }

    /** Empties the store, which is what a test between cases wants. */
    public void clear() {
        contents.clear();
    }

    @Override
    public String toString() {
        return "InMemoryClaimCheckStore{" + contents.size() + " held}";
    }
}
