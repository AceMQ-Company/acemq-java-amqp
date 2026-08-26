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

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.acemq.amqp.api.IdempotencyStore;

/**
 * An idempotency store held in this process's memory, bounded by size and age.
 *
 * <p>Honest about what it is: this deduplicates within one process and forgets everything when
 * that process restarts. That is enough for a single consumer whose duplicates arrive seconds
 * apart, which is the common case, and it is not enough for a consumer group spread across
 * several instances, where a duplicate can land on a different machine from the original. For
 * that the store has to be shared — Redis or a database table — and this class is the shape
 * such an implementation takes.
 *
 * <p>Both bounds exist to stop the obvious failure. Unbounded, this is a memory leak with a
 * business purpose: every message identifier ever seen, retained forever. Entries expire after
 * a retention period, and the map is capped, because a store that runs a service out of memory
 * has done more damage than the duplicates it prevented.
 */
public final class InMemoryIdempotencyStore implements IdempotencyStore {

    private static final int DEFAULT_MAX_ENTRIES = 100_000;

    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    private final Duration retention;
    private final int maxEntries;
    private final AtomicLong evictions = new AtomicLong();

    /**
     * @param retention how long a confirmed identifier is remembered; duplicates arriving
     *     later than this are handled again
     */
    public InMemoryIdempotencyStore(Duration retention) {
        this(retention, DEFAULT_MAX_ENTRIES);
    }

    /**
     * @param retention how long a confirmed identifier is remembered
     * @param maxEntries hard cap on remembered identifiers
     */
    public InMemoryIdempotencyStore(Duration retention, int maxEntries) {
        if (retention.isNegative() || retention.isZero()) {
            throw new IllegalArgumentException("retention must be positive, was " + retention);
        }
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be at least 1, was " + maxEntries);
        }
        this.retention = retention;
        this.maxEntries = maxEntries;
    }

    /**
     * @return a store that remembers identifiers for a day, a reasonable default when the
     *     alternative is a duplicate charge or a duplicate email
     */
    public static InMemoryIdempotencyStore forOneDay() {
        return new InMemoryIdempotencyStore(Duration.ofHours(24));
    }

    @Override
    public boolean claim(String messageId) {
        expireStaleEntries();
        Instant now = Instant.now();

        Entry existing = entries.putIfAbsent(messageId, Entry.claimed(now));
        if (existing == null) {
            // Nobody held it, and putIfAbsent inserted ours in one atomic step, so the claim
            // is unambiguously this caller's.
            capSize();
            return true;
        }
        if (isExpired(existing, now)) {
            // Old enough to forget. Replacing with a compare-and-set rather than a put means
            // a racing caller cannot also conclude it won.
            return entries.replace(messageId, existing, Entry.claimed(now));
        }
        // Either confirmed already, or claimed by someone still working on it. Both mean this
        // delivery is a duplicate as far as this consumer is concerned.
        return false;
    }

    @Override
    public void confirm(String messageId) {
        entries.put(messageId, Entry.confirmed(Instant.now()));
        capSize();
    }

    @Override
    public void release(String messageId) {
        // Removed rather than marked: a released identifier must look untouched, or the retry
        // that follows would be discarded as a duplicate and the message lost.
        entries.remove(messageId);
    }

    @Override
    public boolean isConfirmed(String messageId) {
        Entry entry = entries.get(messageId);
        if (entry == null) {
            return false;
        }
        if (isExpired(entry, Instant.now())) {
            entries.remove(messageId, entry);
            return false;
        }
        return entry.confirmed;
    }

    private boolean isExpired(Entry entry, Instant now) {
        return entry.recordedAt.plus(retention).isBefore(now);
    }

    /** @return how many identifiers are currently remembered */
    public int size() {
        return entries.size();
    }

    /** @return how many identifiers have been dropped to stay within the size cap */
    public long evictions() {
        return evictions.get();
    }

    private void expireStaleEntries() {
        if (entries.size() < maxEntries / 2) {
            return; // sweeping on every claim would cost more than it saves
        }
        Instant cutoff = Instant.now().minus(retention);
        entries.entrySet().removeIf(entry -> entry.getValue().recordedAt.isBefore(cutoff));
        capSize();
    }

    private void capSize() {
        // Oldest-first, which is the closest thing to a useful policy without keeping an
        // access-ordered structure: the oldest identifier is the least likely to be duplicated
        // again.
        while (entries.size() > maxEntries) {
            String oldest = null;
            Instant oldestAt = Instant.MAX;
            for (Map.Entry<String, Entry> candidate : entries.entrySet()) {
                if (candidate.getValue().recordedAt.isBefore(oldestAt)) {
                    oldestAt = candidate.getValue().recordedAt;
                    oldest = candidate.getKey();
                }
            }
            if (oldest == null || entries.remove(oldest) == null) {
                return;
            }
            evictions.incrementAndGet();
        }
    }

    /**
     * One remembered identifier: claimed and being worked on, or confirmed and done.
     *
     * <p>Immutable, so a state change is a replacement rather than a mutation, which is what
     * lets the compare-and-set in claim mean something.
     */
    private static final class Entry {

        private final Instant recordedAt;
        private final boolean confirmed;

        private Entry(Instant recordedAt, boolean confirmed) {
            this.recordedAt = recordedAt;
            this.confirmed = confirmed;
        }

        static Entry claimed(Instant at) {
            return new Entry(at, false);
        }

        static Entry confirmed(Instant at) {
            return new Entry(at, true);
        }
    }
}
