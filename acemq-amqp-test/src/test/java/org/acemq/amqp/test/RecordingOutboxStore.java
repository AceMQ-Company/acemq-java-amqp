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
package org.acemq.amqp.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.api.OutboxStore;

/**
 * An outbox store held in a map, for testing the relay without a database.
 *
 * <p>Deliberately not shipped in the patterns module. An in-memory idempotency store is a limited
 * but honest production choice; an in-memory outbox is never one, because a store that dies with
 * the process cannot make a message durable alongside a database commit — it gives up the only
 * thing the pattern provides. Somebody would use it. So it lives here, where it cannot be
 * imported by accident.
 */
class RecordingOutboxStore implements OutboxStore {

    private final Map<String, OutboxRecord> pending = new LinkedHashMap<>();
    private final List<String> publishedIds = new CopyOnWriteArrayList<>();
    private final List<String> failures = new CopyOnWriteArrayList<>();
    private final int maxAttempts;

    RecordingOutboxStore() {
        this(3);
    }

    RecordingOutboxStore(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    @Override
    public synchronized void add(OutboxRecord record) {
        pending.put(record.id(), record);
    }

    @Override
    public synchronized List<OutboxRecord> claimBatch(int batchSize, Duration lease) {
        List<OutboxRecord> claimed = new ArrayList<>();
        for (OutboxRecord record : pending.values()) {
            if (claimed.size() == batchSize) {
                break;
            }
            if (record.attempts() < maxAttempts) {
                claimed.add(record);
            }
        }
        return claimed;
    }

    @Override
    public synchronized void markPublished(String id) {
        pending.remove(id);
        publishedIds.add(id);
    }

    @Override
    public synchronized void markFailed(String id, String reason) {
        failures.add(reason);
        OutboxRecord record = pending.get(id);
        if (record != null) {
            pending.put(id, new OutboxRecord(
                    record.id(),
                    record.exchange(),
                    record.routingKey(),
                    record.type(),
                    record.payload(),
                    record.correlationId().orElse(null),
                    record.causationId().orElse(null),
                    record.createdAt(),
                    record.attempts() + 1,
                    reason));
        }
    }

    @Override
    public synchronized long pendingCount() {
        return pending.size();
    }

    synchronized List<String> published() {
        return Collections.unmodifiableList(new ArrayList<>(publishedIds));
    }

    synchronized List<String> failures() {
        return Collections.unmodifiableList(new ArrayList<>(failures));
    }

    synchronized int attemptsFor(String id) {
        OutboxRecord record = pending.get(id);
        return record == null ? -1 : record.attempts();
    }
}
