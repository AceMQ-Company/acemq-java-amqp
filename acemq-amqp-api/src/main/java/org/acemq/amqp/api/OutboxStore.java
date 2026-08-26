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

import java.time.Duration;
import java.util.List;

/**
 * Holds messages that must be published if, and only if, the work that produced them was
 * committed.
 *
 * <p>The problem this solves has no solution inside the application. A handler that writes an
 * order to the database and then publishes an event has two systems and one flow of control, and
 * a crash in the gap leaves them disagreeing: the order exists and nobody was told, or the event
 * went out for an order that rolled back. Distributed transactions across a database and a broker
 * would close it and are not worth what they cost. The outbox closes it differently, by making
 * the message part of the same commit as the work, and moving the publish out of the request
 * entirely.
 *
 * <p>Everything therefore turns on {@link #add} running inside the caller's transaction. A store
 * that opens its own connection and commits by itself compiles, passes a careless test, and
 * provides no guarantee whatsoever — it has merely moved the same race somewhere less visible.
 * An implementation must be explicit about where its transaction comes from, and must fail
 * loudly rather than quietly borrow one of its own.
 *
 * <p>What the outbox buys is at-least-once delivery, not exactly-once. A relay that dies between
 * the broker confirming and the row being marked published will send the message again on
 * restart, because the alternative — marking first — loses messages instead, and a duplicate is
 * recoverable where a loss is not. The counterpart on the consumer side is
 * {@link IdempotencyStore}, and the two are designed to be used together: the record keeps its
 * identifier across redeliveries precisely so the consumer can recognise them.
 */
public interface OutboxStore {

    /**
     * Writes a message into the outbox, inside the caller's transaction.
     *
     * <p>Must participate in whatever transaction the calling code has open, so that the message
     * is committed or rolled back with the work that produced it. This is the whole pattern; an
     * implementation that cannot honour it should throw rather than pretend.
     *
     * @param record the message to publish once the surrounding work commits
     * @throws AceMqException if the record cannot be written, or if no transaction is available
     */
    void add(OutboxRecord record);

    /**
     * Takes a batch of unpublished messages for this relay to work on.
     *
     * <p>Claiming is by lease rather than by lock, so that a relay which dies holding a batch
     * does not strand it: the lease expires and another relay picks the messages up. The lease
     * must therefore be comfortably longer than a batch takes to publish, and shorter than the
     * delay anyone would accept before a stalled message moves.
     *
     * @param batchSize most records to claim
     * @param lease how long this claim holds before other relays may take the records
     * @return claimed records in the order they were created, oldest first; empty when there is
     *     nothing to do
     */
    List<OutboxRecord> claimBatch(int batchSize, Duration lease);

    /**
     * Records that a claimed message reached the broker and needs no further attention.
     *
     * @param id identifier of the published record
     */
    void markPublished(String id);

    /**
     * Records that an attempt failed, so the message is tried again later.
     *
     * <p>Increments the attempt count and releases the claim. A record that has failed more than
     * the store's limit stops being claimed and stays for someone to look at, which is the right
     * outcome: a message that cannot be published after many tries is a problem to be seen, not
     * one to be retried into the next outage.
     *
     * @param id identifier of the record that failed
     * @param reason why it failed, for whoever reads the table later
     */
    void markFailed(String id, String reason);

    /**
     * @return how many messages are waiting to be published, including those currently claimed
     *     and those that have exhausted their attempts
     */
    long pendingCount();
}
