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
 * Remembers which messages have already been handled, so handling one twice does not happen
 * twice.
 *
 * <p>Every broker worth using delivers at least once, which means duplicates are not an error
 * condition to be avoided but a normal event to be absorbed. A message is redelivered whenever
 * a consumer dies between doing the work and acknowledging it, whenever a connection drops
 * mid-delivery, and whenever a retry is triggered by something that had in fact succeeded.
 *
 * <p>The three-step shape is deliberate and is what makes this safe under failure. A naive
 * store offers "have I seen this?" followed by "record that I have", and loses either way: mark
 * before the handler runs and a crash mid-handler means the work never happens and never can,
 * because the message now looks handled; mark after and two concurrent deliveries both pass the
 * check. Claiming, then confirming only on success and releasing on failure, closes both.
 *
 * <p>Implementations must be safe under concurrent use and must make {@link #claim} atomic:
 * exactly one caller can hold a claim on an identifier at a time.
 */
public interface IdempotencyStore {

    /**
     * Takes ownership of a message identifier, if nobody else has it.
     *
     * @param messageId the identifier, normally {@link Envelope#id()}
     * @return {@code true} when the caller now owns this identifier and should do the work;
     *     {@code false} when it is already confirmed or claimed elsewhere. Which of the two it is
     *     matters — only a confirmed one is a duplicate — so consumers call {@link #tryClaim}
     */
    boolean claim(String messageId);

    /**
     * Records that the work for a claimed identifier completed.
     *
     * <p>After this, {@link #claim} returns {@code false} for the identifier until the store
     * forgets it, which is what stops a redelivery from repeating the work.
     *
     * @param messageId the identifier being confirmed
     */
    void confirm(String messageId);

    /**
     * Gives up a claim without recording completion, so the message can be tried again.
     *
     * <p>Called when a handler fails. Without it a failed attempt would poison the identifier
     * and the retry would be discarded as a duplicate, which turns a transient failure into
     * permanent message loss.
     *
     * @param messageId the identifier being released
     */
    void release(String messageId);

    /**
     * @param messageId the identifier to test
     * @return whether the work for this identifier is already known to have completed
     */
    boolean isConfirmed(String messageId);

    /**
     * Takes ownership of a message identifier, and says why not when it cannot.
     *
     * <p>{@link #claim} answers {@code false} for two different situations, and a consumer must
     * not treat them alike. A confirmed identifier is finished work, and a redelivery of it is a
     * duplicate to acknowledge. A claimed but unconfirmed identifier is work nobody has finished:
     * acknowledging that redelivery loses the message whenever the first handler died and its
     * release failed too. So the consumer acknowledges only {@link ClaimResult#ALREADY_CONFIRMED},
     * and sends {@link ClaimResult#IN_PROGRESS} back to the queue to be tried again.
     *
     * <p>The default asks {@link #claim} and then {@link #isConfirmed}, which suits every store
     * written against the two-state contract. The two calls are not one atomic step, and both
     * races resolve safely: a confirmation landing in between reads as a duplicate, which it is,
     * and a confirmation expiring in between reads as in progress, which only costs a retry.
     *
     * @param messageId the identifier, normally {@link Envelope#id()}
     * @return whether the caller now owns the identifier, or why it does not
     */
    default ClaimResult tryClaim(String messageId) {
        if (claim(messageId)) {
            return ClaimResult.CLAIMED;
        }
        return isConfirmed(messageId) ? ClaimResult.ALREADY_CONFIRMED : ClaimResult.IN_PROGRESS;
    }

    /** What {@link #tryClaim} found. */
    enum ClaimResult {

        /** The caller owns the identifier and should do the work. */
        CLAIMED,

        /** The work is done; the delivery is a duplicate and may be acknowledged. */
        ALREADY_CONFIRMED,

        /**
         * Someone holds a live claim and has not confirmed it. The delivery must be neither
         * handled nor acknowledged, but tried again later.
         */
        IN_PROGRESS
    }
}
