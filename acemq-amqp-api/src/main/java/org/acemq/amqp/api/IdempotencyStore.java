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
     *     {@code false} when it is already confirmed or claimed elsewhere, and the delivery
     *     should be treated as a duplicate
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
}
