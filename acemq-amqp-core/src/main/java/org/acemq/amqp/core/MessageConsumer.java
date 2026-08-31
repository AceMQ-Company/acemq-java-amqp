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
package org.acemq.amqp.core;

/** A running consumer. */
public interface MessageConsumer extends AutoCloseable {

    /** @return the queue being consumed */
    String queue();

    /** @return whether it is still receiving deliveries */
    boolean isRunning();

    /** @return how many messages have been acknowledged */
    long acknowledged();

    /** @return how many messages have been rejected after a handler failure */
    long rejected();

    /**
     * @return how many deliveries were recognised as already handled and acknowledged without
     *     running the handler; zero unless an idempotency store is configured
     */
    long duplicates();

    /** @return how many messages have been sent to a retry queue for another attempt */
    long retried();

    /** @return how many messages have been dead-lettered or parked */
    long deadLettered();

    /**
     * @return how many messages this consumer is handling right now. Zero means it can be
     *     stopped without manufacturing a redelivery
     */
    long inFlight();

    /**
     * Stops taking new messages and waits for the ones in hand to finish.
     *
     * <p>The difference between this and {@link #close()} is what happens to work in progress.
     * Closing cancels the subscription and lets whatever was mid-handler settle however it
     * settles; draining cancels the subscription and then <em>waits</em>, so a consumer being
     * removed finishes its work rather than having it redelivered somewhere else.
     *
     * <p>That matters whenever a handler is not idempotent, and it is the primitive a blue-green
     * deployment needs: drain the old side, then cut over.
     *
     * @param timeout how long to wait for in-flight messages
     * @return {@code true} if everything finished, {@code false} if the timeout passed first — in
     *     which case the consumer is still stopped, but something is still running
     */
    boolean drain(java.time.Duration timeout);

    /**
     * Changes how many unsettled deliveries this consumer may hold, while it is running.
     *
     * <p>Takes effect for deliveries after the call. Messages already in flight are unaffected,
     * because they have been sent and there is no way to un-send them.
     *
     * @param prefetch the new limit, at least 1
     */
    void prefetch(int prefetch);

    /** @return the prefetch currently in force */
    int prefetch();

    /** Stops consuming, letting in-flight deliveries settle. */
    @Override
    void close();
}
