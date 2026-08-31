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
package org.acemq.amqp.transport;

/** A live consumer registration. Closing it stops delivery. */
public interface Subscription extends AutoCloseable {

    /** @return the queue being consumed */
    String queue();

    /** @return whether the subscription is still receiving deliveries */
    boolean isActive();

    /**
     * Changes how many unsettled deliveries this subscription may hold.
     *
     * <p>Takes effect for deliveries after the call; messages already in flight are unaffected,
     * because they have been sent. That is the protocol's behaviour, not a shortcut — there is
     * no way to un-send them.
     *
     * <p>The default refuses. A transport that silently ignored a prefetch change would leave an
     * operator watching a number that says one thing while the consumer does another, which is
     * worse than an error.
     *
     * @param prefetch the new limit, at least 1
     * @throws TransportException if this transport cannot change prefetch on a live subscription
     */
    default void setPrefetch(int prefetch) {
        throw new TransportException("the transport behind queue '" + queue() + "' cannot change prefetch on a"
                + " live subscription. Recreate the consumer instead.");
    }

    /**
     * Stops delivery and releases broker resources.
     *
     * <p>Implementations should let in-flight deliveries finish settling rather than
     * abandoning them, so a clean shutdown does not manufacture redeliveries.
     */
    @Override
    void close();
}
