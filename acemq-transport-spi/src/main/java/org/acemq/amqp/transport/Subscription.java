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
     * Stops delivery and releases broker resources.
     *
     * <p>Implementations should let in-flight deliveries finish settling rather than
     * abandoning them, so a clean shutdown does not manufacture redeliveries.
     */
    @Override
    void close();
}
