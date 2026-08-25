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

/**
 * Settles one delivery.
 *
 * <p>Exactly one method must be called for each delivery. A transport is entitled to treat a
 * delivery that is never settled as a leak, because to the broker it is one: the message stays
 * unacknowledged and occupies a prefetch slot until the connection drops.
 */
public interface Acknowledger {

    /** Acknowledges the message so the broker discards it. */
    void accept();

    /**
     * Rejects the message.
     *
     * @param requeue {@code true} to return it for another consumer, {@code false} to let the
     *     broker dead-letter or drop it
     */
    void reject(boolean requeue);
}
