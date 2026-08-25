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
 * Receives deliveries from a subscription.
 *
 * <p>Called on a transport-owned thread. Implementations must settle the delivery through the
 * supplied {@link Acknowledger} and must not throw: the core wraps application handlers so
 * that a failure becomes a rejection rather than a lost delivery.
 */
@FunctionalInterface
public interface DeliveryListener {

    /**
     * @param delivery the message received
     * @param acknowledger used to settle it exactly once
     */
    void onDelivery(InboundDelivery delivery, Acknowledger acknowledger);
}
