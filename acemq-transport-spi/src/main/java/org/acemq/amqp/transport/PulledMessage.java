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

import java.util.Objects;

/**
 * One message taken off a queue by {@link TransportConnection#receive}, and the means to settle
 * it.
 *
 * <p>The two travel together because they are useless apart: a delivery nobody can acknowledge
 * stays unsettled until the connection drops, and an acknowledger with no delivery has nothing to
 * settle. A subscription gets them as two arguments to a listener; a pull has no listener, so it
 * gets them as one object.
 */
public final class PulledMessage {

    private final InboundDelivery delivery;
    private final Acknowledger acknowledger;

    /**
     * @param delivery the message
     * @param acknowledger settles it exactly once
     */
    public PulledMessage(InboundDelivery delivery, Acknowledger acknowledger) {
        this.delivery = Objects.requireNonNull(delivery, "delivery");
        this.acknowledger = Objects.requireNonNull(acknowledger, "acknowledger");
    }

    /** @return the message */
    public InboundDelivery delivery() {
        return delivery;
    }

    /** @return how to settle it; must be called exactly once */
    public Acknowledger acknowledger() {
        return acknowledger;
    }

    @Override
    public String toString() {
        return "PulledMessage{" + delivery + "}";
    }
}
