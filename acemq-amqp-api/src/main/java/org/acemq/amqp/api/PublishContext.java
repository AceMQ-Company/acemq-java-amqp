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

import java.util.Objects;

/**
 * A message about to be published, as an interceptor sees it.
 *
 * <p>Immutable, and changed by {@link #withEnvelope}, which returns a new context. An interceptor
 * that mutated what it was handed would be invisible to the one after it and impossible to
 * reason about when two of them touch the same header.
 *
 * <p>The payload is the application's object, before encoding. That is deliberate: an interceptor
 * that needs to look at what is being sent should see an {@code OrderPlaced}, not a byte array it
 * would have to decode again to understand.
 */
public final class PublishContext {

    private final String exchange;
    private final String routingKey;
    private final Envelope envelope;
    private final Object payload;

    /**
     * @param exchange target exchange
     * @param routingKey routing key
     * @param envelope the envelope as it stands
     * @param payload the application object being sent, before encoding
     */
    public PublishContext(String exchange, String routingKey, Envelope envelope, Object payload) {
        this.exchange = Objects.requireNonNull(exchange, "exchange");
        this.routingKey = Objects.requireNonNull(routingKey, "routingKey");
        this.envelope = Objects.requireNonNull(envelope, "envelope");
        this.payload = Objects.requireNonNull(payload, "payload");
    }

    /** @return target exchange, empty when publishing straight to a queue */
    public String exchange() {
        return exchange;
    }

    /** @return the routing key */
    public String routingKey() {
        return routingKey;
    }

    /** @return the envelope, including headers an interceptor may have added */
    public Envelope envelope() {
        return envelope;
    }

    /** @return the application object being published, before encoding */
    public Object payload() {
        return payload;
    }

    /**
     * @param replacement the envelope to carry on with
     * @return a copy of this context with a different envelope
     */
    public PublishContext withEnvelope(Envelope replacement) {
        return new PublishContext(exchange, routingKey, Objects.requireNonNull(replacement, "envelope"), payload);
    }

    @Override
    public String toString() {
        return "PublishContext{exchange=" + exchange + ", routingKey=" + routingKey
                + ", id=" + envelope.id() + "}";
    }
}
