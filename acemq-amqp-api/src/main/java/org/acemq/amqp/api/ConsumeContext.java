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
 * A message about to be handled, as an interceptor sees it.
 *
 * <p>Read-only, unlike {@link PublishContext}. A consume interceptor can observe, refuse, or set
 * up and tear down surrounding state — a log context, a tenant, a transaction — but it cannot
 * rewrite the message on its way in. Rewriting an inbound message would mean the handler no
 * longer sees what the broker delivered, and the difference would show up as a bug in the
 * handler rather than in the interceptor that caused it.
 */
public final class ConsumeContext {

    private final String queue;
    private final Message<?> message;

    /**
     * @param queue queue the message arrived on
     * @param message the decoded message
     */
    public ConsumeContext(String queue, Message<?> message) {
        this.queue = Objects.requireNonNull(queue, "queue");
        this.message = Objects.requireNonNull(message, "message");
    }

    /** @return the queue this message arrived on */
    public String queue() {
        return queue;
    }

    /** @return the decoded message, including its payload */
    public Message<?> message() {
        return message;
    }

    /** @return the envelope, a shortcut for {@code message().envelope()} */
    public Envelope envelope() {
        return message.envelope();
    }

    @Override
    public String toString() {
        return "ConsumeContext{queue=" + queue + ", id=" + envelope().id() + "}";
    }
}
