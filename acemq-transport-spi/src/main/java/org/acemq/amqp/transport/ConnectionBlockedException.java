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
 * The broker has stopped accepting publishes, and waiting for it to resume ran out of time.
 *
 * <p>RabbitMQ blocks publishing connections when it runs low on memory or disk, and the way it
 * does so is quiet: the socket stops draining, {@code basicPublish} keeps returning, and the
 * confirm never arrives. A publisher that waits for confirms then waits forever — no exception,
 * no timeout, no log line. It is the least visible way a healthy-looking service stops working.
 *
 * <p>Its own type because the right response is specific. This does not mean the message was
 * bad, and it does not mean the connection is broken; it means the broker is under pressure and
 * an operator needs to know. Retrying immediately makes it worse. Backing off, shedding load, or
 * alerting are all reasonable; treating it as an ordinary publish failure is not.
 *
 * <p>Whether the message reached the broker depends on when the block was noticed, and
 * {@link #mayHaveBeenPublished()} says which case this is. RabbitMQ does not announce an alarm
 * to an idle connection: it tells a connection it is blocked only when that connection next
 * publishes. So the first message sent into an alarm is written to the socket and then waits for
 * a confirm that may never come, while every message after it is refused before anything is
 * written. The first is uncertain; the rest certainly did not arrive.
 */
public class ConnectionBlockedException extends TransportException {

    private static final long serialVersionUID = 1L;

    private final String reason;

    private final boolean mayHaveBeenPublished;

    /**
     * Nothing was sent: the connection was already known to be blocked.
     *
     * @param message what happened and for how long
     * @param reason the broker's own explanation, such as {@code low on memory}
     */
    public ConnectionBlockedException(String message, String reason) {
        this(message, reason, false);
    }

    /**
     * @param message what happened and for how long
     * @param reason the broker's own explanation, such as {@code low on memory}
     * @param mayHaveBeenPublished whether the message had already been written to the connection
     *     when the block was discovered
     */
    public ConnectionBlockedException(String message, String reason, boolean mayHaveBeenPublished) {
        super(message);
        this.reason = reason;
        this.mayHaveBeenPublished = mayHaveBeenPublished;
    }

    /**
     * Whether this message might already be at the broker.
     *
     * <p>{@code false} means it certainly is not, and resending it is safe. {@code true} means it
     * was written to the connection but never confirmed, so it may have arrived, may have been
     * dropped, and may still arrive — the ordinary at-least-once situation, and the reason
     * consumers need to be idempotent.
     *
     * @return whether the message may have reached the broker despite this failure
     */
    public boolean mayHaveBeenPublished() {
        return mayHaveBeenPublished;
    }

    /**
     * @return why the broker said it was blocking, in its own words. RabbitMQ sends
     *     {@code low on memory} or {@code low on disk}, which is the difference between adding
     *     consumers and adding disk
     */
    public String reason() {
        return reason;
    }
}
