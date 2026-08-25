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

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.acemq.amqp.api.AceHeaders;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.RetryPolicy;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.OutboundMessage;
import org.acemq.amqp.transport.TransportConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides what happens to a delivery that failed, and carries it out.
 *
 * <p>Three outcomes exist. A message with attempts left is republished into a retry rung with
 * its attempt counter incremented. A message that has run out of attempts, or grown too old,
 * is republished to the dead-letter queue with the reason attached. A message that could not
 * even be decoded goes to the parking lot instead, because a payload that fails to parse will
 * fail identically on every future attempt and retrying it only wastes capacity.
 *
 * <p>In every case the original delivery is then acknowledged. That looks surprising for a
 * failure, but it is what makes the mechanism reliable: the message has already been safely
 * republished elsewhere, so acknowledging the original is simply removing the copy that has
 * been dealt with. Rejecting it instead would either requeue it into a hot loop or, with a
 * broker-side dead-letter configuration, send it somewhere this class did not choose.
 */
final class RetryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(RetryDispatcher.class);

    private final TransportConnection connection;
    private final RetryTopology topology;
    private final RetryPolicy policy;
    private final Telemetry telemetry;

    RetryDispatcher(TransportConnection connection, RetryTopology topology, Telemetry telemetry) {
        this.connection = connection;
        this.topology = topology;
        this.policy = topology.policy();
        this.telemetry = telemetry;
    }

    /**
     * Routes a failed delivery to its next destination.
     *
     * @param delivery the delivery that failed
     * @param envelope its envelope, already parsed
     * @param failure why it failed
     * @param fatal when {@code true} the retry schedule is skipped entirely and the message is
     *     dead-lettered immediately, because the handler has said that trying again cannot
     *     help
     * @return what was done, for metrics and logging
     */
    Outcome onFailure(InboundDelivery delivery, Envelope envelope, Throwable failure, boolean fatal) {
        if (fatal) {
            deadLetter(delivery, envelope, "the handler reported an unprocessable message: " + describe(failure));
            return Outcome.DEAD_LETTERED;
        }

        Duration age = envelope.age();
        Optional<Duration> delay = policy.nextDelay(envelope.attempt(), age);

        if (!delay.isPresent()) {
            String reason = envelope.attempt() >= policy.maxAttempts()
                    ? "exhausted " + policy.maxAttempts() + " attempts"
                    : "exceeded the maximum message age of " + policy.maxMessageAge();
            deadLetter(delivery, envelope, reason + ": " + describe(failure));
            return Outcome.DEAD_LETTERED;
        }

        Duration wait = delay.get();
        Optional<String> rung = topology.rungFor(wait);
        if (!rung.isPresent()) {
            // A policy with attempts but no schedule cannot happen through the public API,
            // but failing loudly beats silently dropping the message if it ever does.
            deadLetter(delivery, envelope, "no retry queue exists for a delay of " + wait);
            return Outcome.DEAD_LETTERED;
        }

        Envelope next = envelope.nextAttempt();
        publish(rung.get(), delivery, next, null);
        telemetry.messageRetried(topology.sourceQueue(), next, wait);
        log.debug(
                "retrying {} attempt {} of {} after {} via {}",
                envelope.id(),
                next.attempt(),
                policy.maxAttempts(),
                wait,
                rung.get());
        return Outcome.RETRIED;
    }

    /**
     * Sends a delivery whose payload could not be decoded straight to the parking lot.
     *
     * @param delivery the delivery
     * @param failure why decoding failed
     */
    void park(InboundDelivery delivery, Throwable failure) {
        Map<String, Object> headers = new LinkedHashMap<>(delivery.headers());
        headers.put(AceHeaders.ERROR, "could not be decoded: " + describe(failure));

        OutboundMessage message = OutboundMessage.body(delivery.body())
                .exchange("")
                .routingKey(topology.parkingLotQueue())
                .headers(headers)
                .messageId(delivery.messageId())
                .contentType(delivery.contentType())
                .build();

        connection.send(message);
        log.warn(
                "parked an undecodable message from {} in {}: {}",
                delivery.queue(),
                topology.parkingLotQueue(),
                describe(failure));
    }

    private void deadLetter(InboundDelivery delivery, Envelope envelope, String reason) {
        publish(topology.deadLetterQueue(), delivery, envelope, reason);
        telemetry.messageDeadLettered(topology.sourceQueue(), envelope, reason);
        log.warn(
                "dead-lettered {} from {} after {} attempts: {}",
                envelope.id(),
                topology.sourceQueue(),
                envelope.attempt(),
                reason);
    }

    private void publish(String queue, InboundDelivery delivery, Envelope envelope, String error) {
        // The reason travels as an envelope field, so a consumer of the dead-letter queue can
        // read it back through the API rather than having to know the wire header name.
        Envelope outgoing = error == null ? envelope : envelope.toBuilder().error(error).build();
        Map<String, Object> headers = new LinkedHashMap<>(EnvelopeHeaders.toHeaders(outgoing));

        OutboundMessage message = OutboundMessage.body(delivery.body())
                // Published through the default exchange, which routes straight to the named
                // queue. The retry and dead-letter exchanges exist for the broker's own
                // dead-lettering of expired rung messages, not for this hop.
                .exchange("")
                .routingKey(queue)
                .headers(headers)
                .messageId(outgoing.id())
                .contentType(delivery.contentType())
                .build();

        connection.send(message);
    }

    private static String describe(Throwable failure) {
        if (failure == null) {
            return "no reason given";
        }
        String message = failure.getMessage();
        return failure.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    /** What the dispatcher did with a failed delivery. */
    enum Outcome {

        /** Republished into a retry rung; it will come back when the delay expires. */
        RETRIED,

        /** Republished to the dead-letter queue; it will not come back on its own. */
        DEAD_LETTERED
    }
}
