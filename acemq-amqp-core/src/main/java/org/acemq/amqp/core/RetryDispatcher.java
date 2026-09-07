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
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides what happens to a delivery that failed, and carries it out.
 *
 * <p>Three outcomes exist. A message with attempts left is republished with its attempt counter
 * incremented — into a retry rung when the wait is long enough for the broker to be worth it,
 * and otherwise back onto the source queue after waiting here. A message that has run out of
 * attempts, or grown too old, is republished to the dead-letter queue with the reason attached.
 * A message that could not even be decoded goes to the parking lot instead, because a payload
 * that fails to parse will fail identically on every future attempt and retrying it only wastes
 * capacity.
 *
 * <p>Republished rather than requeued, in every one of those cases. A requeue returns the bytes
 * the broker was given, so the attempt header would still read what the publisher wrote however
 * many times the message had come round, and the count would live only in this process's
 * memory — which is the one place it is lost when the process that has been failing restarts.
 * The cost is that a retried message goes to the back of the queue rather than the front; for a
 * message that has already failed once, that is the better trade.
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
        Optional<RetryPolicy.Wait> next = policy.nextWait(envelope.attempt(), age);

        if (!next.isPresent()) {
            String reason = envelope.attempt() >= policy.maxAttempts()
                    ? "exhausted " + policy.maxAttempts() + " attempts"
                    : "exceeded the maximum message age of " + policy.maxMessageAge();
            deadLetter(delivery, envelope, reason + ": " + describe(failure));
            return Outcome.DEAD_LETTERED;
        }

        RetryPolicy.Wait wait = next.get();
        if (wait.isInBroker() && retryInBroker(delivery, envelope, wait.delay())) {
            return Outcome.RETRIED;
        }
        return retryHere(delivery, envelope, wait.delay());
    }

    /**
     * Puts the message on a rung queue and lets the broker return it, reporting whether the rung
     * took it.
     *
     * <p>The rung's {@code x-message-ttl} is the delay and its dead-letter target is the source
     * queue, so the wait costs this process nothing: no delivery held, no prefetch slot spent,
     * and — the reason it exists at all — nothing lost when this process restarts halfway
     * through. A consumer sleeping on a five-minute backoff that dies at minute one does not
     * resume at minute one; the broker redelivers the unacknowledged message immediately, and
     * the policy that said five minutes delivers in none.
     *
     * <p>{@code false} means the rung is not there, and the caller should fall back to waiting
     * here. Degraded rather than fatal: the message is still deliverable, and waiting for it
     * here is what this library did before there were rungs.
     */
    private boolean retryInBroker(InboundDelivery delivery, Envelope envelope, Duration wait) {
        Optional<String> rung = topology.rungFor(wait);
        if (!rung.isPresent()) {
            // Loud, because a topology declared without its rungs otherwise looks like it works
            // right up until a long backoff quietly becomes a held prefetch slot.
            log.error(
                    "no rung exists on {} for a wait of {}, so {} will wait in this consumer instead",
                    topology.sourceQueue(),
                    wait,
                    envelope.id());
            return false;
        }

        Envelope advanced = envelope.nextAttempt();
        publish(rung.get(), delivery, advanced, null);
        telemetry.messageRetried(topology.sourceQueue(), advanced, wait);
        log.debug(
                "retrying {} attempt {} of {} after {} via {}",
                envelope.id(),
                advanced.attempt(),
                policy.maxAttempts(),
                wait,
                rung.get());
        return true;
    }

    /**
     * Waits out a short delay here and puts the message back on its own queue, one attempt
     * further on.
     *
     * <p>Waiting here holds the delivery, and so holds one of this consumer's prefetch slots.
     * For a wait of a few seconds that is the cheaper of the two costs, and it is why the policy
     * has a threshold at all: below it, a queue nobody asked for is the more expensive answer,
     * and the seconds a restart loses are only seconds.
     */
    private Outcome retryHere(InboundDelivery delivery, Envelope envelope, Duration wait) {
        if (!wait.isZero() && !wait.isNegative()) {
            try {
                Thread.sleep(wait.toMillis());
            } catch (InterruptedException e) {
                // Shutting down. The flag goes back so whatever is stopping this consumer still
                // sees it, and the message is republished anyway rather than left unsettled: it
                // is due sooner than intended, which beats coming back on attempt one after a
                // redelivery that forgot every attempt before it.
                Thread.currentThread().interrupt();
            }
        }

        Envelope advanced = envelope.nextAttempt();
        publish(topology.sourceQueue(), delivery, advanced, null);
        telemetry.messageRetried(topology.sourceQueue(), advanced, wait);
        log.debug(
                "retrying {} attempt {} of {} after waiting {} in this consumer",
                envelope.id(),
                advanced.attempt(),
                policy.maxAttempts(),
                wait);
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

    private void publish(
            String queue, InboundDelivery delivery, Envelope envelope, @Nullable String error) {
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

        /** Republished one attempt further on; it will come back when the delay is up. */
        RETRIED,

        /** Republished to the dead-letter queue; it will not come back on its own. */
        DEAD_LETTERED
    }
}
