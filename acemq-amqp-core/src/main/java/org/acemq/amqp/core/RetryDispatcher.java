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
import org.acemq.amqp.transport.ConfirmResult;
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
            return deadLetter(delivery, envelope, "the handler reported an unprocessable message: " + describe(failure))
                    ? Outcome.DEAD_LETTERED
                    : Outcome.NOT_REPUBLISHED;
        }

        Duration age = envelope.age();
        Optional<RetryPolicy.Wait> next = policy.nextWait(envelope.attempt(), age);

        if (!next.isPresent()) {
            String reason = envelope.attempt() >= policy.maxAttempts()
                    ? "exhausted " + policy.maxAttempts() + " attempts"
                    : "exceeded the maximum message age of " + policy.maxMessageAge();
            return deadLetter(delivery, envelope, reason + ": " + describe(failure))
                    ? Outcome.DEAD_LETTERED
                    : Outcome.NOT_REPUBLISHED;
        }

        RetryPolicy.Wait wait = next.get();
        if (wait.isInBroker()) {
            switch (retryInBroker(delivery, envelope, wait.delay())) {
                case LANDED :
                    return Outcome.RETRIED;
                case DECLINED :
                    return Outcome.NOT_REPUBLISHED;
                default :
                    // The rung is not on the broker. Waiting here is the documented fallback.
                    break;
            }
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
    private RungResult retryInBroker(InboundDelivery delivery, Envelope envelope, Duration wait) {
        Optional<String> rung = topology.rungFor(wait);
        if (!rung.isPresent()) {
            // Loud, because a topology declared without its rungs otherwise looks like it works
            // right up until a long backoff quietly becomes a held prefetch slot.
            String missing = topology.rungNameFor(wait);
            log.error(
                    "{} is not on the broker, so {} will wait {} in this consumer instead. Declare it, or let"
                            + " the consumer declare it.",
                    missing,
                    envelope.id(),
                    wait);
            telemetry.retryRungMissing(topology.sourceQueue(), missing, wait);
            return RungResult.NO_RUNG;
        }

        Envelope advanced = envelope.nextAttempt();
        if (!publish(rung.get(), delivery, advanced, null, false)) {
            // The rung exists as far as the topology is concerned and the broker still would not
            // take the message. Reported as declined rather than fallen back to waiting here:
            // waiting would hold a delivery whose copy may or may not be on the rung.
            return RungResult.DECLINED;
        }
        telemetry.messageRetried(topology.sourceQueue(), advanced, wait);
        log.debug(
                "retrying {} attempt {} of {} after {} via {}",
                envelope.id(),
                advanced.attempt(),
                policy.maxAttempts(),
                wait,
                rung.get());
        return RungResult.LANDED;
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
        if (!publish(topology.sourceQueue(), delivery, advanced, null, false)) {
            return Outcome.NOT_REPUBLISHED;
        }
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
     * Puts back a message another consumer has claimed and not yet confirmed, without spending
     * an attempt on it.
     *
     * <p>Republished rather than requeued, for the reason everything else here is: a quorum queue
     * counts every requeue against its delivery limit (twenty by default on RabbitMQ 4), so a
     * message bounced while a five-minute lease runs out would be dropped or dead-lettered by the
     * broker long before the lease ended. A republished copy starts a fresh count. The envelope
     * goes out unchanged, so the attempt counter is not advanced and the policy never sees this as
     * a failure: a claim in progress can never be what dead-letters a message.
     *
     * <p>The pause first is what keeps it from becoming a hot loop between one consumer and the
     * queue while the lease runs.
     *
     * @return {@code true} when the copy landed and the original may be acknowledged
     */
    boolean deferInProgress(InboundDelivery delivery, Envelope envelope, Duration pause) {
        // ponytail: waits in this consumer, holding a prefetch slot for the pause. A claim in
        // progress is rare and the pause is short; a broker-side rung would free the slot.
        try {
            Thread.sleep(pause.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return publish(topology.sourceQueue(), delivery, envelope, null, false);
    }

    /**
     * Sends a delivery whose payload could not be decoded straight to the parking lot.
     *
     * @param delivery the delivery
     * @param failure why decoding failed
     * @return {@code true} when the parking lot has the message, so the caller may acknowledge
     *     the original. {@code false} means the broker answered and declined it — the parking
     *     lot is not there, or it refused the message — and this delivery is then the only copy
     *     there is. Acknowledging on that answer is the moment the bytes nobody could read
     *     become bytes nobody can look at either, which is the same defect the retry hop had
     *     until 0.7.4 and was still here afterwards.
     */
    boolean park(InboundDelivery delivery, Throwable failure) {
        String reason = "could not be decoded: " + describe(failure);
        Map<String, Object> headers = new LinkedHashMap<>(delivery.headers());
        headers.put(AceHeaders.ERROR, reason);

        OutboundMessage message = OutboundMessage.body(delivery.body())
                .exchange("")
                .routingKey(topology.parkingLotQueue())
                .headers(headers)
                .messageId(delivery.messageId())
                .contentType(delivery.contentType())
                .alwaysConfirmed()
                .build();

        if (!send(message, topology.parkingLotQueue())) {
            // Not counted as parked, because it was not. The counter an operator reads to know
            // a message reached the parking lot must not be raised by one that did not get
            // there, and the caller has to be told so it can leave the message on the broker.
            return false;
        }
        // The envelope is read from the headers rather than from the message, because the
        // message is the thing that would not decode. Headers survive a payload that does not,
        // so the type and the attempt count are still reportable.
        telemetry.messageParked(
                topology.sourceQueue(),
                EnvelopeHeaders.fromHeaders(
                        delivery.headers(),
                        delivery.messageId(),
                        delivery.routingKey().isEmpty() ? "message" : delivery.routingKey()),
                reason);
        log.warn(
                "parked an undecodable message from {} in {}: {}",
                delivery.queue(),
                topology.parkingLotQueue(),
                describe(failure));
        return true;
    }

    /**
     * Republishes to a queue that sets a message aside, counting a failure to get it there.
     *
     * <p>Counted and then rethrown, deliberately. Counting is not a reason to change what
     * happens to the message: the caller settles the delivery only once the copy is safely
     * elsewhere, and swallowing this would acknowledge a message that went nowhere. What the
     * counter buys is the ability to tell the two apart from outside, because a dead-letter
     * queue that was never declared and a dead-letter queue doing its job look identical in
     * every other series — one queue draining, in both cases.
     */
    private boolean send(OutboundMessage message, String target) {
        try {
            return landed(connection.send(message), target);
        } catch (RuntimeException e) {
            telemetry.setAsideFailed(topology.sourceQueue(), target, describe(e));
            log.error(
                    "could not set aside a message from {} in {}; it is going back to the broker",
                    topology.sourceQueue(),
                    target,
                    e);
            throw e;
        }
    }

    private boolean deadLetter(InboundDelivery delivery, Envelope envelope, String reason) {
        if (!publish(topology.deadLetterQueue(), delivery, envelope, reason, true)) {
            // Not counted as dead-lettered, because it was not: the counter an operator reads to
            // know a message reached the dead-letter queue must not be raised by one that did
            // not get there.
            return false;
        }
        telemetry.messageDeadLettered(topology.sourceQueue(), envelope, reason);
        log.warn(
                "dead-lettered {} from {} after {} attempts: {}",
                envelope.id(),
                topology.sourceQueue(),
                envelope.attempt(),
                reason);
        return true;
    }

    private boolean publish(
            String queue, InboundDelivery delivery, Envelope envelope, @Nullable String error, boolean setAside) {
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
                .alwaysConfirmed()
                .build();

        return setAside ? send(message, queue) : landed(connection.send(message), queue);
    }

    /**
     * Whether the broker actually took a republished message.
     *
     * <p>{@link TransportConnection#send} reports two of its three failures as return values
     * rather than exceptions: {@link ConfirmResult#unroutable} when nothing was bound to receive
     * the message, and {@link ConfirmResult#failed} when the broker refused it or never answered.
     * Only an IO or shutdown failure throws.
     *
     * <p>That result used to be discarded here, and the consequence was message loss rather than a
     * missing counter. {@code DefaultConsumer} acknowledges the original delivery on the strength
     * of this dispatcher having republished it — correctly, since the message is meant to be
     * elsewhere by then — so a hop that reached no queue was acknowledged into nothing. A
     * dead-letter queue deleted by hand, or a rung queue that was never declared, silently ate
     * every message routed to it, and the only visible trace was one queue draining.
     */
    private boolean landed(ConfirmResult result, String target) {
        if (result.isConfirmed() && result.isRouted()) {
            return true;
        }
        String reason = result.detail() == null
                ? (result.isConfirmed() ? "nothing was bound to receive it" : "the broker did not confirm it")
                : result.detail();
        telemetry.setAsideFailed(topology.sourceQueue(), target, reason);
        log.error(
                "a message from {} was not taken by {}: {}. It is going back to the broker rather"
                        + " than being acknowledged into nothing",
                topology.sourceQueue(),
                target,
                reason);
        return false;
    }

    private static String describe(Throwable failure) {
        if (failure == null) {
            return "no reason given";
        }
        String message = failure.getMessage();
        return failure.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    /**
     * What a rung queue did with a message, which is three things rather than two.
     *
     * <p>A missing rung and a rung that refused the message are not the same: the first is a
     * topology that was never fully declared and the documented answer is to wait out the delay
     * in this consumer, while the second is a broker that answered and declined, where waiting
     * would hold a delivery whose copy may or may not be on the rung.
     */
    private enum RungResult {
        LANDED, DECLINED, NO_RUNG
    }

    /** What the dispatcher did with a failed delivery. */
    enum Outcome {

        /** Republished one attempt further on; it will come back when the delay is up. */
        RETRIED,

        /** Republished to the dead-letter queue; it will not come back on its own. */
        DEAD_LETTERED,

        /**
         * The republish did not land, so the original delivery must go back to the broker.
         *
         * <p>Reported when the broker answered and declined: nothing was bound to receive the
         * message, or it refused to confirm it. The message is still only on the source queue, so
         * acknowledging the original would be the moment it is lost — {@code DefaultConsumer}
         * rejects it with requeue instead, and the next delivery tries the hop again.
         */
        NOT_REPUBLISHED
    }
}
