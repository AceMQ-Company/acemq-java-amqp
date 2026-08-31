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

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.acemq.amqp.api.AceFatalException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.IdempotencyStore;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.MessageHandler;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.transport.Acknowledger;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.Subscription;
import org.acemq.amqp.transport.TransportConnection;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs one handler against one queue.
 *
 * <p>The contract this class keeps is that every delivery is settled exactly once, whatever
 * the handler does. A handler that throws, a payload that will not decode, and a handler that
 * throws an {@link Error} all end with the broker being told something, because a delivery
 * that is never settled holds a prefetch slot until the connection drops and then comes back
 * as a redelivery nobody expected.
 *
 * @param <T> payload type
 */
final class DefaultConsumer<T> implements MessageConsumer {

    private static final Logger log = LoggerFactory.getLogger(DefaultConsumer.class);

    private final TransportConnection connection;
    private final Codec codec;
    private final String queue;
    private final Class<T> payloadType;
    private final ConsumerOptions options;
    private final MessageHandler<T> handler;
    private final Telemetry telemetry;
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final @Nullable IdempotencyStore idempotency;
    private final @Nullable RetryDispatcher retries;
    private final AtomicLong inFlight = new AtomicLong();
    private final java.util.concurrent.atomic.AtomicInteger prefetch;
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile @Nullable Subscription subscription;

    DefaultConsumer(
            TransportConnection connection,
            Codec codec,
            String queue,
            Class<T> payloadType,
            ConsumerOptions options,
            MessageHandler<T> handler,
            Telemetry telemetry) {
        this.connection = connection;
        this.codec = codec;
        this.queue = queue;
        this.payloadType = payloadType;
        this.options = options;
        this.handler = handler;
        this.telemetry = telemetry;
        this.prefetch = new java.util.concurrent.atomic.AtomicInteger(options.prefetch());
        this.idempotency = options.idempotencyStore().orElse(null);
        this.retries = options.retryPolicy()
                .map(policy -> {
                    RetryTopology topology = RetryTopology.forQueue(queue, policy);
                    topology.declare(connection);
                    return new RetryDispatcher(connection, topology, telemetry);
                })
                .orElse(null);
    }

    void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        this.subscription = connection.subscribe(queue, options.prefetch(), this::dispatch);
        log.info("consuming {} with prefetch {}", queue, options.prefetch());
    }

    @Override
    public long inFlight() {
        return inFlight.get();
    }

    @Override
    public int prefetch() {
        return prefetch.get();
    }

    @Override
    public void prefetch(int updated) {
        Subscription current = this.subscription;
        if (current == null) {
            throw new org.acemq.amqp.api.AceMqException("this consumer is not running, so its prefetch cannot"
                    + " be changed");
        }
        current.setPrefetch(updated);
        prefetch.set(updated);
        log.info("prefetch on {} changed to {}", queue, updated);
    }

    @Override
    public boolean drain(java.time.Duration timeout) {
        // Cancel first, so nothing new arrives while waiting. Waiting with the subscription
        // still open would be waiting on a queue that keeps refilling.
        Subscription current = this.subscription;
        if (current != null && current.isActive()) {
            current.close();
        }
        running.set(false);

        long deadline = System.nanoTime() + timeout.toNanos();
        while (inFlight.get() > 0 && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return inFlight.get() == 0;
            }
        }
        boolean quiet = inFlight.get() == 0;
        if (!quiet) {
            log.warn("drained {} but {} message(s) are still being handled after {}", queue, inFlight.get(), timeout);
        }
        return quiet;
    }

    private void dispatch(InboundDelivery delivery, Acknowledger acknowledger) {
        inFlight.incrementAndGet();
        try {
            dispatchCounted(delivery, acknowledger);
        } finally {
            inFlight.decrementAndGet();
        }
    }

    private void dispatchCounted(InboundDelivery delivery, Acknowledger acknowledger) {
        Message<T> message;
        try {
            message = decode(delivery);
        } catch (Exception e) {
            // A payload that cannot be decoded will not decode on the next attempt either.
            // Retrying it would occupy the queue forever, so it goes to the parking lot when
            // one exists, keeping the original bytes for inspection, and is otherwise
            // rejected without requeue.
            rejected.incrementAndGet();
            if (retries != null) {
                retries.park(delivery, e);
                acknowledger.accept();
            } else {
                log.warn("rejecting a message on {} that could not be decoded; it will not be retried", queue, e);
                acknowledger.reject(false);
            }
            return;
        }

        try (Telemetry.Scope scope = telemetry.consumeStarted(queue, message.envelope())) {
            dispatchWithin(scope, message, delivery, acknowledger);
        }
    }

    /** Runs the handler and settles the delivery, recording how it went. */
    private void dispatchWithin(
            Telemetry.Scope scope, Message<T> message, InboundDelivery delivery, Acknowledger acknowledger) {
        String messageId = message.envelope().id();

        if (idempotency != null && !idempotency.claim(messageId)) {
            // Already handled, or being handled right now by someone else. Acknowledging is
            // correct rather than merely convenient: the work is done or in hand, and leaving
            // the delivery unsettled would only cause it to be redelivered again later.
            duplicates.incrementAndGet();
            scope.outcome(MetricNames.OUTCOME_ACKED);
            log.debug("skipping {}: already handled", messageId);
            acknowledger.accept();
            return;
        }

        try {
            handler.handle(message);
            acknowledged.incrementAndGet();
            if (idempotency != null) {
                // Confirmed only after the handler returned. Recording it earlier would mean a
                // crash mid-handler leaves the work undone and unrepeatable, because the
                // message would look handled for ever after.
                idempotency.confirm(messageId);
            }
            scope.outcome(MetricNames.OUTCOME_ACKED);
            acknowledger.accept();
        } catch (AceFatalException e) {
            // Fatal means retrying cannot help, so the retry ladder is skipped entirely and
            // the message goes straight to the dead-letter queue.
            rejected.incrementAndGet();
            releaseClaim(messageId);
            scope.failed(e);
            scope.outcome(MetricNames.OUTCOME_DEAD_LETTERED);
            log.warn("handler rejected {} as unprocessable: {}", message, e.getMessage());
            if (retries != null) {
                retries.onFailure(delivery, message.envelope(), e, true);
                deadLettered.incrementAndGet();
                acknowledger.accept();
            } else {
                acknowledger.reject(false);
            }
        } catch (Exception e) {
            rejected.incrementAndGet();
            // Released before anything else: a failed attempt must leave the identifier
            // looking untouched, or the retry that follows is discarded as a duplicate and the
            // message is lost to a transient failure.
            releaseClaim(messageId);
            scope.failed(e);
            if (retries != null) {
                RetryDispatcher.Outcome outcome = retries.onFailure(delivery, message.envelope(), e, false);
                if (outcome == RetryDispatcher.Outcome.RETRIED) {
                    retried.incrementAndGet();
                    scope.outcome(MetricNames.OUTCOME_RETRIED);
                } else {
                    deadLettered.incrementAndGet();
                    scope.outcome(MetricNames.OUTCOME_DEAD_LETTERED);
                }
                // The message has already been republished elsewhere, so the original copy is
                // acknowledged rather than rejected into a requeue loop.
                acknowledger.accept();
            } else {
                scope.outcome(MetricNames.OUTCOME_REJECTED);
                log.warn("handler failed for {}", message, e);
                acknowledger.reject(options.isRequeueOnFailure());
            }
        } catch (Throwable t) {
            // An Error means the process is in trouble, but the delivery still has to be
            // settled before the stack unwinds, or it is stuck until the connection dies.
            rejected.incrementAndGet();
            releaseClaim(messageId);
            scope.outcome(MetricNames.OUTCOME_REJECTED);
            acknowledger.reject(false);
            throw t;
        }
    }

    private Message<T> decode(InboundDelivery delivery) {
        Envelope envelope = EnvelopeHeaders.fromHeaders(
                delivery.headers(), delivery.messageId(),
                delivery.routingKey().isEmpty() ? "message" : delivery.routingKey());
        // The content type goes to the codec, because a codec that reads more than one format
        // cannot choose between them without knowing what the sender said it wrote.
        T payload = codec.decode(delivery.body(), payloadType, delivery.contentType());
        return new ReceivedMessage<>(payload, envelope, queue, delivery.routingKey(), Instant.now());
    }

    @Override
    public String queue() {
        return queue;
    }

    @Override
    public boolean isRunning() {
        Subscription current = subscription;
        return running.get() && current != null && current.isActive();
    }

    @Override
    public long acknowledged() {
        return acknowledged.get();
    }

    @Override
    public long rejected() {
        return rejected.get();
    }

    /** Gives up a claim, tolerating a store that fails so it cannot break delivery. */
    private void releaseClaim(String messageId) {
        if (idempotency == null) {
            return;
        }
        try {
            idempotency.release(messageId);
        } catch (RuntimeException e) {
            // A store that cannot release leaves the identifier claimed until it expires,
            // which delays a retry. Letting the exception escape would leave the delivery
            // unsettled instead, which is worse.
            log.warn("could not release the idempotency claim on {}", messageId, e);
        }
    }

    @Override
    public long duplicates() {
        return duplicates.get();
    }

    @Override
    public long retried() {
        return retried.get();
    }

    @Override
    public long deadLettered() {
        return deadLettered.get();
    }

    @Override
    public void close() {
        // Not guarded on the running flag any more: drain() clears it first, and a drained
        // consumer must still release its subscription when it is closed.
        boolean wasRunning = running.getAndSet(false);
        Subscription current = subscription;
        if (current != null) {
            current.close();
            subscription = null;
        }
        if (!wasRunning) {
            return;
        }
        log.info("stopped consuming {} after {} acknowledged and {} rejected", queue, acknowledged(), rejected());
    }

    @Override
    public String toString() {
        return "Consumer{queue=" + queue + ", running=" + isRunning() + "}";
    }
}
