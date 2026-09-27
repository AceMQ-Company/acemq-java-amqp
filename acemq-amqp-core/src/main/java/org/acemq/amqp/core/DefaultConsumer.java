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
import org.acemq.amqp.api.Ack;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.ConsumeContext;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.IdempotencyStore;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.MessageHandler;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.RetryPolicy;
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
    private final Interceptors interceptors;
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong rejected = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final @Nullable IdempotencyStore idempotency;
    private final RetryDispatcher retries;

    /**
     * Whether a failed handler should be requeued instead of going to the dead-letter queue.
     *
     * <p>Only when {@link ConsumerOptions#requeueOnFailure()} was asked for <em>and</em> no
     * retry policy was: a policy has always won over this flag, and a message that takes the
     * ladder must not also be requeued behind it. Narrowed to that pair deliberately, so
     * building the dispatcher unconditionally changes what happens to a message nobody had a
     * plan for and nothing else.
     */
    private final boolean requeueWithoutAPolicy;
    private final AtomicLong inFlight = new AtomicLong();
    private final java.util.concurrent.atomic.AtomicInteger prefetch;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean paused = new AtomicBoolean();
    private volatile @Nullable Subscription subscription;

    DefaultConsumer(
            TransportConnection connection,
            Codec codec,
            String queue,
            Class<T> payloadType,
            ConsumerOptions options,
            MessageHandler<T> handler,
            Telemetry telemetry) {
        this(connection, codec, queue, payloadType, options, handler, telemetry, new Interceptors());
    }

    DefaultConsumer(
            TransportConnection connection,
            Codec codec,
            String queue,
            Class<T> payloadType,
            ConsumerOptions options,
            MessageHandler<T> handler,
            Telemetry telemetry,
            Interceptors interceptors) {
        this.interceptors = interceptors;
        this.connection = connection;
        this.codec = codec;
        this.queue = queue;
        this.payloadType = payloadType;
        this.options = options;
        this.handler = handler;
        this.telemetry = telemetry;
        this.prefetch = new java.util.concurrent.atomic.AtomicInteger(options.prefetch());
        this.idempotency = options.idempotencyStore().orElse(null);
        this.requeueWithoutAPolicy = !options.retryPolicy().isPresent() && options.isRequeueOnFailure();
        // Built whether or not a policy was asked for, which is the fix for a default that
        // threw messages away.
        //
        // Without a policy there used to be no dispatcher, and a failed handler or an
        // undecodable body was rejected without requeue: the broker then dropped the message
        // unless the queue itself carried an x-dead-letter-exchange, which nothing here
        // declares. So the out-of-the-box behaviour of the one library in five that did this
        // was to delete a message whose handler had failed once, silently, with `rejected` as
        // the only trace. Go, .NET, Python and Ruby all publish it to {queue}.dlq instead, and
        // all four declare that queue from the consumer for exactly this reason.
        //
        // RetryPolicy.none() is one attempt and no rungs, so this declares the two exchanges,
        // {queue}.dlq and {queue}.parked, and nothing else — and a failure goes to the
        // dead-letter queue on the first attempt, because there is no second one. That is
        // Go's NoRetry() and .NET's null policy, spelled the same way.
        RetryPolicy policy = options.retryPolicy().orElseGet(RetryPolicy::none);
        RetryTopology topology = RetryTopology.forQueue(queue, policy);
        topology.declare(connection);
        this.retries = new RetryDispatcher(connection, topology, telemetry);
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
    public void pause() {
        if (!paused.compareAndSet(false, true)) {
            return;
        }
        Subscription current = this.subscription;
        if (current != null && current.isActive()) {
            // Cancelled rather than closed: the broker stops sending, and whatever is already
            // in a handler runs to completion. Closing here would block for as long as that
            // handler takes, which is not what pausing means.
            current.cancel();
        }
        this.subscription = null;
        log.info("paused consuming {}", queue);
    }

    @Override
    public void resume() {
        if (!paused.compareAndSet(true, false)) {
            return;
        }
        this.subscription = connection.subscribe(queue, prefetch.get(), this::dispatch);
        log.info("resumed consuming {} with prefetch {}", queue, prefetch.get());
    }

    @Override
    public boolean isPaused() {
        return paused.get();
    }

    @Override
    public boolean drain(java.time.Duration timeout) {
        // Cancel first, so nothing new arrives while waiting. Waiting with the subscription
        // still open would be waiting on a queue that keeps refilling.
        Subscription current = this.subscription;
        if (current != null && current.isActive()) {
            // Cancel, then wait on the caller's timeout. Closing first would wait on the
            // transport's own terms and make the timeout meaningless.
            current.cancel();
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
            // Retrying it would occupy the queue forever, so it goes to the parking lot,
            // keeping the original bytes for inspection.
            //
            // The parking lot exists whether or not a retry policy was asked for. It did not
            // before, and the undecodable message was rejected without requeue and dropped by
            // the broker: the bytes nobody could read were also the bytes nobody could look at.
            rejected.incrementAndGet();
            if (!retries.park(delivery, e)) {
                // The parking lot declined it, so this delivery is the only copy. Requeued
                // rather than acknowledged, which keeps the bytes on the broker for whoever
                // declares the queue; it is the same answer the retry hop gives when its rung
                // declines, and the same one Go, Python and .NET give here.
                log.error(
                        "could not park an undecodable message from {}; leaving it on the broker",
                        queue,
                        e);
                acknowledger.reject(true);
                return;
            }
            acknowledger.accept();
            return;
        }

        try (Telemetry.Scope scope = telemetry.consumeStarted(queue, message.envelope())) {
            dispatchWithin(scope, message, delivery, acknowledger);
        }
    }

    /**
     * Tells the interceptors a delivery failed, on the way to whatever the retry policy decides.
     *
     * <p>{@code afterHandle} runs here as well as on success, because an interceptor that set
     * something up on the way in has to be able to tear it down whatever happened. It is told the
     * delivery was released rather than accepted, which is the truthful answer: nothing processed
     * this message.
     */
    private void notifyFailure(@Nullable ConsumeContext context, Throwable failure) {
        if (context == null) {
            return;
        }
        interceptors.onConsumeError(context, failure);
        interceptors.afterHandle(context, Ack.release());
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

        ConsumeContext context = interceptors.isEmptyForConsuming() ? null : new ConsumeContext(queue, message);
        try {
            if (context != null) {
                // Before the handler, and allowed to throw: an interceptor that refuses a
                // message has to send it down the same path a failed handler takes, or the
                // message would be acknowledged with nothing having processed it.
                interceptors.beforeHandle(context);
            }
            handler.handle(message);
            if (context != null) {
                interceptors.afterHandle(context, Ack.accept());
            }
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
            notifyFailure(context, e);
            rejected.incrementAndGet();
            releaseClaim(messageId);
            scope.failed(e);
            // Reported as rejected, not dead-lettered, though it lands in the dead-letter queue
            // either way. The two are different events and a dashboard that cannot tell them
            // apart is missing the more useful one: this is a decision somebody's code took
            // about this message, where dead_lettered is the engine running out of attempts.
            // Go, Python and Ruby have always drawn the line here.
            scope.outcome(MetricNames.OUTCOME_REJECTED);
            log.warn("handler rejected {} as unprocessable: {}", message, e.getMessage());
            if (retries.onFailure(delivery, message.envelope(), e, true) == RetryDispatcher.Outcome.NOT_REPUBLISHED) {
                // The dead-letter queue declined it, so this delivery is the only copy there
                // is. Requeued for the same reason the ordinary failure path requeues.
                acknowledger.reject(true);
                return;
            }
            deadLettered.incrementAndGet();
            acknowledger.accept();
        } catch (Exception e) {
            notifyFailure(context, e);
            rejected.incrementAndGet();
            // Released before anything else: a failed attempt must leave the identifier
            // looking untouched, or the retry that follows is discarded as a duplicate and the
            // message is lost to a transient failure.
            releaseClaim(messageId);
            scope.failed(e);
            if (requeueWithoutAPolicy) {
                // Asked for explicitly, and only when no policy was: shedding load onto another
                // consumer is a thing somebody chooses, and it is the one case where putting the
                // message straight back is what was wanted.
                scope.outcome(MetricNames.OUTCOME_REJECTED);
                log.warn("handler failed for {}; requeueing as asked", message, e);
                acknowledger.reject(true);
                return;
            }
            {
                RetryDispatcher.Outcome outcome = retries.onFailure(delivery, message.envelope(), e, false);
                if (outcome == RetryDispatcher.Outcome.NOT_REPUBLISHED) {
                    // The hop did not land: the broker answered and declined it, so this delivery
                    // is the only copy there is. Acknowledging it here is the moment the message
                    // is lost, which is what happened until the dispatcher started reporting it —
                    // a dead-letter queue deleted by hand ate everything routed to it and the
                    // only visible trace was one queue draining.
                    //
                    // Requeued rather than rejected outright so the next delivery tries the hop
                    // again, which is what Go, Python and .NET do in the same place. A broker that
                    // keeps refusing means a redelivery loop, and that is the right failure: it is
                    // loud, and it is recoverable the moment the queue is declared.
                    acknowledger.reject(true);
                    scope.outcome(MetricNames.OUTCOME_REJECTED);
                    return;
                }
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
        return new ReceivedMessage<>(
                payload,
                envelope,
                queue,
                delivery.routingKey(),
                Instant.now(),
                delivery.replyTo().orElse(null),
                delivery.contentType());
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
