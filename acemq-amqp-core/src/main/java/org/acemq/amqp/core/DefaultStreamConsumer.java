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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.MessageHandler;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.transport.InboundDelivery;
import org.acemq.amqp.transport.Subscription;
import org.acemq.amqp.transport.TransportConnection;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a stream from a stated offset.
 *
 * <p>Acknowledging here means "I have got this far", not "delete this". Nothing is removed and
 * nothing is moved, so the retry ladder and the dead-letter queue have no part to play — see
 * {@link StreamConsumer} for what that leaves.
 *
 * @param <T> payload type
 */
final class DefaultStreamConsumer<T> implements StreamConsumer {

    /** Where the broker records a message's position in the log. */
    static final String OFFSET_HEADER = "x-stream-offset";

    private static final Logger log = LoggerFactory.getLogger(DefaultStreamConsumer.class);

    private final TransportConnection connection;
    private final Codec codec;
    private final String queue;
    private final Class<T> payloadType;
    private final StreamOptions options;
    private final MessageHandler<T> handler;
    private final Telemetry telemetry;

    private final AtomicLong handled = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong skipped = new AtomicLong();
    private final AtomicLong lastOffset = new AtomicLong(-1L);
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<@Nullable Throwable> stoppedBy = new AtomicReference<>();

    private volatile @Nullable Subscription subscription;

    DefaultStreamConsumer(
            TransportConnection connection,
            Codec codec,
            String queue,
            Class<T> payloadType,
            StreamOptions options,
            MessageHandler<T> handler,
            Telemetry telemetry) {
        this.connection = connection;
        this.codec = codec;
        this.queue = queue;
        this.payloadType = payloadType;
        this.options = options;
        this.handler = handler;
        this.telemetry = telemetry;
    }

    void start() {
        Map<String, Object> arguments = new LinkedHashMap<>();
        arguments.put(OFFSET_HEADER, options.offset().toConsumerArgument());

        running.set(true);
        this.subscription = connection.subscribe(
                queue, options.prefetchCount(), arguments, (delivery, acknowledger) -> {
                    if (!running.get()) {
                        // Stopped by an earlier failure. Deliveries already in flight are left
                        // unsettled so the broker keeps this consumer's position where it was.
                        return;
                    }
                    dispatch(delivery, acknowledger);
                });
        log.info("reading stream {} from {} with prefetch {}", queue, options.offset(), options.prefetchCount());
    }

    private void dispatch(InboundDelivery delivery, org.acemq.amqp.transport.Acknowledger acknowledger) {
        long offset = offsetOf(delivery);
        Message<T> message;
        try {
            message = decode(delivery);
        } catch (RuntimeException e) {
            // A payload that will not decode will not decode next time either, and there is
            // nowhere on a stream to put it. Treated as any other handler failure so the
            // configured policy decides, rather than being silently different.
            recordFailure(offset, e, acknowledger);
            return;
        }

        try (Telemetry.Scope scope = telemetry.consumeStarted(queue, message.envelope())) {
            try {
                handler.handle(message);
                if (offset >= 0) {
                    lastOffset.set(offset);
                }
                handled.incrementAndGet();
                scope.outcome(MetricNames.OUTCOME_ACKED);
                acknowledger.accept();
            } catch (Exception e) {
                scope.failed(e);
                recordFailure(offset, e, acknowledger);
            }
        }
    }

    private void recordFailure(long offset, Throwable failure, org.acemq.amqp.transport.Acknowledger acknowledger) {
        failed.incrementAndGet();
        if (options.onFailure() == StreamOptions.OnFailure.SKIP) {
            skipped.incrementAndGet();
            if (offset >= 0) {
                lastOffset.set(offset);
            }
            log.warn("skipping offset {} on stream {} after a failure; nothing else will report this gap",
                    offset, queue, failure);
            // Acknowledged despite failing, because on a stream that only moves the position on.
            // Nothing is discarded: the message is still in the log for anything reading it again.
            acknowledger.accept();
            return;
        }

        stoppedBy.compareAndSet(null, failure);
        running.set(false);
        log.error("stopping the reader of stream {} at offset {}. The message is still in the log; fix the handler"
                + " and resume from offset {}.", queue, offset, offset < 0 ? "the last recorded" : offset, failure);
        // Deliberately not settled. Leaving it unacknowledged keeps the broker's idea of this
        // consumer's position behind the message that failed, so a restart sees it again.
        stopSubscription();
    }

    private long offsetOf(InboundDelivery delivery) {
        Object raw = delivery.headers().get(OFFSET_HEADER);
        return raw instanceof Number ? ((Number) raw).longValue() : -1L;
    }

    private Message<T> decode(InboundDelivery delivery) {
        Envelope envelope = EnvelopeHeaders.fromHeaders(
                delivery.headers(),
                delivery.messageId(),
                delivery.routingKey().isEmpty() ? "message" : delivery.routingKey());
        T payload = codec.decode(delivery.body(), payloadType, delivery.contentType());
        return new ReceivedMessage<>(payload, envelope, queue, delivery.routingKey(), Instant.now());
    }

    @Override
    public String queue() {
        return queue;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public OptionalLong lastHandledOffset() {
        long current = lastOffset.get();
        return current < 0 ? OptionalLong.empty() : OptionalLong.of(current);
    }

    @Override
    public long handled() {
        return handled.get();
    }

    @Override
    public long failed() {
        return failed.get();
    }

    @Override
    public long skipped() {
        return skipped.get();
    }

    @Override
    public Optional<Throwable> stoppedBy() {
        return Optional.ofNullable(stoppedBy.get());
    }

    @Override
    public void close() {
        running.set(false);
        stopSubscription();
    }

    private void stopSubscription() {
        Subscription current = this.subscription;
        if (current != null) {
            this.subscription = null;
            try {
                current.close();
            } catch (RuntimeException e) {
                log.debug("could not close the subscription to stream {}", queue, e);
            }
        }
    }

    @Override
    public String toString() {
        return "StreamConsumer{queue=" + queue + ", handled=" + handled.get() + ", lastOffset="
                + (lastOffset.get() < 0 ? "none" : lastOffset.get()) + "}";
    }
}
