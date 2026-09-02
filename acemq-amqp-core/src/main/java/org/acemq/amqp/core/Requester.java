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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Message;
import org.acemq.amqp.api.MetricNames;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.transport.QueueType;

/**
 * Asks a question and waits for the answer.
 *
 * <p><strong>Read this before using it.</strong> Request/reply over a broker is synchronous
 * calling wearing asynchronous clothes, and it inherits the worst of both: the caller is blocked
 * like an HTTP client, and the failure modes are a message broker's. If the two services can talk
 * over HTTP or gRPC, they should — those tools have timeouts, load balancing, circuit breakers and
 * tracing that a messaging library will not match.
 *
 * <p>What it is genuinely for: reaching a service that is <em>only</em> on the broker — no HTTP
 * endpoint, behind a firewall, or one worker among many where the broker is already doing the load
 * balancing. That case is real, and doing it by hand means reply queues, correlation ids and a
 * timeout that somebody always forgets.
 *
 * <pre>{@code
 * try (Requester requester = mq.requester()) {
 *     Price price = requester.request("", "pricing", quote, Price.class, Duration.ofSeconds(5));
 * }
 * }</pre>
 *
 * <p>One reply queue serves every request from this instance, and replies are matched by
 * correlation id. A requester per connection is the intended shape; one per call would create and
 * destroy a queue for every question asked.
 */
public final class Requester implements AutoCloseable {

    /**
     * How long the broker keeps the reply queue after the last requester using it goes away.
     *
     * <p>Reply queues are rubbish if their owner has died: nobody is waiting for what arrives in
     * them. Without this, a process killed mid-request leaves a queue behind forever, and a service
     * that restarts often turns into thousands of them — which is a real way to run a broker out of
     * memory with nothing obviously wrong.
     */
    private static final long QUEUE_EXPIRY_MILLIS = Duration.ofMinutes(10).toMillis();

    private final AceMq mq;
    private final String replyQueue;
    private final MessageConsumer replies;
    private final Map<String, CompletableFuture<Message<byte[]>>> waiting = new ConcurrentHashMap<>();
    private final AtomicLong timedOut = new AtomicLong();
    private final AtomicLong unmatched = new AtomicLong();

    Requester(AceMq mq) {
        this.mq = mq;
        this.replyQueue = "acemq.reply." + UUID.randomUUID();

        // Not durable, and self-deleting. A reply queue holds answers nobody will read once the
        // asking process is gone, so outliving it is pure cost.
        mq.declareQueue(replyQueue, QueueType.CLASSIC, Map.of("x-expires", QUEUE_EXPIRY_MILLIS));

        // Replies are read as raw bytes and decoded once a caller has been matched to them,
        // because one requester may be waiting on several different response types at once.
        this.replies = mq.consume(
                replyQueue,
                byte[].class,
                ConsumerOptions.prefetch(100).as(Codecs.byName("bytes")),
                message -> {
                    String id = message.envelope().correlationId();
                    CompletableFuture<Message<byte[]>> caller = waiting.remove(id);
                    if (caller == null) {
                        // The caller gave up, or this is a duplicate. Counted rather than
                        // logged per message: under a slow responder this is the number that
                        // says the timeout is too short, and it is worth graphing.
                        unmatched.incrementAndGet();
                        return;
                    }
                    caller.complete(message);
                });
    }

    /**
     * Asks, and waits.
     *
     * @param exchange exchange to publish the request to, or the empty string for a queue
     * @param routingKey routing key, or the queue name when the exchange is empty
     * @param request the request payload
     * @param responseType type to decode the reply into
     * @param timeout how long to wait before giving up
     * @param <Q> request type
     * @param <A> response type
     * @return the reply
     * @throws RequestTimedOutException if no reply arrives in time
     */
    public <Q, A> A request(
            String exchange, String routingKey, Q request, Class<A> responseType, Duration timeout) {
        String correlationId = UUID.randomUUID().toString();
        Envelope envelope = Envelope.of(request.getClass().getSimpleName())
                .correlationId(correlationId)
                .build();

        // Only the blocking call is wrapped. requestAsync hands back a future with no timeout
        // attached, so the wait belongs to whoever holds it -- a scope closed at return would
        // measure the publish and call it the round trip.
        Telemetry.Scope scope = mq.telemetry().requestStarted(
                routingKey == null || routingKey.isEmpty() ? exchange : routingKey, envelope);
        try {
            A answer = send(exchange, routingKey, request, responseType, envelope)
                    .get(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            scope.outcome(MetricNames.OUTCOME_ANSWERED);
            return answer;
        } catch (java.util.concurrent.TimeoutException e) {
            scope.outcome(MetricNames.OUTCOME_TIMED_OUT);
            // Forgetting the caller is the point. Left in place, the entry is never removed --
            // one leaked future per timeout, and a late reply would be handed to a caller that
            // stopped waiting, which looks like the request having succeeded.
            waiting.remove(correlationId);
            timedOut.incrementAndGet();
            throw new RequestTimedOutException(
                    "no reply to the request sent to '" + routingKey + "' within " + timeout
                            + ". The request may still be queued, being handled, or already done with"
                            + " the reply lost on the way back, so resending it is only safe if the"
                            + " responder is idempotent.",
                    timeout);
        } catch (InterruptedException e) {
            waiting.remove(correlationId);
            scope.failed(e);
            Thread.currentThread().interrupt();
            throw new AceMqException("interrupted while waiting for a reply from '" + routingKey + "'", e);
        } catch (java.util.concurrent.ExecutionException e) {
            waiting.remove(correlationId);
            Throwable cause = e.getCause();
            scope.failed(cause == null ? e : cause);
            throw new AceMqException("the request to '" + routingKey + "' failed", cause == null ? e : cause);
        } finally {
            scope.close();
        }
    }

    /**
     * Asks, and hands back a future.
     *
     * <p>The future never completes on its own if no reply arrives — a timeout belongs to the
     * caller, and {@link CompletableFuture#orTimeout} is the usual way to attach one.
     *
     * @return a future completed with the decoded reply
     */
    public <Q, A> CompletableFuture<A> requestAsync(
            String exchange, String routingKey, Q request, Class<A> responseType) {
        return send(exchange, routingKey, request, responseType,
                Envelope.of(request.getClass().getSimpleName())
                        .correlationId(UUID.randomUUID().toString())
                        .build());
    }

    private <Q, A> CompletableFuture<A> send(
            String exchange, String routingKey, Q request, Class<A> responseType, Envelope envelope) {
        String correlationId = envelope.correlationId();
        CompletableFuture<Message<byte[]>> raw = new CompletableFuture<>();
        waiting.put(correlationId, raw);

        try {
            mq.<Q>publisher(exchange, routingKey).replyingTo(replyQueue).send(request, envelope);
        } catch (RuntimeException failure) {
            // Nothing will ever complete this one, and leaving it in the map leaks a future
            // per failed publish.
            waiting.remove(correlationId);
            throw failure;
        }

        Codec codec = Codecs.forConsuming();
        return raw.thenApply(
                message -> codec.decode(message.payload(), responseType, message.contentType().orElse(null)));
    }

    /** @return the queue replies arrive on; useful in a log line, and nowhere else */
    public String replyQueue() {
        return replyQueue;
    }

    /** @return how many requests gave up waiting */
    public long timedOut() {
        return timedOut.get();
    }

    /** @return replies that arrived with nobody waiting for them, usually after a timeout */
    public long unmatched() {
        return unmatched.get();
    }

    @Override
    public void close() {
        replies.close();
        // Everyone still waiting is told, rather than left blocked until their own timeout.
        waiting.values().forEach(caller -> caller
                .completeExceptionally(new AceMqException("the requester was closed before a reply arrived")));
        waiting.clear();
        try {
            mq.deleteQueue(replyQueue);
        } catch (RuntimeException ignored) {
            // The queue expires on its own; failing to delete it is not worth an exception on
            // the way out.
        }
    }

    /** Replies waiting to be matched, exposed for tests and diagnostics. */
    Map<String, CompletableFuture<Message<byte[]>>> pending() {
        return Collections.unmodifiableMap(waiting);
    }
}
