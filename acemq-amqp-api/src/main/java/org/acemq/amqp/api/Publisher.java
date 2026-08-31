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

/**
 * Sends messages of one type to one destination.
 *
 * <p>Publishers are thread safe and meant to be long lived: build one per message type at
 * startup and reuse it.
 *
 * @param <T> payload type
 */
public interface Publisher<T> extends AutoCloseable {

    /**
     * Publishes a message and waits for the broker to confirm it.
     *
     * @param payload the payload to send
     * @return the confirmed result
     * @throws PublishFailedException if the broker rejected the message, could not route it,
     *     or did not confirm in time
     */
    PublishResult send(T payload);

    /**
     * Publishes a message built on an explicit envelope.
     *
     * <p>Used when correlation or causation must be carried from a message being handled, so
     * that a whole flow can be followed in traces and logs.
     *
     * @param payload the payload to send
     * @param envelope metadata to publish it with
     * @return the confirmed result
     * @throws PublishFailedException as for {@link #send(Object)}
     */
    PublishResult send(T payload, Envelope envelope);

    /**
     * Publishes without waiting for the broker to confirm.
     *
     * <p>A synchronous publish costs a round trip per message, so a loop over ten thousand of them
     * spends nearly all its time waiting. This lets the next message go out while the last is
     * still in flight, and is typically an order of magnitude faster for bulk work.
     *
     * <p>Nothing about the guarantees changes: the future carries the same result
     * {@link #send(Object)} returns, and fails the same way — including when nothing was bound to
     * receive the message. What changes is <em>when</em> you find out.
     *
     * <p><strong>A future nobody waits on is a message nobody knows the fate of.</strong> That is
     * the failure this library exists to prevent, so somebody has to check, eventually. If you do
     * not intend to look at the result, use {@link #send(Object)} and take the round trip.
     *
     * <pre>{@code
     * List<CompletableFuture<PublishResult>> inFlight = new ArrayList<>();
     * for (Order order : thousands) {
     *     inFlight.add(publisher.sendAsync(order));
     * }
     * CompletableFuture.allOf(inFlight.toArray(new CompletableFuture[0])).join();
     * }</pre>
     *
     * <p>Publishing blocks once too many messages are awaiting confirmation, which is deliberate:
     * an asynchronous publisher with no ceiling is a memory leak that looks like throughput.
     *
     * @param payload the payload to send
     * @return a future completed with the broker's answer, or failed as {@link #send(Object)}
     *     would have thrown
     */
    java.util.concurrent.CompletableFuture<PublishResult> sendAsync(T payload);

    /**
     * Publishes without waiting, with an envelope of your own.
     *
     * @param payload the payload to send
     * @param envelope identity, correlation and headers to travel with it
     * @return a future completed with the broker's answer
     */
    java.util.concurrent.CompletableFuture<PublishResult> sendAsync(T payload, Envelope envelope);

    /**
     * Publishes a batch and waits for every confirm.
     *
     * <p>What most bulk publishing actually wants: the throughput of pipelining with the safety of
     * having waited. Every message goes out before any confirm is awaited, then all of them are
     * checked together.
     *
     * <p>Fails if <em>any</em> message failed, and the exception names how many succeeded — a
     * partial batch is the normal outcome of a broker problem halfway through, and pretending
     * otherwise would leave the caller resending messages that already arrived.
     *
     * <p>This is not atomic. AMQP has no such thing: there is no way to publish a hundred messages
     * such that all or none arrive, and a library that offered one would be lying.
     *
     * @param payloads the payloads to send, in order
     * @return the results, in the same order
     * @throws PublishFailedException if any message was not confirmed
     */
    java.util.List<PublishResult> sendAll(java.util.Collection<? extends T> payloads);

    @Override
    void close();
}
