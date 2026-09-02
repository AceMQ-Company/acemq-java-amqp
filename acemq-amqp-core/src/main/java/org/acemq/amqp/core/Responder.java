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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.acemq.amqp.api.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The other end of {@link Requester}: reads a request, computes an answer, sends it back.
 *
 * <pre>{@code
 * try (Responder responder = mq.respond("pricing", Quote.class, Price.class, quote -> price(quote))) {
 *     // serving until closed
 * }
 * }</pre>
 *
 * <p>The reply goes wherever the request asked, which is AMQP's own {@code reply-to} property, and
 * carries the request's correlation id back unchanged. Neither is the handler's problem.
 */
public final class Responder implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Responder.class);

    private final MessageConsumer consumer;
    private final AtomicLong answered = new AtomicLong();
    private final AtomicLong unanswerable = new AtomicLong();

    <Q, A> Responder(
            AceMq mq, String queue, Class<Q> requestType, ConsumerOptions options, Function<Q, A> handler) {
        this.consumer = mq.consume(queue, requestType, options, message -> {
            String replyTo = message.replyTo().orElse(null);
            if (replyTo == null) {
                // A request nobody can answer. Failing here would retry it forever and
                // eventually dead-letter it; the honest thing is to handle it, count it, and
                // say so once -- the sender is the thing that is broken.
                unanswerable.incrementAndGet();
                log.warn(
                        "a message on {} asked for no reply, so none was sent. It was published"
                                + " without a reply-to, which usually means the sender used publish"
                                + " where it meant to use request.",
                        queue);
                return;
            }

            A answer = handler.apply(message.payload());

            // The empty exchange: reply-to names a queue directly. The correlation id is the
            // caller's, carried back unchanged -- it is the only thing tying the answer to the
            // question.
            mq.<A>publisher("", replyTo)
                    .send(answer, Envelope.of(answer == null ? "Reply" : answer.getClass().getSimpleName())
                            .correlationId(message.envelope().correlationId())
                            .build());
            answered.incrementAndGet();
        });
    }

    /** @return how many requests were answered */
    public long answered() {
        return answered.get();
    }

    /**
     * @return how many arrived with no reply-to. Anything above zero means a caller is publishing
     *     where it means to request
     */
    public long unanswerable() {
        return unanswerable.get();
    }

    /** @return whether this responder is still serving */
    public boolean isRunning() {
        return consumer.isRunning();
    }

    @Override
    public void close() {
        // Drained rather than cut off: a request being answered right now has a caller blocked
        // on the other side, and dropping it turns their call into a timeout.
        consumer.drain(Duration.ofSeconds(10));
        consumer.close();
    }
}
