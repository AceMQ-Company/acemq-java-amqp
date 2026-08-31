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
 * Runs around every handler on a connection.
 *
 * <p>For the work that surrounds handling rather than being part of it: putting the correlation
 * identifier into the logging context, adopting the tenant the message came from, opening and
 * closing a unit of work, timing something the built-in metrics do not cover.
 *
 * <pre>{@code
 * mq.intercept(new ConsumeInterceptor() {
 *     public void beforeHandle(ConsumeContext context) {
 *         MDC.put("correlationId", context.envelope().correlationId());
 *     }
 *     public void afterHandle(ConsumeContext context, Ack ack) {
 *         MDC.remove("correlationId");
 *     }
 * });
 * }</pre>
 *
 * <h2>Failure</h2>
 *
 * <p>Throwing from {@link #beforeHandle} means the handler never runs and the delivery is treated
 * exactly as a failed handler would be — retried, then dead-lettered. An interceptor that refuses
 * a message must be willing for that message to end up in the dead-letter queue, which is the
 * honest outcome: the alternative is acknowledging something nothing processed.
 *
 * <p>{@link #afterHandle} runs whether the handler succeeded or failed, so it is the place to
 * undo whatever {@link #beforeHandle} set up. Throwing from it is logged and ignored: the message
 * has already been settled, and an exception cannot un-settle it.
 *
 * <p>Implementations must be thread safe, and must not assume that {@code beforeHandle} and
 * {@code afterHandle} run on the same thread as any other message.
 */
public interface ConsumeInterceptor {

    /**
     * Called after the payload is decoded and before the handler runs.
     *
     * @param context the message about to be handled
     * @throws RuntimeException to refuse the message, which fails the delivery
     */
    void beforeHandle(ConsumeContext context);

    /**
     * Called after the handler returns or throws, before the delivery is settled.
     *
     * @param context the message that was handled
     * @param ack how the delivery is about to be settled
     */
    default void afterHandle(ConsumeContext context, Ack ack) {
        // Nothing to undo by default.
    }

    /**
     * Called when the handler threw, before {@link #afterHandle}.
     *
     * @param context the message being handled
     * @param failure what the handler threw
     */
    default void onError(ConsumeContext context, Throwable failure) {
        // Most interceptors have nothing to add to a failure.
    }

    /**
     * @return where this interceptor sits; lower runs first on the way in, and the reverse on the
     *     way out, so a pair that sets up and tears down state nests properly
     */
    default int order() {
        return 0;
    }
}
