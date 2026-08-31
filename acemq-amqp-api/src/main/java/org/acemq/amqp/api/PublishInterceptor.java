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
 * Runs around every publish on a connection.
 *
 * <p>The extension point for the things every message in an organisation needs and no library can
 * guess: a tenant identifier, an authorisation token, a schema version, a size limit, a log
 * context. Without it these end up copied into every call site, where one of them is always
 * missing.
 *
 * <pre>{@code
 * mq.intercept((PublishInterceptor) context ->
 *         context.withEnvelope(context.envelope().toBuilder()
 *                 .header("tenant", TenantContext.current())
 *                 .build()));
 * }</pre>
 *
 * <h2>Failure</h2>
 *
 * <p>Throwing from {@link #beforePublish} <strong>fails the publish</strong>, and nothing is sent.
 * That is the point: an interceptor enforcing a policy has to be able to refuse. Throwing from
 * {@link #afterConfirm} or {@link #onError} is logged and otherwise ignored, because the message
 * has already gone and there is nothing useful left to decide.
 *
 * <p>Implementations must be thread safe. One instance serves every publisher on the connection,
 * on whatever thread is publishing.
 */
@FunctionalInterface
public interface PublishInterceptor {

    /**
     * Called before the payload is encoded, with whatever the previous interceptor returned.
     *
     * @param context the message as it stands
     * @return the context to carry on with; return the argument unchanged to leave it alone
     * @throws RuntimeException to refuse the publish
     */
    PublishContext beforePublish(PublishContext context);

    /**
     * Called once the broker has confirmed the message.
     *
     * @param context the context as {@link #beforePublish} left it
     * @param result what the broker said
     */
    default void afterConfirm(PublishContext context, PublishResult result) {
        // Most interceptors only care about the way in.
    }

    /**
     * Called when a publish fails, including when another interceptor refused it.
     *
     * @param context the context as it stood when the failure happened
     * @param failure what went wrong
     */
    default void onError(PublishContext context, Throwable failure) {
        // Most interceptors have nothing to add to a failure.
    }

    /**
     * @return where this interceptor sits; lower runs first, and equal orders run in the order
     *     they were registered. Matters whenever one interceptor reads what another wrote
     */
    default int order() {
        return 0;
    }
}
