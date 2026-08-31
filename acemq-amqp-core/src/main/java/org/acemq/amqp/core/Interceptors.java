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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.acemq.amqp.api.Ack;
import org.acemq.amqp.api.ConsumeContext;
import org.acemq.amqp.api.ConsumeInterceptor;
import org.acemq.amqp.api.PublishContext;
import org.acemq.amqp.api.PublishInterceptor;
import org.acemq.amqp.api.PublishResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The interceptors registered on one connection, and the rules for running them.
 *
 * <p>Held by {@link AceMq} and read at publish and handle time rather than copied into each
 * publisher, so an interceptor registered during start-up applies to publishers that already
 * exist. Registration is expected at start-up all the same: swapping the snapshot makes a late
 * addition safe rather than sensible, and a message already in flight will not see it.
 */
final class Interceptors {

    private static final Logger log = LoggerFactory.getLogger(Interceptors.class);

    /**
     * Immutable snapshots, replaced whole on registration.
     *
     * <p>Not a copy-on-write list, because these have to be kept sorted and sorting one in place
     * means clearing it first — during which a publisher on another thread would see no
     * interceptors at all and send a message that should have been stamped or refused. Swapping a
     * reference is atomic; emptying and refilling a list is not.
     */
    private volatile List<PublishInterceptor> publishing = Collections.emptyList();

    private volatile List<ConsumeInterceptor> consuming = Collections.emptyList();

    synchronized void add(PublishInterceptor interceptor) {
        publishing = sorted(publishing, interceptor, PublishInterceptor::order);
    }

    synchronized void add(ConsumeInterceptor interceptor) {
        consuming = sorted(consuming, interceptor, ConsumeInterceptor::order);
    }

    boolean isEmptyForPublishing() {
        return publishing.isEmpty();
    }

    boolean isEmptyForConsuming() {
        return consuming.isEmpty();
    }

    /**
     * Runs every interceptor's {@code beforePublish}, threading the context through them.
     *
     * <p>An exception is allowed straight out. Refusing a publish is the whole reason a policy
     * interceptor exists, and swallowing it here would turn "this message is not allowed" into
     * "this message was sent".
     */
    PublishContext beforePublish(PublishContext context) {
        PublishContext current = context;
        for (PublishInterceptor interceptor : publishing) {
            PublishContext returned = interceptor.beforePublish(current);
            if (returned == null) {
                throw new NullPointerException(interceptor.getClass().getName()
                        + ".beforePublish returned null; return the context unchanged to leave it alone");
            }
            current = returned;
        }
        return current;
    }

    void afterConfirm(PublishContext context, PublishResult result) {
        for (PublishInterceptor interceptor : publishing) {
            try {
                interceptor.afterConfirm(context, result);
            } catch (RuntimeException e) {
                // The message is already at the broker. There is nothing left to decide, and
                // throwing here would report a successful publish as a failed one.
                log.warn("a publish interceptor failed after the confirm; the message was still sent", e);
            }
        }
    }

    void onPublishError(PublishContext context, Throwable failure) {
        for (PublishInterceptor interceptor : publishing) {
            try {
                interceptor.onError(context, failure);
            } catch (RuntimeException e) {
                // Never allowed to replace the original failure: the caller needs to see what
                // actually went wrong, not what an observer did about it.
                log.warn("a publish interceptor failed while handling an error", e);
            }
        }
    }

    /**
     * Runs every interceptor's {@code beforeHandle}, in order.
     *
     * <p>An exception is allowed out, and the caller treats it as a handler failure. That is the
     * honest outcome for a refused message: it is retried and eventually dead-lettered, rather
     * than acknowledged as though something had processed it.
     */
    void beforeHandle(ConsumeContext context) {
        for (ConsumeInterceptor interceptor : consuming) {
            interceptor.beforeHandle(context);
        }
    }

    void onConsumeError(ConsumeContext context, Throwable failure) {
        for (ConsumeInterceptor interceptor : consuming) {
            try {
                interceptor.onError(context, failure);
            } catch (RuntimeException e) {
                log.warn("a consume interceptor failed while handling an error", e);
            }
        }
    }

    /**
     * Runs every interceptor's {@code afterHandle}, in reverse order.
     *
     * <p>Reversed so a pair that sets something up on the way in and tears it down on the way out
     * nests properly: the first interceptor to open a scope is the last to close it. Running both
     * halves in the same order would have the outermost scope closing first, while the inner one
     * was still using it.
     */
    void afterHandle(ConsumeContext context, Ack ack) {
        for (int i = consuming.size() - 1; i >= 0; i--) {
            try {
                consuming.get(i).afterHandle(context, ack);
            } catch (RuntimeException e) {
                // The delivery is settled either way; an exception here cannot un-settle it, and
                // letting it out would skip the remaining teardowns.
                log.warn("a consume interceptor failed after the handler; the delivery was still settled", e);
            }
        }
    }

    private static <T> List<T> sorted(
            List<T> existing, T addition, java.util.function.ToIntFunction<T> order) {
        // Sorted once per registration rather than once per message. The sort is stable, so
        // interceptors with equal orders stay in registration order -- which is what makes
        // "register these two in this sequence" mean anything.
        List<T> combined = new ArrayList<>(existing);
        combined.add(java.util.Objects.requireNonNull(addition, "interceptor"));
        combined.sort(Comparator.comparingInt(order));
        return Collections.unmodifiableList(combined);
    }
}
