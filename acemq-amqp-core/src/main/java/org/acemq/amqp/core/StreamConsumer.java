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

import java.util.OptionalLong;

/**
 * A running read of a stream.
 *
 * <p>Separate from {@link MessageConsumer} on purpose, because a stream is not a queue with
 * different settings — it is a different model, and a single type covering both would be a type
 * where half the methods throw.
 *
 * <p>What a stream does not have:
 *
 * <ul>
 *   <li><strong>No dead-letter queue.</strong> The retry ladder does not apply. A stream never
 *       moves a message anywhere, because it never removes one.
 *   <li><strong>No requeue and no selective reject.</strong> There is nothing to put back.
 *   <li><strong>No destructive read.</strong> Acknowledging advances this consumer's position.
 *       Every other consumer still sees the message, and so will this one if it starts again
 *       from an earlier offset.
 * </ul>
 *
 * <p>So a handler that fails has only two honest outcomes, and {@link StreamOptions} makes the
 * choice explicit: stop, or skip and count. There is no third option that quietly works.
 */
public interface StreamConsumer extends AutoCloseable {

    /** @return the stream being read */
    String queue();

    /** @return whether this consumer is still attached */
    boolean isRunning();

    /**
     * @return the offset of the last message handled successfully, or empty if none has been.
     *     This is the number to persist if the consumer must resume where it left off; resume
     *     from {@link org.acemq.amqp.api.StreamOffset#at(long)} with one more than this
     */
    OptionalLong lastHandledOffset();

    /** @return how many messages this consumer has handled successfully */
    long handled();

    /** @return how many handlers have failed */
    long failed();

    /**
     * @return how many messages were passed over after a failure, which is only ever non-zero
     *     when the failure policy is to skip. Data in a stream that nothing processed is worth
     *     alerting on
     */
    long skipped();

    /**
     * @return why this consumer stopped, when it stopped because a handler failed and the policy
     *     was to stop
     */
    java.util.Optional<Throwable> stoppedBy();

    @Override
    void close();
}
