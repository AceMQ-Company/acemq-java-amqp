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
 * Raised when a publish did not result in a message the broker took responsibility for and
 * could route.
 *
 * <p>Unroutable publishes raise this too. Silently dropping a message because no queue was
 * bound is the failure mode this library exists to prevent, so it is an error unless the
 * caller opts out.
 */
public class PublishFailedException extends AceMqException {

    private static final long serialVersionUID = 1L;

    private final boolean unroutable;

    public PublishFailedException(String message) {
        this(message, false);
    }

    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
        this.unroutable = false;
    }

    /**
     * @param message what happened
     * @param unroutable {@code true} when the broker returned the message because nothing was
     *     bound to receive it, rather than refusing or failing to confirm it
     */
    public PublishFailedException(String message, boolean unroutable) {
        super(message);
        this.unroutable = unroutable;
    }

    /**
     * Whether the broker took the message and returned it as unroutable.
     *
     * <p>{@code false} for a {@code basic.nack}, a publish that was never confirmed and one that
     * could not be written: the broker would not keep the message, or it may not have reached
     * it. The two need different fixes — a missing binding against a broker in trouble — so the
     * difference is a field rather than wording to match.
     *
     * @return {@code true} when nothing was bound to receive the message
     */
    public boolean isUnroutable() {
        return unroutable;
    }
}
