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

import java.time.Duration;
import java.util.Objects;

/** The outcome of one publish, after the broker has confirmed it. */
public final class PublishResult {

    private final String messageId;
    private final boolean routed;
    private final Duration latency;

    public PublishResult(String messageId, boolean routed, Duration latency) {
        this.messageId = Objects.requireNonNull(messageId, "messageId must not be null");
        this.routed = routed;
        this.latency = Objects.requireNonNull(latency, "latency must not be null");
    }

    /** @return the identifier the message was published with */
    public String messageId() {
        return messageId;
    }

    /**
     * @return whether the broker had somewhere to route the message
     * @implNote a confirmed publish that was not routed still means the message is gone. By
     *     default the publisher treats this as a failure, because a message nobody receives is
     *     almost never what was intended.
     */
    public boolean routed() {
        return routed;
    }

    /** @return how long the broker took to confirm */
    public Duration latency() {
        return latency;
    }

    @Override
    public String toString() {
        return "PublishResult{messageId=" + messageId + ", routed=" + routed + ", latency=" + latency + "}";
    }
}
