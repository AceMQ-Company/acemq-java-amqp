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

import org.acemq.amqp.api.AceMqException;

/**
 * No reply arrived in time.
 *
 * <p>Its own type because the answer is different from every other failure here. A timeout does
 * <strong>not</strong> mean the request was not handled: the message may be sitting in a queue,
 * being worked on right now, or already done with the reply lost on the way back. Retrying is
 * therefore a decision about idempotency, not a reflex — and where the work is not idempotent,
 * a timeout is a question for a human rather than for a retry loop.
 */
public class RequestTimedOutException extends AceMqException {

    private static final long serialVersionUID = 1L;

    private final transient Duration waited;

    public RequestTimedOutException(String message, Duration waited) {
        super(message);
        this.waited = waited;
    }

    /** @return how long the caller waited before giving up */
    public Duration waited() {
        return waited;
    }
}
