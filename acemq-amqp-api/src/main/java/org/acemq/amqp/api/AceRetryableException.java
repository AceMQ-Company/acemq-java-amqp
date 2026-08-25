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
 * Signals a failure worth retrying: the message is sound but the attempt did not
 * succeed, typically because a dependency was briefly unavailable.
 *
 * <p>Throwing this from a handler sends the delivery to the next retry tier with its
 * attempt counter incremented. It never blocks the consumer thread.
 */
public class AceRetryableException extends AceMqException {

    private static final long serialVersionUID = 1L;

    public AceRetryableException(String message) {
        super(message);
    }

    public AceRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
