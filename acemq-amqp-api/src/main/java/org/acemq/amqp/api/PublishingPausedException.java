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
 * Publishing was paused on this connection, so the message was not sent.
 *
 * <p>Its own type rather than a generic failure, because a caller wants to tell this apart from
 * a broker rejecting a message. This one means "not now" — the message is fine, the connection is
 * fine, and somebody is in the middle of a cutover. Retrying in a moment is the right response,
 * where retrying a rejected message usually is not.
 *
 * <p>Nothing was sent. There is no half-published message to reason about.
 */
public class PublishingPausedException extends AceMqException {

    private static final long serialVersionUID = 1L;

    /** @param message what was paused and what to do about it */
    public PublishingPausedException(String message) {
        super(message);
    }
}
