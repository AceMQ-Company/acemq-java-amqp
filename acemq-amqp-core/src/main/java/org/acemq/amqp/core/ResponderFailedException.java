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

import org.acemq.amqp.api.AceMqException;

/**
 * The responder answered, and the answer was a failure.
 *
 * <p>The round trip completed: the request was handled, the handler threw, and the responder sent
 * that back in the {@code acemq-error} header rather than leaving the caller to wait out its
 * timeout. Python raises {@code ResponderError}, Ruby {@code ResponderFailed} and Go
 * {@code ErrResponderFailed} for the same reply, whichever language the responder was written in.
 */
public class ResponderFailedException extends AceMqException {

    private static final long serialVersionUID = 1L;

    private final String failure;

    public ResponderFailedException(String message, String failure) {
        super(message);
        this.failure = failure;
    }

    /** @return the responder's failure as it reported it, for example {@code IllegalStateException: out of stock} */
    public String failure() {
        return failure;
    }
}
