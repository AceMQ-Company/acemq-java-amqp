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

    public PublishFailedException(String message) {
        super(message);
    }

    public PublishFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
