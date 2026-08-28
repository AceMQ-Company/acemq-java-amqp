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
package org.acemq.amqp.security;

/**
 * Something about the security configuration is wrong.
 *
 * <p>Always thrown before a connection is attempted rather than after it fails, so the message
 * says what to fix instead of reporting a handshake error twelve frames deep.
 */
public class SecurityConfigurationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** @param message what is wrong and what to do about it */
    public SecurityConfigurationException(String message) {
        super(message);
    }

    /**
     * @param message what is wrong and what to do about it
     * @param cause the underlying failure
     */
    public SecurityConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
