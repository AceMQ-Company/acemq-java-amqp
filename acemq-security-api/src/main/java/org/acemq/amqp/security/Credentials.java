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

import java.util.Objects;

/**
 * A username and a secret.
 *
 * <p>The secret never appears in {@link #toString()}, and there is no getter that reads like
 * one. Credentials reach logs, health endpoints, exception messages and crash dumps by way of
 * some object's {@code toString}, and that is how they end up in a log aggregator that half the
 * company can search.
 */
public final class Credentials {

    private final String username;
    private final char[] secret;

    private Credentials(String username, char[] secret) {
        this.username = Objects.requireNonNull(username, "username");
        this.secret = Objects.requireNonNull(secret, "secret").clone();
    }

    /**
     * @param username the user to connect as
     * @param password its password
     * @return the pair
     */
    public static Credentials of(String username, String password) {
        return new Credentials(username, Objects.requireNonNull(password, "password").toCharArray());
    }

    /**
     * A bearer token, as RabbitMQ's OAuth 2 support expects it.
     *
     * @param token the token, which the broker reads as the password
     * @return credentials carrying it
     */
    public static Credentials token(String token) {
        // The username is ignored by the broker in this mode, but something has to be sent, and
        // a recognisable value is kinder to whoever reads the connection list.
        return new Credentials("oauth2", Objects.requireNonNull(token, "token").toCharArray());
    }

    /** @return the user to connect as */
    public String username() {
        return username;
    }

    /**
     * @return a copy of the secret. A copy, so that a caller clearing its array cannot blank
     *     this one, and so that this one's array never escapes
     */
    public char[] secret() {
        return secret.clone();
    }

    @Override
    public String toString() {
        return "Credentials{username=" + username + ", secret=<redacted>}";
    }
}
