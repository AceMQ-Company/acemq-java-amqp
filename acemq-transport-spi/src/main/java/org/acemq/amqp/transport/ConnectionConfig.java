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
package org.acemq.amqp.transport;

import java.time.Duration;
import java.util.Objects;

import org.acemq.amqp.security.Security;
import org.jspecify.annotations.Nullable;

/** Everything a transport needs in order to open a connection. */
public final class ConnectionConfig {

    private final String url;
    private final @Nullable String username;
    private final @Nullable String password;
    private final @Nullable String virtualHost;
    private final String clientName;
    private final Duration connectionTimeout;
    private final Duration confirmTimeout;
    private final Duration blockedTimeout;
    private final boolean publisherConfirms;
    private final int maxOutstandingPublishes;
    private final Security security;

    private ConnectionConfig(Builder builder) {
        this.url = Objects.requireNonNull(builder.url, "url must not be null");
        this.username = builder.username;
        this.password = builder.password;
        this.virtualHost = builder.virtualHost;
        this.clientName = builder.clientName;
        this.connectionTimeout = builder.connectionTimeout;
        this.confirmTimeout = builder.confirmTimeout;
        this.blockedTimeout = builder.blockedTimeout;
        this.publisherConfirms = builder.publisherConfirms;
        this.maxOutstandingPublishes = builder.maxOutstandingPublishes;
        this.security = builder.security;
    }

    /**
     * @param url broker URL whose scheme selects the transport, for example
     *     {@code amqp://localhost:5672}
     * @return a builder with safe defaults already applied
     */
    public static Builder url(String url) {
        return new Builder().url(url);
    }

    public String url() {
        return url;
    }

    public @Nullable String username() {
        return username;
    }

    public @Nullable String password() {
        return password;
    }

    public @Nullable String virtualHost() {
        return virtualHost;
    }

    public String clientName() {
        return clientName;
    }

    public Duration connectionTimeout() {
        return connectionTimeout;
    }

    /** @return how long a publish waits for the broker to confirm it */
    public Duration confirmTimeout() {
        return confirmTimeout;
    }

    /**
     * @return how long a publish waits for a blocked broker to resume before giving up.
     *     Waiting rather than failing at once is deliberate: a memory alarm is usually brief,
     *     and turning every one of them into an immediate application error would replace a
     *     pause with an outage. Waiting forever, which is what happens without this, is worse
     *     than both
     */
    public Duration blockedTimeout() {
        return blockedTimeout;
    }

    /** @return whether publishes wait for broker confirmation; on by default */
    /**
     * @return how this connection is protected. Never null: a connection always has a policy,
     *     and the default is the safe one
     */
    public Security security() {
        return security;
    }

    /**
     * How many publishes may be waiting for a confirm at once.
     *
     * <p>Only asynchronous publishing uses this, and it is the setting that keeps it honest: a
     * publisher that hands out futures faster than the broker confirms them accumulates
     * unconfirmed messages in memory until the process dies. Once the limit is reached, publishing
     * blocks — turning a memory leak into backpressure, which is the trade a queueing library
     * should make.
     *
     * @return the ceiling on unconfirmed publishes
     */
    public int maxOutstandingPublishes() {
        return maxOutstandingPublishes;
    }

    public boolean publisherConfirms() {
        return publisherConfirms;
    }

    /** @return the scheme of {@link #url()}, used to select a transport */
    public String scheme() {
        int end = url.indexOf("://");
        return end < 0 ? url : url.substring(0, end);
    }

    @Override
    public String toString() {
        // Credentials are never rendered: this string reaches logs and health endpoints.
        return "ConnectionConfig{url=" + url + ", virtualHost=" + virtualHost + ", confirms="
                + publisherConfirms + "}";
    }

    /** Builds {@link ConnectionConfig} instances. */
    public static final class Builder {

        private @Nullable String url;
        private @Nullable String username;
        private @Nullable String password;
        private @Nullable String virtualHost;
        private String clientName = "acemq";
        private Duration connectionTimeout = Duration.ofSeconds(10);
        private Duration confirmTimeout = Duration.ofSeconds(10);
        private Duration blockedTimeout = Duration.ofSeconds(30);
        private boolean publisherConfirms = true;

        /**
         * Enough to keep the broker busy, small enough that the memory cost is bounded and
         * obvious. A publisher that wants more should say so rather than inherit it.
         */
        private int maxOutstandingPublishes = 1_000;
        // Secure by default. An amqps:// URL then needs nothing said about it, and a plaintext
        // one to anywhere but this machine is warned about rather than silently accepted.
        private Security security = Security.required();

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder credentials(@Nullable String username, @Nullable String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public Builder virtualHost(@Nullable String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        /** @param clientName name the broker shows for this connection, aiding operators */
        public Builder clientName(String clientName) {
            this.clientName = clientName;
            return this;
        }

        public Builder connectionTimeout(Duration connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder confirmTimeout(Duration confirmTimeout) {
            this.confirmTimeout = confirmTimeout;
            return this;
        }

        /**
         * @param blockedTimeout how long a publish waits for a blocked broker to resume
         * @return this builder
         */
        public Builder blockedTimeout(Duration blockedTimeout) {
            this.blockedTimeout = java.util.Objects.requireNonNull(blockedTimeout, "blockedTimeout");
            return this;
        }

        /**
         * Turns publisher confirms off.
         *
         * <p>Named rather than boolean-flagged, because losing messages silently should
         * require saying so out loud.
         */
        /**
         * @param security how the connection is protected
         * @return this builder
         */
        public Builder security(Security security) {
            this.security = java.util.Objects.requireNonNull(security, "security");
            return this;
        }

        public Builder withoutPublisherConfirms() {
            this.publisherConfirms = false;
            return this;
        }

        /**
         * @param maxOutstandingPublishes how many asynchronous publishes may await a confirm
         *     before publishing blocks; must be at least 1
         */
        public Builder maxOutstandingPublishes(int maxOutstandingPublishes) {
            if (maxOutstandingPublishes < 1) {
                throw new IllegalArgumentException(
                        "maxOutstandingPublishes must be at least 1, was " + maxOutstandingPublishes);
            }
            this.maxOutstandingPublishes = maxOutstandingPublishes;
            return this;
        }

        public ConnectionConfig build() {
            return new ConnectionConfig(this);
        }
    }
}
