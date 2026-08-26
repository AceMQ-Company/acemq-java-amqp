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
    private final boolean publisherConfirms;

    private ConnectionConfig(Builder builder) {
        this.url = Objects.requireNonNull(builder.url, "url must not be null");
        this.username = builder.username;
        this.password = builder.password;
        this.virtualHost = builder.virtualHost;
        this.clientName = builder.clientName;
        this.connectionTimeout = builder.connectionTimeout;
        this.confirmTimeout = builder.confirmTimeout;
        this.publisherConfirms = builder.publisherConfirms;
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

    /** @return whether publishes wait for broker confirmation; on by default */
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
        private boolean publisherConfirms = true;

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
         * Turns publisher confirms off.
         *
         * <p>Named rather than boolean-flagged, because losing messages silently should
         * require saying so out loud.
         */
        public Builder withoutPublisherConfirms() {
            this.publisherConfirms = false;
            return this;
        }

        public ConnectionConfig build() {
            return new ConnectionConfig(this);
        }
    }
}
