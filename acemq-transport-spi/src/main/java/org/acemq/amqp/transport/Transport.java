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

import java.util.Set;

import org.acemq.amqp.api.Capability;

/**
 * A binding to one messaging protocol.
 *
 * <p>Implementations are discovered with {@link java.util.ServiceLoader} and selected by the
 * scheme of the broker URL, so adding a broker to an application is a matter of putting a jar
 * on the classpath rather than changing code.
 *
 * <p>{@link #capabilities()} is the honest part of the contract. The core consults it before
 * planning topology and either uses a native feature, applies a documented alternative, or
 * fails at startup naming what is missing. A transport that overstates its capabilities turns
 * a clear startup failure into a subtle production one.
 */
public interface Transport {

    /**
     * @return the URL scheme this transport handles, for example {@code amqp} or
     *     {@code amqp10}
     */
    String scheme();

    /** @return a short name for logs and diagnostics, such as {@code rabbitmq} */
    String name();

    /** @return what brokers reachable through this transport can do */
    Set<Capability> capabilities();

    /**
     * Opens a connection.
     *
     * @param config connection settings; {@link ConnectionConfig#scheme()} must match
     *     {@link #scheme()}
     * @return an open connection
     * @throws TransportException if the broker cannot be reached or refuses the credentials
     */
    TransportConnection connect(ConnectionConfig config);
}
