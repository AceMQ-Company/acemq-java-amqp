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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.transport.Transport;

/** Finds the transport that handles a broker URL. */
final class Transports {

    private Transports() {
        throw new AssertionError("Transports is a utility and must not be instantiated");
    }

    /**
     * @param scheme URL scheme, for example {@code amqp}
     * @return the transport registered for it
     * @throws AceMqException if none is on the classpath, naming what was found so the fix is
     *     obvious: the message tells you which dependency to add
     */
    static Transport forScheme(String scheme) {
        List<Transport> available = new ArrayList<>();
        for (Transport transport : ServiceLoader.load(Transport.class)) {
            if (transport.schemes().stream().anyMatch(known -> known.equalsIgnoreCase(scheme))) {
                return transport;
            }
            available.add(transport);
        }
        String found = available.isEmpty()
                ? "none"
                : available.stream().flatMap(t -> t.schemes().stream()).collect(Collectors.joining(", "));
        throw new AceMqException("no transport handles the scheme '" + scheme + "'. Schemes available on the"
                + " classpath: " + found + ". Add the module for the broker you are connecting to, for example"
                + " org.acemq:acemq-transport-rabbitmq for amqp:// URLs.");
    }
}
