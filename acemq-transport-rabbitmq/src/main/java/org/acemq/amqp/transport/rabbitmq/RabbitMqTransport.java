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
package org.acemq.amqp.transport.rabbitmq;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Capability;
import org.acemq.amqp.transport.ConnectionConfig;
import org.acemq.amqp.transport.Transport;
import org.acemq.amqp.transport.TransportConnection;
import org.acemq.amqp.transport.TransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ConnectionFactory;

/**
 * AMQP 0-9-1 transport backed by the RabbitMQ Java client.
 *
 * <p>Discovered through {@link java.util.ServiceLoader}, so putting this module on the
 * classpath is all it takes for {@code amqp://} URLs to work.
 */
public final class RabbitMqTransport implements Transport {

    private static final Logger log = LoggerFactory.getLogger(RabbitMqTransport.class);

    /**
     * What a RabbitMQ broker can do.
     *
     * <p>Two absences are deliberate. {@code DELAYED_DELIVERY} is missing because it needs a
     * plugin that may not be installed, so the core generates a time-to-live and dead-letter
     * ladder instead rather than assuming. {@code CONSISTENT_HASH_ROUTING} is missing for the
     * same reason.
     */
    // TRANSACTIONS is deliberately absent. RabbitMQ has tx.select, and this library offers no
    // way to reach it: a capability is a promise that the library can do the thing, not that
    // the broker could if someone wrote the code. Publisher confirms cover what almost every
    // caller wants transactions for, at a fraction of the cost, and the transactional outbox
    // covers the rest. Claiming it made mq.supports(TRANSACTIONS) return true and left the
    // caller with nothing to call.
    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(EnumSet.of(
            Capability.EXCHANGE_ROUTING,
            Capability.TOPIC_WILDCARDS,
            Capability.HEADERS_ROUTING,
            Capability.PUBLISHER_CONFIRMS,
            Capability.DEAD_LETTER_NATIVE,
            Capability.TTL_PER_MESSAGE,
            Capability.PRIORITY,
            Capability.QUORUM_QUEUES,
            Capability.STREAMS,
            Capability.SINGLE_ACTIVE_CONSUMER));

    @Override
    public String scheme() {
        return "amqp";
    }

    @Override
    public java.util.Set<String> schemes() {
        return java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(java.util.Arrays.asList(
                "amqp", "amqps")));
    }

    @Override
    public String name() {
        return "rabbitmq";
    }

    @Override
    public Set<Capability> capabilities() {
        return CAPABILITIES;
    }

    /**
     * Applies the connection's security policy.
     *
     * <p>Two separate questions, and conflating them was a real bug: <strong>the URL decides
     * whether the connection is encrypted, and the policy decides how strictly the other end is
     * verified.</strong> A default policy that turned every {@code amqp://} URL into a TLS
     * handshake would break every plaintext connection in the world, which is exactly what it
     * did until this was split.
     *
     * <p>So {@code amqps://} encrypts and needs nothing configured, {@code amqp://} stays
     * plaintext and says so, and asking for both at once is refused rather than guessed at.
     *
     * <p>Note the warning rather than a refusal for plaintext. Refusing would be defensible and
     * would also be the reason somebody sets a flag once and forgets it; one line naming the
     * host, every time, is harder to ignore and impossible to configure away.
     */
    private static void applySecurity(ConnectionFactory factory, ConnectionConfig config) {
        org.acemq.amqp.security.Security security = config.security();
        boolean encrypted = "amqps".equalsIgnoreCase(config.scheme());

        if (!encrypted) {
            if (security.mode() == org.acemq.amqp.security.Security.Mode.DISABLED) {
                return;
            }
            if (!isLoopback(factory.getHost())) {
                log.warn("connecting to {} without TLS. Credentials and message bodies cross the network in the"
                        + " clear. Use amqps:// before this reaches anything shared.", factory.getHost());
            }
            return;
        }

        if (security.mode() == org.acemq.amqp.security.Security.Mode.DISABLED) {
            throw new TransportException("the URL asks for amqps:// and the security policy is disabled."
                    + " One of the two is wrong, and guessing which would either drop the encryption somebody"
                    + " asked for or add one they refused.");
        }

        javax.net.ssl.SSLContext context = security.sslContext().orElseThrow(
                () -> new TransportException("no TLS context for an amqps:// connection"));
        factory.useSslProtocol(context);
        if (security.verifiesHostname()) {
            // Without this, any valid certificate satisfies the connection whoever it was issued
            // to, which reduces TLS to an encrypted conversation with whoever answered.
            factory.enableHostnameVerification();
        } else {
            log.warn("TLS is on but nothing is verified. The connection to {} is encrypted and its other end is"
                    + " unproven.", factory.getHost());
        }
    }

    private static boolean isLoopback(String host) {
        return "localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host) || "::1".equals(host);
    }

    @Override
    public TransportConnection connect(ConnectionConfig config) {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(config.url());
        } catch (URISyntaxException | java.security.NoSuchAlgorithmException | java.security.KeyManagementException e) {
            throw new TransportException("invalid broker URL: " + config.url(), e);
        }

        config.security().credentials().ifPresent(provider -> {
            // Consulted here, on every connect, rather than once at start-up: a token has an
            // expiry, and automatic recovery calls this again when it reconnects.
            org.acemq.amqp.security.Credentials credentials = provider.get();
            factory.setUsername(credentials.username());
            factory.setPassword(new String(credentials.secret()));
        });
        if (config.username() != null) {
            factory.setUsername(config.username());
        }
        if (config.password() != null) {
            factory.setPassword(config.password());
        }
        if (config.virtualHost() != null) {
            factory.setVirtualHost(config.virtualHost());
        }
        factory.setConnectionTimeout((int) config.connectionTimeout().toMillis());
        applySecurity(factory, config);

        // Topology recovery is what makes a reconnect transparent: without it the client
        // reopens the connection but the consumers and bindings are gone.
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(TimeUnit.SECONDS.toMillis(5));

        ExecutorService dispatch = dispatchPool(config.clientName());
        factory.setSharedExecutor(dispatch);

        try {
            return new RabbitMqConnection(factory.newConnection(config.clientName()), config, dispatch);
        } catch (Exception e) {
            dispatch.shutdownNow();
            throw new TransportException("could not connect to " + config.url(), e);
        }
    }

    /**
     * The pool every handler on this connection runs on.
     *
     * <p>Supplied rather than left to the client, and that is a correctness fix rather than
     * tuning. Left alone, the RabbitMQ client dispatches consumers on a fixed pool of
     * {@code availableProcessors()} threads, shared by every channel on the connection — so the
     * number of handlers that can run at the same time is the size of the machine, whatever
     * concurrency was asked for. On a four-core pod, ten consumers at {@code concurrency(10)}
     * ran four at a time and the other six waited, silently, with no configuration anywhere
     * saying so.
     *
     * <p>That cap is wrong for what this library says concurrency is for: handlers that spend
     * their time waiting on a database or an HTTP call, where the right number is the number
     * the caller asked for and has nothing to do with cores. So the pool grows on demand and
     * lets idle threads go after a minute.
     *
     * <p>Unbounded in form only. The client dispatches at most one delivery per channel at a
     * time and each consumer holds its own channel, so the thread count cannot exceed the number
     * of consumers the application itself created.
     */
    private static ExecutorService dispatchPool(String clientName) {
        AtomicInteger counter = new AtomicInteger();
        return new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                // Not daemon threads, matching what the client would have created: a handler
                // part-way through a message should not be abandoned by an exiting JVM. Nothing
                // is pinned by this either way -- the core size is zero, so every thread here is
                // reclaimed once it has been idle for a minute.
                runnable -> new Thread(runnable, "acemq-consumer-" + clientName + "-" + counter.incrementAndGet()));
    }
}
