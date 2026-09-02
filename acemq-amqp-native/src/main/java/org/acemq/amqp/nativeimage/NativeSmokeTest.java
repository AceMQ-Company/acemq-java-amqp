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
package org.acemq.amqp.nativeimage;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.acemq.amqp.api.Codec;
import org.acemq.amqp.api.SchemaDefinition;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.crypto.EncryptedCodec;
import org.acemq.amqp.crypto.Keyring;
import org.acemq.amqp.crypto.Keys;
import org.acemq.amqp.patterns.JdbcSchemaRegistry;
import org.acemq.amqp.transport.QueueType;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Exercises, in a native image, every part of the library that ahead-of-time compilation is
 * known to break elsewhere.
 *
 * <p>Each check is here because it is a thing native images take away, not because it is
 * interesting on its own:
 *
 * <ul>
 *   <li><strong>Codec discovery</strong> — {@code ServiceLoader} over {@code META-INF/services}.
 *       Nothing calls these constructors by name, so a closed-world compiler has every reason to
 *       drop them.
 *   <li><strong>Jackson</strong> — reflection over a type's accessors, with no call site naming
 *       them.
 *   <li><strong>The in-memory transport</strong> — threads and scheduled expiry, started from a
 *       static initialiser.
 *   <li><strong>Encryption</strong> — {@code SecureRandom} and the AES-GCM cipher, both looked
 *       up by string through the JCA.
 *   <li><strong>The schema registry</strong> — a {@code .sql} file read out of a jar, which is a
 *       resource nothing references by name, and a JDBC driver found by service lookup.
 *   <li><strong>TLS</strong> — providers, PKCS12 reading, and the trust manager factory.
 * </ul>
 *
 * <p>Deliberately no broker. Every check here runs anywhere in about a second, so it can be a
 * gate rather than a thing somebody remembers to run; the RabbitMQ transport over a real socket
 * is covered by the integration suite on the JVM, and the transport does nothing in a native
 * image that the in-memory one does not exercise here.
 *
 * <p>It exits non-zero on the first failure. A native image that starts and does nothing is the
 * usual way this kind of test passes while proving nothing.
 */
public final class NativeSmokeTest {

    /** Java 11 target across the library, so a plain class rather than a record. */
    public static final class Order {

        private String id;
        private double amount;

        public Order() {
        }

        Order(String id, double amount) {
            this.id = id;
            this.amount = amount;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public double getAmount() {
            return amount;
        }

        public void setAmount(double amount) {
            this.amount = amount;
        }
    }

    private NativeSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        check("codec discovery by name", () -> {
            require(Codecs.byName("json") != null, "the JSON codec was not found");
            // A second module, so this proves discovery rather than one hard-wired default.
            require(Codecs.byName("yaml") != null, "the YAML codec was not found");
        });

        Codec json = Codecs.byName("json");
        check("Jackson over an application type", () -> {
            byte[] encoded = json.encode(new Order("o-1", 42.0));
            require(new String(encoded).contains("\"id\":\"o-1\""), "unexpected JSON: " + new String(encoded));
            require("o-1".equals(json.decode(encoded, Order.class).getId()), "decode lost the id");
        });

        check("the in-memory transport, end to end", () -> {
            CountDownLatch delivered = new CountDownLatch(1);
            try (AceMq mq = AceMq.connect("memory://native-smoke", Telemetry.NONE)) {
                mq.declareExchange("orders", "topic");
                mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
                mq.bind("orders.new", "orders", "order.*");
                mq.consume("orders.new", Order.class, message -> delivered.countDown());
                mq.publisher("orders", "order.placed", Order.class).send(new Order("o-2", 7.0));
                require(delivered.await(20, TimeUnit.SECONDS), "the message was never delivered");
            }
        });

        check("payload encryption", () -> {
            Codec encrypting = EncryptedCodec.wrapping(json, Keyring.of("smoke-key", Keys.generate()));
            byte[] cipher = encrypting.encode(new Order("o-3", 3.0));
            require("smoke-key".equals(EncryptedCodec.keyIdOf(cipher)), "the key id did not survive");
            require("o-3".equals(encrypting.decode(cipher, Order.class).getId()), "decryption lost the id");
        });

        check("the JDBC schema registry, including its .sql resource", () -> {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:native-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            dataSource.setUser("sa");
            JdbcSchemaRegistry registry = new JdbcSchemaRegistry(dataSource);
            // Reads schema-registry-schema.sql out of the jar. A resource nothing names is
            // exactly what a closed-world build drops.
            registry.createSchemaIfAbsent();
            int id = registry.idFor(new SchemaDefinition("avro", "Order", "{\"type\":\"record\"}"));
            require(id == 1, "expected the first schema to be id 1, got " + id);
            require("Order".equals(registry.schemaFor(id).subject()), "the schema did not come back");
        });

        check("TLS", () -> {
            require(SSLContext.getInstance("TLSv1.3") != null, "no TLSv1.3");
            require(
                    SSLContext.getDefault().getSupportedSSLParameters().getProtocols().length > 0,
                    "the default SSL context supports no protocols, so TLS is not in this image");

            String keystore = System.getProperty("acemq.smoke.keystore");
            if (keystore == null) {
                // The providers are the part that goes missing; reading a store is checked when
                // one is supplied rather than by generating a key pair on every run.
                return;
            }
            KeyStore store = KeyStore.getInstance("PKCS12");
            try (FileInputStream in = new FileInputStream(keystore)) {
                store.load(in, "changeit".toCharArray());
            }
            KeyManagerFactory keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(store, "changeit".toCharArray());
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys.getKeyManagers(), trust.getTrustManagers(), null);
            require(context.getSocketFactory() != null, "the configured SSL context has no socket factory");
        });

        System.out.println("native smoke test: every check passed");
    }

    private static void check(String what, Check body) {
        try {
            body.run();
            System.out.println("  ok    " + what);
        } catch (Throwable e) {
            System.out.println("  FAIL  " + what);
            e.printStackTrace(System.out);
            // Non-zero, so this is a gate rather than a report nobody reads.
            System.exit(1);
        }
    }

    private static void require(boolean condition, String failure) {
        if (!condition) {
            throw new IllegalStateException(failure);
        }
    }

    @FunctionalInterface
    private interface Check {
        void run() throws Exception;
    }
}
