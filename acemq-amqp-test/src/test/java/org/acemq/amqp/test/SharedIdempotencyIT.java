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
package org.acemq.amqp.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.JdbcIdempotencyStore;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The shared idempotency store, against a real database and a real broker.
 *
 * <p>H2 proves the logic; it cannot prove the guarantee. Whether exactly one of eight simultaneous
 * claims wins is a property of a particular engine's row locking, and H2 agreeing would be
 * evidence about H2. The same goes for how a duplicate key arrives: this store classifies failures
 * by SQLState rather than vendor error code precisely so it needs no dialect table, and that claim
 * is only worth something once a second database has agreed.
 *
 * <p>The end-to-end test is the one that matters to a reader, though. Two consumers on one queue,
 * one store between them, the same message delivered twice — and the handler runs once. That is
 * the situation an in-process store cannot help with, because the redelivery lands on a different
 * instance from the original.
 */
@DisplayName("the shared idempotency store, against PostgreSQL and RabbitMQ")
class SharedIdempotencyIT {

    private static PostgreSQLContainer<?> postgres;
    private static RabbitMQContainer rabbit;
    private static PGSimpleDataSource dataSource;

    private final List<AutoCloseable> opened = new ArrayList<>();

    @BeforeAll
    static void startInfrastructure() {
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:17-alpine"));
        rabbit = new RabbitMQContainer(DockerImageName.parse("rabbitmq:4-management"));
        postgres.start();
        rabbit.start();

        dataSource = new PGSimpleDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUser(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
    }

    @AfterAll
    static void stopInfrastructure() {
        if (rabbit != null) {
            rabbit.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
    }

    @BeforeEach
    void emptyTheTable() throws SQLException {
        newStore(Duration.ofHours(1), Duration.ofMinutes(5)).createSchemaIfAbsent();
        try (Connection connection = dataSource.getConnection();
                java.sql.Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM acemq_idempotency");
        }
    }

    @AfterEach
    void closeWhatWasOpened() throws Exception {
        for (AutoCloseable closeable : opened) {
            closeable.close();
        }
        opened.clear();
    }

    private static JdbcIdempotencyStore newStore(Duration retention, Duration claimTimeout) {
        return new JdbcIdempotencyStore(dataSource, retention, claimTimeout, "acemq_idempotency");
    }

    @Test
    @Timeout(120)
    @DisplayName("two consumers on one queue handle a duplicate once between them")
    void twoConsumersHandleADuplicateOnce() throws Exception {
        String queue = "orders.shared";
        // A store instance each, over one table. Two consumers sharing a single object would
        // prove nothing that a synchronized map could not: the point is that the coordination
        // happens in the database, where a second process can see it.
        JdbcIdempotencyStore firstStore = newStore(Duration.ofHours(1), Duration.ofMinutes(5));
        JdbcIdempotencyStore secondStore = newStore(Duration.ofHours(1), Duration.ofMinutes(5));

        List<String> handledBy = new CopyOnWriteArrayList<>();
        AceMq first = connectBroker(queue);
        AceMq second = connectBroker(queue);

        opened.add(consume(first, queue, firstStore, "first", handledBy));
        opened.add(consume(second, queue, secondStore, "second", handledBy));

        // One identifier, published twice: what a redelivery looks like from the broker's side,
        // and what a retried publish looks like from the application's.
        Envelope envelope = Envelope.of("order.placed").id("order-42").build();
        first.publisher("orders", "order.placed", String.class).send("{\"id\":42}", envelope);
        first.publisher("orders", "order.placed", String.class).send("{\"id\":42}", envelope);

        await().atMost(Duration.ofSeconds(30)).until(() -> !handledBy.isEmpty());
        // Waiting past the first delivery on purpose: the assertion is about the second one never
        // being handled, and an absence needs time to fail to appear.
        Thread.sleep(2_000);

        assertThat(handledBy)
                .as("the same identifier must be handled once across both consumers, whichever one gets it")
                .hasSize(1);
        assertThat(firstStore.isConfirmed("order-42")).isTrue();
    }

    @Test
    @Timeout(120)
    @DisplayName("exactly one of eight simultaneous claims wins, on a real engine")
    void oneWinnerUnderRealContention() throws Exception {
        int contenders = 8;
        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        try {
            CyclicBarrier startTogether = new CyclicBarrier(contenders);
            List<Callable<Boolean>> attempts = new ArrayList<>();
            for (int i = 0; i < contenders; i++) {
                JdbcIdempotencyStore contender = newStore(Duration.ofHours(1), Duration.ofMinutes(5));
                attempts.add(() -> {
                    startTogether.await(30, TimeUnit.SECONDS);
                    return contender.claim("contested");
                });
            }

            AtomicInteger winners = new AtomicInteger();
            for (Future<Boolean> outcome : pool.invokeAll(attempts)) {
                if (outcome.get()) {
                    winners.incrementAndGet();
                }
            }

            assertThat(winners.get())
                    .as("the primary key is what makes a claim exclusive; this is where that is actually proven")
                    .isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @Timeout(120)
    @DisplayName("a claim left behind by a dead consumer is taken over, not held forever")
    void anAbandonedClaimIsTakenOver() throws Exception {
        JdbcIdempotencyStore died = newStore(Duration.ofHours(1), Duration.ofSeconds(1));
        assertThat(died.claim("order-abandoned")).isTrue();

        JdbcIdempotencyStore survivor = newStore(Duration.ofHours(1), Duration.ofSeconds(1));
        assertThat(survivor.claim("order-abandoned"))
                .as("while the lease runs, the message belongs to whoever holds it")
                .isFalse();

        Thread.sleep(1_200);

        assertThat(survivor.claim("order-abandoned"))
                .as("without a lease, one crash would discard every future redelivery of this message")
                .isTrue();
    }

    @Test
    @Timeout(120)
    @DisplayName("purging removes expired rows and leaves live claims alone")
    void purgingKeepsLiveClaims() throws Exception {
        JdbcIdempotencyStore brief = newStore(Duration.ofSeconds(1), Duration.ofMinutes(5));
        brief.claim("order-old");
        brief.confirm("order-old");
        brief.claim("order-live");

        Thread.sleep(1_200);

        assertThat(brief.purgeExpired()).isEqualTo(1);
        assertThat(brief.size()).isEqualTo(1L);
        assertThat(brief.claim("order-live"))
                .as("the surviving claim must still bind, or its message would be handled twice")
                .isFalse();
    }

    private AceMq connectBroker(String queue) {
        AceMq mq = AceMq.connect(rabbit.getAmqpUrl(), Telemetry.NONE);
        mq.declareExchange("orders", "topic");
        mq.declareQueue(queue, QueueType.CLASSIC, Collections.emptyMap());
        mq.bind(queue, "orders", "order.*");
        opened.add(mq);
        return mq;
    }

    private static MessageConsumer consume(
            AceMq mq, String queue, JdbcIdempotencyStore store, String name, List<String> handledBy) {
        Set<String> seen = ConcurrentHashMap.newKeySet();
        return mq.consume(queue, String.class, ConsumerOptions.prefetch(1).idempotent(store), message -> {
            seen.add(message.envelope().id());
            handledBy.add(name + ":" + message.envelope().id());
        });
    }
}
