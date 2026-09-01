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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.api.Telemetry;
import org.acemq.amqp.core.AceMq;
import org.acemq.amqp.core.Codecs;
import org.acemq.amqp.core.ConsumerOptions;
import org.acemq.amqp.core.MessageConsumer;
import org.acemq.amqp.patterns.InMemoryIdempotencyStore;
import org.acemq.amqp.patterns.JdbcOutboxStore;
import org.acemq.amqp.patterns.OutboxRelay;
import org.acemq.amqp.transport.QueueType;
import org.junit.jupiter.api.AfterAll;
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
 * The outbox against the two systems it exists to keep in agreement.
 *
 * <p>The unit suite runs this store against H2 and proves the SQL is right. It cannot prove what
 * matters most here. Whether a claim really is exclusive when two relays go for the same rows is
 * a property of a particular engine's locking, and H2 agreeing would be evidence about H2. So the
 * concurrency is tested against PostgreSQL, and the round trip against PostgreSQL and RabbitMQ
 * together, because a message that survives a commit but never reaches a broker has still been
 * lost.
 */
@DisplayName("the outbox, against a real database and a real broker")
class OutboxIT {

    private static PostgreSQLContainer<?> postgres;
    private static RabbitMQContainer rabbit;
    private static PGSimpleDataSource dataSource;

    private AceMq mq;

    @BeforeAll
    static void startInfrastructure() {
        postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:17-alpine"));
        rabbit = new RabbitMQContainer(BrokerImage.current());
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
    void emptyTheOutbox() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            newStore(connection).createSchemaIfAbsent();
        }
        try (Connection connection = dataSource.getConnection();
                java.sql.Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM acemq_outbox");
        }
    }

    private static JdbcOutboxStore newStore(Connection transactional) {
        return new JdbcOutboxStore(() -> transactional, dataSource);
    }

    private AceMq connectBroker() {
        mq = AceMq.connect(rabbit.getAmqpUrl(), Telemetry.NONE);
        mq.declareExchange("orders", "topic");
        mq.declareQueue("orders.new", QueueType.CLASSIC, Collections.emptyMap());
        mq.bind("orders.new", "orders", "order.*");
        return mq;
    }

    private static OutboxRecord order(String id) {
        return OutboxRecord.of(
                "orders", "order.placed", Envelope.of("order.placed").id(id).build(), "{\"id\":\"" + id + "\"}");
    }

    @Test
    @Timeout(180)
    void a_committed_order_reaches_the_consumer_and_a_rolled_back_one_never_does() throws Exception {
        connectBroker();
        List<String> received = new CopyOnWriteArrayList<>();

        try (Connection transactional = dataSource.getConnection()) {
            transactional.setAutoCommit(false);
            JdbcOutboxStore store = newStore(transactional);

            try (MessageConsumer consumer = mq.consume(
                    "orders.new",
                    String.class,
                    // As text: the relay publishes the payload the transaction committed,
                    // unchanged, and these are plain strings rather than JSON documents.
                    ConsumerOptions.prefetch(10)
                            .as(Codecs.byName("text"))
                            .idempotent(InMemoryIdempotencyStore.forOneDay()),
                    message -> received.add(message.envelope().id()));
                    OutboxRelay relay = new OutboxRelay(mq, store, 50, Duration.ofMillis(100), Duration.ofMinutes(1))) {

                // One order that goes through.
                store.add(order("o-committed"));
                transactional.commit();

                // One that does not. The message for it was written the same way, into the same
                // table, in the same transaction — and dies with it.
                store.add(order("o-rolled-back"));
                transactional.rollback();

                relay.start();

                await().atMost(Duration.ofSeconds(60)).until(() -> received.contains("o-committed"));

                // Give the relay several more passes to publish the rolled-back order if it were
                // ever going to. Asserting only that the good one arrived would pass even with
                // the guarantee broken.
                Thread.sleep(1500);

                assertThat(received).containsExactly("o-committed");
                assertThat(store.pendingCount()).isZero();
                assertThat(consumer.acknowledged()).isEqualTo(1);
            }
        } finally {
            if (mq != null && mq.isOpen()) {
                mq.close();
            }
        }
    }

    @Test
    @Timeout(180)
    void two_relays_never_claim_the_same_record() throws Exception {
        int records = 300;
        int relays = 4;

        try (Connection transactional = dataSource.getConnection()) {
            transactional.setAutoCommit(false);
            JdbcOutboxStore writer = newStore(transactional);
            for (int i = 0; i < records; i++) {
                writer.add(order("o-" + i));
            }
            transactional.commit();
        }

        Set<String> everClaimed = ConcurrentHashMap.newKeySet();
        List<String> claimedTwice = new CopyOnWriteArrayList<>();
        AtomicInteger totalClaimed = new AtomicInteger();
        CountDownLatch startTogether = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(relays);

        try {
            for (int relay = 0; relay < relays; relay++) {
                pool.execute(() -> {
                    try (Connection own = dataSource.getConnection()) {
                        JdbcOutboxStore store = newStore(own);
                        startTogether.await();
                        while (true) {
                            List<OutboxRecord> batch = store.claimBatch(7, Duration.ofMinutes(5));
                            if (batch.isEmpty()) {
                                return;
                            }
                            for (OutboxRecord record : batch) {
                                if (!everClaimed.add(record.id())) {
                                    claimedTwice.add(record.id());
                                }
                                totalClaimed.incrementAndGet();
                                store.markPublished(record.id());
                            }
                        }
                    } catch (Exception e) {
                        claimedTwice.add("failed: " + e);
                    }
                });
            }
            startTogether.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        // If the claim were decided by the select rather than by the update's row count, four
        // relays scanning the same oldest-first window would hand the same records to several of
        // them and the same order would be published several times.
        assertThat(claimedTwice).isEmpty();
        assertThat(everClaimed).hasSize(records);
        assertThat(totalClaimed).hasValue(records);
    }

    @Test
    @Timeout(180)
    void a_relay_that_dies_holding_a_batch_does_not_strand_it() throws Exception {
        try (Connection transactional = dataSource.getConnection()) {
            transactional.setAutoCommit(false);
            JdbcOutboxStore store = newStore(transactional);
            store.add(order("o-1"));
            transactional.commit();

            // A short lease stands in for a relay that claimed this record and was killed. No
            // rollback follows and no connection is closed, because that is the point: nothing
            // tidies up, and the record has to come back anyway.
            assertThat(store.claimBatch(10, Duration.ofSeconds(2))).hasSize(1);
            assertThat(store.claimBatch(10, Duration.ofSeconds(2))).isEmpty();

            await().atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> !store.claimBatch(10, Duration.ofMinutes(1)).isEmpty());

            assertThat(store.pendingCount()).isEqualTo(1);
        }
    }
}
