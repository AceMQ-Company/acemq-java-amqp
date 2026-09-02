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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.SchemaDefinition;
import org.acemq.amqp.patterns.JdbcSchemaRegistry;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("the JDBC schema registry")
class JdbcSchemaRegistryTest {

    private static final String ORDER = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[]}";
    private static final String CUSTOMER = "{\"type\":\"record\",\"name\":\"Customer\",\"fields\":[]}";

    private JdbcDataSource dataSource;
    private JdbcSchemaRegistry registry;

    @BeforeEach
    void setUp() {
        dataSource = new JdbcDataSource();
        // A database per test, so nothing leaks between them and each starts empty.
        dataSource.setURL("jdbc:h2:mem:schemas-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        registry = new JdbcSchemaRegistry(dataSource);
        registry.createSchemaIfAbsent();
    }

    private static SchemaDefinition avro(String subject, String definition) {
        return new SchemaDefinition("avro", subject, definition);
    }

    @Nested
    @DisplayName("registering")
    class Registering {

        @Test
        @DisplayName("the same schema twice gets the same identifier")
        void sameSchemaSameId() {
            int first = registry.idFor(avro("Order", ORDER));
            int second = registry.idFor(avro("Order", ORDER));

            assertThat(second).isEqualTo(first);
            assertThat(registry.size()).isEqualTo(1);
        }

        @Test
        @DisplayName("two schemas get two identifiers")
        void differentSchemasDifferentIds() {
            int order = registry.idFor(avro("Order", ORDER));
            int customer = registry.idFor(avro("Customer", CUSTOMER));

            assertThat(customer).isNotEqualTo(order);
            assertThat(registry.size()).isEqualTo(2);
        }

        @Test
        @DisplayName("identifiers start at one, so zero is never a schema")
        void idsStartAtOne() {
            // An int field nobody set reads as zero. Leaving it unused means a message claiming
            // schema 0 is a bug rather than a lookup that happens to succeed.
            assertThat(registry.idFor(avro("Order", ORDER))).isEqualTo(1);
        }

        @Test
        @DisplayName("the same bytes under another subject are the same schema")
        void subjectIsNotIdentity() {
            int first = registry.idFor(avro("Order", ORDER));
            int renamed = registry.idFor(avro("SomethingElse", ORDER));

            assertThat(renamed).isEqualTo(first);
            assertThat(registry.size()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("looking up")
    class LookingUp {

        @Test
        @DisplayName("gives back the schema that was registered")
        void roundTrips() {
            int id = registry.idFor(avro("Order", ORDER));

            SchemaDefinition found = registry.schemaFor(id);

            assertThat(found.format()).isEqualTo("avro");
            assertThat(found.subject()).isEqualTo("Order");
            assertThat(found.definition()).isEqualTo(ORDER);
        }

        @Test
        @DisplayName("an unknown identifier says so, and says where to look")
        void unknownIdFails() {
            assertThatThrownBy(() -> registry.schemaFor(4242))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("4242")
                    .hasMessageContaining("another environment");
        }
    }

    @Nested
    @DisplayName("across restarts")
    class AcrossRestarts {

        @Test
        @DisplayName("identifiers survive, because the messages that carry them do")
        void idsAreStable() {
            int id = new JdbcSchemaRegistry(dataSource).idFor(avro("Order", ORDER));

            // A second instance on the same table is what a restart looks like from the
            // database's side, and is also what a second replica looks like. Neither may
            // invent a new identifier: a message written yesterday still names this one.
            JdbcSchemaRegistry afterRestart = new JdbcSchemaRegistry(dataSource);

            assertThat(afterRestart.idFor(avro("Order", ORDER))).isEqualTo(id);
            assertThat(afterRestart.schemaFor(id).definition()).isEqualTo(ORDER);
        }

        @Test
        @DisplayName("a fresh instance reads a schema it never registered")
        void readsWithoutACache() {
            int id = registry.idFor(avro("Order", ORDER));

            assertThat(new JdbcSchemaRegistry(dataSource).schemaFor(id).definition()).isEqualTo(ORDER);
        }
    }

    @Nested
    @DisplayName("under contention")
    class UnderContention {

        @Test
        @Timeout(30)
        @DisplayName("everyone registering one schema at once agrees on one identifier")
        void oneSchemaOneId() throws Exception {
            List<Integer> ids = inParallel(8, index -> registry.idFor(avro("Order", ORDER)));

            assertThat(new HashSet<>(ids)).hasSize(1);
            assertThat(registry.size()).isEqualTo(1);
        }

        @Test
        @Timeout(30)
        @DisplayName("separate instances racing on one schema agree too")
        void separateInstancesAgree() throws Exception {
            // Separate instances share no cache, so every one of them reaches the table. This is
            // the start-up of eight replicas, and the case the fingerprint index exists for.
            List<Integer> ids = inParallel(8, index -> new JdbcSchemaRegistry(dataSource).idFor(avro("Order", ORDER)));

            assertThat(new HashSet<>(ids)).hasSize(1);
            assertThat(registry.size()).isEqualTo(1);
        }

        @Test
        @Timeout(30)
        @DisplayName("different schemas registered at once each keep an identifier of their own")
        void differentSchemasSurviveTheRace() throws Exception {
            // Here the collision is on the identifier rather than the fingerprint: two writers
            // compute the same "one past the highest" for two different schemas. Whoever loses
            // has to try again rather than adopt the winner's identifier, which would hand two
            // schemas the same number and make one of them unreadable.
            int writers = 8;
            List<Integer> ids = inParallel(
                    writers,
                    index -> new JdbcSchemaRegistry(dataSource)
                            .idFor(avro("Type" + index, "{\"type\":\"record\",\"name\":\"T" + index + "\"}")));

            Set<Integer> distinct = new HashSet<>(ids);
            assertThat(distinct).hasSize(writers);
            assertThat(registry.size()).isEqualTo(writers);

            // And every identifier still resolves to the schema it was given for.
            for (int i = 0; i < writers; i++) {
                assertThat(new JdbcSchemaRegistry(dataSource).schemaFor(ids.get(i)).subject())
                        .isEqualTo("Type" + i);
            }
        }

        /** Runs {@code work} on {@code count} threads released together, and collects the results. */
        private List<Integer> inParallel(int count, ThrowingIntFunction work) throws Exception {
            ExecutorService pool = Executors.newFixedThreadPool(count);
            CyclicBarrier startTogether = new CyclicBarrier(count);
            try {
                List<Future<Integer>> futures = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    int index = i;
                    Callable<Integer> task = () -> {
                        startTogether.await(20, TimeUnit.SECONDS);
                        return work.apply(index);
                    };
                    futures.add(pool.submit(task));
                }
                List<Integer> results = new ArrayList<>();
                for (Future<Integer> future : futures) {
                    results.add(future.get(20, TimeUnit.SECONDS));
                }
                return results;
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Nested
    @DisplayName("the table name")
    class TheTableName {

        @Test
        @DisplayName("is rejected unless it is a plain identifier")
        void rejectsInjection() {
            // It reaches SQL by concatenation because no database binds an identifier as a
            // parameter, so this check is the only thing standing there.
            assertThatThrownBy(() -> new JdbcSchemaRegistry(dataSource, "schemas; DROP TABLE orders"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("plain SQL identifier");
        }

        @Test
        @DisplayName("can be chosen")
        void honoursACustomTable() {
            JdbcSchemaRegistry custom = new JdbcSchemaRegistry(dataSource, "my_schemas");
            custom.createSchemaIfAbsent();

            int id = custom.idFor(avro("Order", ORDER));

            assertThat(new JdbcSchemaRegistry(dataSource, "my_schemas").schemaFor(id).definition())
                    .isEqualTo(ORDER);
            // The default table is a different table, and knows nothing about it.
            assertThat(registry.size()).isZero();
        }

        @Test
        @DisplayName("creating the schema twice is not an error")
        void createIsIdempotent() {
            registry.idFor(avro("Order", ORDER));
            registry.createSchemaIfAbsent();

            assertThat(registry.size()).isEqualTo(1);
        }
    }

    @FunctionalInterface
    private interface ThrowingIntFunction {
        Integer apply(int index) throws Exception;
    }
}
