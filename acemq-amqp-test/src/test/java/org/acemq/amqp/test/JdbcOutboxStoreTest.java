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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.Envelope;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.patterns.JdbcOutboxStore;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("the JDBC outbox store")
class JdbcOutboxStoreTest {

    private JdbcDataSource dataSource;
    private Connection transactional;
    private JdbcOutboxStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = new JdbcDataSource();
        // A database per test, so nothing leaks between them and each starts empty.
        dataSource.setURL("jdbc:h2:mem:outbox-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");

        transactional = dataSource.getConnection();
        transactional.setAutoCommit(false);

        store = new JdbcOutboxStore(() -> transactional, dataSource);
        store.createSchemaIfAbsent();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (transactional != null && !transactional.isClosed()) {
            transactional.rollback();
            transactional.close();
        }
    }

    private static OutboxRecord order(String id) {
        return OutboxRecord.of("orders", "order.placed", Envelope.of("order.placed").id(id).build(), "{\"id\":\"" + id
                + "\"}");
    }

    @Nested
    @DisplayName("the transaction it writes in")
    class Transaction {

        @Test
        @Timeout(30)
        void a_record_written_in_a_rolled_back_transaction_is_never_published() throws SQLException {
            store.add(order("o-1"));

            // The whole pattern, in one assertion. The business work failed, so the message it
            // would have announced must not exist. A store that opened its own connection would
            // have committed this row already and the relay would be publishing an event for an
            // order that does not exist.
            transactional.rollback();

            assertThat(store.pendingCount()).isZero();
            assertThat(store.claimBatch(10, Duration.ofMinutes(1))).isEmpty();
        }

        @Test
        @Timeout(30)
        void a_record_written_in_a_committed_transaction_is_published() throws SQLException {
            store.add(order("o-1"));

            // Uncommitted, the relay cannot see it: the message becomes real at exactly the
            // moment the work it describes becomes real, and not a moment sooner.
            assertThat(store.claimBatch(10, Duration.ofMinutes(1))).isEmpty();

            transactional.commit();

            assertThat(store.claimBatch(10, Duration.ofMinutes(1)))
                    .extracting(OutboxRecord::id)
                    .containsExactly("o-1");
        }

        @Test
        @Timeout(30)
        void refuses_to_write_when_no_transactional_connection_can_be_had() {
            JdbcOutboxStore broken = new JdbcOutboxStore(
                    () -> {
                        throw new SQLException("no transaction is bound to this thread");
                    },
                    dataSource);

            assertThatThrownBy(() -> broken.add(order("o-1")))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("no transactional connection");
        }

        @Test
        @Timeout(30)
        void refuses_to_write_when_the_supplier_returns_nothing() {
            JdbcOutboxStore broken = new JdbcOutboxStore(() -> null, dataSource);

            assertThatThrownBy(() -> broken.add(order("o-1")))
                    .isInstanceOf(AceMqException.class)
                    .hasMessageContaining("returned null");
        }

        @Test
        @Timeout(30)
        void leaves_the_caller_connection_open_for_the_rest_of_the_transaction() throws SQLException {
            store.add(order("o-1"));

            // Closing a borrowed connection would end the caller's transaction underneath it,
            // committing or rolling back work the caller had not finished.
            assertThat(transactional.isClosed()).isFalse();
            assertThat(transactional.getAutoCommit()).isFalse();
            transactional.commit();
        }
    }

    @Nested
    @DisplayName("claiming")
    class Claiming {

        @Test
        @Timeout(30)
        void a_claimed_record_is_not_handed_out_again_while_the_lease_holds() throws SQLException {
            store.add(order("o-1"));
            transactional.commit();

            assertThat(store.claimBatch(10, Duration.ofMinutes(5))).hasSize(1);
            assertThat(store.claimBatch(10, Duration.ofMinutes(5))).isEmpty();
        }

        @Test
        @Timeout(30)
        void an_expired_lease_returns_the_record_to_circulation() throws Exception {
            store.add(order("o-1"));
            transactional.commit();

            // A lease rather than a lock, so a relay that died holding this record strands it for
            // exactly as long as the lease and no longer. Nobody has to notice or intervene.
            assertThat(store.claimBatch(10, Duration.ofMillis(50))).hasSize(1);
            Thread.sleep(120);
            assertThat(store.claimBatch(10, Duration.ofMinutes(5))).hasSize(1);
        }

        @Test
        @Timeout(30)
        void hands_out_oldest_first_and_no_more_than_asked_for() throws SQLException {
            for (int i = 0; i < 5; i++) {
                store.add(order("o-" + i));
            }
            transactional.commit();

            assertThat(store.claimBatch(2, Duration.ofMinutes(5)))
                    .extracting(OutboxRecord::id)
                    .containsExactly("o-0", "o-1");
        }

        @Test
        @Timeout(30)
        void rejects_a_batch_size_or_lease_that_cannot_work() {
            assertThatThrownBy(() -> store.claimBatch(0, Duration.ofMinutes(1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("batchSize");
            assertThatThrownBy(() -> store.claimBatch(1, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("lease");
        }
    }

    @Nested
    @DisplayName("marking")
    class Marking {

        @Test
        @Timeout(30)
        void a_published_record_is_finished_with() throws SQLException {
            store.add(order("o-1"));
            transactional.commit();
            store.claimBatch(10, Duration.ofMinutes(5));

            store.markPublished("o-1");

            assertThat(store.pendingCount()).isZero();
            assertThat(store.claimBatch(10, Duration.ofMinutes(5))).isEmpty();
        }

        @Test
        @Timeout(30)
        void a_failed_record_comes_straight_back_with_its_attempt_counted() throws SQLException {
            store.add(order("o-1"));
            transactional.commit();
            store.claimBatch(10, Duration.ofMinutes(5));

            store.markFailed("o-1", "the broker refused it");

            // The claim is released along with the failure, so the retry does not have to wait
            // out a lease that is no longer held by anyone.
            List<OutboxRecord> again = store.claimBatch(10, Duration.ofMinutes(5));
            assertThat(again).hasSize(1);
            assertThat(again.get(0).attempts()).isEqualTo(1);
            assertThat(again.get(0).lastError()).contains("the broker refused it");
        }

        @Test
        @Timeout(30)
        void a_record_that_keeps_failing_is_left_alone_for_someone_to_look_at() throws SQLException {
            JdbcOutboxStore limited = new JdbcOutboxStore(() -> transactional, dataSource, "acemq_outbox", 2);
            limited.add(order("o-1"));
            transactional.commit();

            for (int attempt = 0; attempt < 5; attempt++) {
                for (OutboxRecord record : limited.claimBatch(10, Duration.ofMinutes(5))) {
                    limited.markFailed(record.id(), "still refused");
                }
            }

            assertThat(limited.claimBatch(10, Duration.ofMinutes(5))).isEmpty();
            // Still there, and still counted as pending: unpublishable is not the same as gone.
            assertThat(limited.pendingCount()).isEqualTo(1);
        }

        @Test
        @Timeout(30)
        void a_failure_that_cannot_itself_be_recorded_is_survived() throws SQLException {
            store.add(order("o-1"));
            transactional.commit();

            JdbcDataSource unusable = new JdbcDataSource();
            unusable.setURL("jdbc:h2:mem:missing-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            JdbcOutboxStore withNoTable = new JdbcOutboxStore(() -> transactional, unusable);

            // markFailed is already the failure path. Throwing here would abandon the rest of the
            // batch to report something that changes nothing: the record stays in the table
            // either way, and its lease will expire.
            withNoTable.markFailed("o-1", "the broker refused it");
        }

        @Test
        @Timeout(30)
        void an_error_too_long_for_the_column_is_shortened_rather_than_lost() throws SQLException {
            store.add(order("o-1"));
            transactional.commit();
            store.claimBatch(10, Duration.ofMinutes(5));

            StringBuilder huge = new StringBuilder();
            for (int i = 0; i < 200; i++) {
                huge.append("a stack trace line that goes on and on. ");
            }
            store.markFailed("o-1", huge.toString());

            List<OutboxRecord> again = store.claimBatch(10, Duration.ofMinutes(5));
            assertThat(again.get(0).lastError()).isPresent();
            assertThat(again.get(0).lastError().get()).hasSize(1000).endsWith("...");
        }
    }

    @Nested
    @DisplayName("the table")
    class Table {

        @Test
        @Timeout(30)
        void creating_the_schema_twice_is_harmless() {
            store.createSchemaIfAbsent();
            store.createSchemaIfAbsent();
            assertThat(store.pendingCount()).isZero();
        }

        @Test
        void refuses_a_table_name_that_is_not_a_plain_identifier() {
            // The table name reaches the SQL by concatenation, because no database lets it be
            // bound. Refusing anything but an identifier is what keeps that from being a hole.
            assertThatThrownBy(() -> new JdbcOutboxStore(() -> transactional, dataSource, "outbox; DROP TABLE t", 5))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("plain SQL identifier");
            assertThatThrownBy(() -> new JdbcOutboxStore(() -> transactional, dataSource, "", 5))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new JdbcOutboxStore(() -> transactional, dataSource, "acemq_outbox", 0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxAttempts");
        }

        @Test
        @Timeout(30)
        void works_under_a_table_name_of_the_caller_s_choosing() throws SQLException {
            JdbcOutboxStore named = new JdbcOutboxStore(() -> transactional, dataSource, "messages_pending", 5);
            named.createSchemaIfAbsent();

            named.add(order("o-1"));
            transactional.commit();

            assertThat(named.claimBatch(10, Duration.ofMinutes(5))).hasSize(1);
            assertThat(named.toString()).contains("messages_pending");
        }

        @Test
        @Timeout(30)
        void purging_removes_published_records_and_leaves_the_rest() throws SQLException {
            store.add(order("o-published"));
            store.add(order("o-waiting"));
            transactional.commit();
            store.markPublished("o-published");

            assertThat(store.purgePublished(Duration.ZERO)).isEqualTo(1);
            assertThat(store.pendingCount()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("what it stores")
    class Fields {

        @Test
        @Timeout(30)
        void carries_every_field_back_out_intact() throws SQLException {
            Envelope envelope = Envelope.of("order.placed")
                    .id("o-1")
                    .correlationId("checkout-9")
                    .causationId("cmd-3")
                    .build();
            store.add(OutboxRecord.of("orders", "order.placed", envelope, "{\"total\":42}"));
            transactional.commit();

            OutboxRecord read = store.claimBatch(1, Duration.ofMinutes(5)).get(0);

            assertThat(read.id()).isEqualTo("o-1");
            assertThat(read.exchange()).isEqualTo("orders");
            assertThat(read.routingKey()).isEqualTo("order.placed");
            assertThat(read.type()).isEqualTo("order.placed");
            assertThat(read.payload()).isEqualTo("{\"total\":42}");
            assertThat(read.correlationId()).contains("checkout-9");
            assertThat(read.causationId()).contains("cmd-3");
            assertThat(read.createdAt()).isNotNull();
            assertThat(read.attempts()).isZero();
            assertThat(read.lastError()).isEmpty();
        }

        @Test
        @Timeout(30)
        void a_record_without_causation_reads_back_without_one() throws SQLException {
            store.add(OutboxRecord.of(
                    "", "orders.direct", Envelope.of("order.placed").id("o-2").build(), "{}"));
            transactional.commit();

            OutboxRecord read = store.claimBatch(1, Duration.ofMinutes(5)).get(0);

            assertThat(read.exchange()).isEmpty();
            assertThat(read.causationId()).isEmpty();
            // A correlation identifier is always present: the envelope defaults it to the message
            // id when the caller does not set one.
            assertThat(read.correlationId()).isPresent();
        }

        @Test
        @Timeout(30)
        void refuses_two_records_with_the_same_identifier() throws SQLException {
            store.add(order("o-1"));
            transactional.commit();

            AtomicInteger rejected = new AtomicInteger();
            try {
                store.add(order("o-1"));
                transactional.commit();
            } catch (AceMqException expected) {
                rejected.incrementAndGet();
                transactional.rollback();
            }

            // The primary key is the message identifier, which makes writing the same message
            // twice a database error rather than two deliveries nobody expected.
            assertThat(rejected).hasValue(1);
            assertThat(store.pendingCount()).isEqualTo(1);
        }
    }
}
