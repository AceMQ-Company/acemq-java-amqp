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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.acemq.amqp.patterns.JdbcIdempotencyStore;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@DisplayName("the shared JDBC idempotency store")
class JdbcIdempotencyStoreTest {

    private JdbcDataSource dataSource;
    private JdbcIdempotencyStore store;

    @BeforeEach
    void setUp() {
        dataSource = new JdbcDataSource();
        // A database per test, so nothing leaks between them and each starts empty.
        dataSource.setURL("jdbc:h2:mem:idem-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        store = newStore(Duration.ofHours(24), Duration.ofMinutes(5));
        store.createSchemaIfAbsent();
    }

    @AfterEach
    void tearDown() {
        dataSource = null;
    }

    private JdbcIdempotencyStore newStore(Duration retention, Duration claimTimeout) {
        return new JdbcIdempotencyStore(dataSource, retention, claimTimeout, "acemq_idempotency");
    }

    @Nested
    @DisplayName("claim, confirm, release")
    class TheThreeSteps {

        @Test
        @DisplayName("the first claim wins and the second is refused")
        void firstClaimWins() {
            assertThat(store.claim("m-1")).isTrue();
            assertThat(store.claim("m-1")).isFalse();
        }

        @Test
        @DisplayName("a confirmed identifier stays refused")
        void confirmedStaysRefused() {
            store.claim("m-1");
            store.confirm("m-1");

            assertThat(store.isConfirmed("m-1")).isTrue();
            assertThat(store.claim("m-1")).isFalse();
        }

        @Test
        @DisplayName("a released identifier can be claimed again")
        void releasedCanBeClaimedAgain() {
            store.claim("m-1");
            store.release("m-1");

            // The point of release: a failed handler must not poison the identifier, or the
            // retry is discarded as a duplicate and the message is lost rather than retried.
            assertThat(store.claim("m-1")).isTrue();
            assertThat(store.isConfirmed("m-1")).isFalse();
        }

        @Test
        @DisplayName("releasing does not undo a confirmation")
        void releaseDoesNotUndoConfirmation() {
            store.claim("m-1");
            store.confirm("m-1");
            store.release("m-1");

            assertThat(store.isConfirmed("m-1"))
                    .as("a late release must not erase completed work")
                    .isTrue();
        }

        @Test
        @DisplayName("confirming an identifier nobody claimed still records it")
        void confirmWithoutClaimStillRecords() {
            // Happens when the row was purged, or the lease expired and another node took it. The
            // work was done either way, and an unrecorded confirmation is how it gets done twice.
            store.confirm("m-unclaimed");

            assertThat(store.isConfirmed("m-unclaimed")).isTrue();
        }
    }

    @Nested
    @DisplayName("sharing between processes")
    class Sharing {

        @Test
        @DisplayName("a second instance sees the first instance's claim")
        void separateInstancesShareState() {
            // Two instances on one table is what a consumer group actually looks like: the
            // redelivery lands on a different machine from the original.
            JdbcIdempotencyStore other = newStore(Duration.ofHours(24), Duration.ofMinutes(5));

            assertThat(store.claim("m-1")).isTrue();
            assertThat(other.claim("m-1"))
                    .as("the other instance must not process this message as well")
                    .isFalse();

            store.confirm("m-1");
            assertThat(other.isConfirmed("m-1")).isTrue();
        }

        @Test
        @DisplayName("one instance cannot release another's claim")
        void releaseIsScopedToTheClaimant() {
            JdbcIdempotencyStore other = newStore(Duration.ofHours(24), Duration.ofMinutes(5));
            store.claim("m-1");

            other.release("m-1");

            // If this returned true, a failed handler on one node would hand the message to a
            // second node while the first was still working on it.
            assertThat(other.claim("m-1")).isFalse();
        }

        @Test
        @Timeout(30)
        @DisplayName("only one of many concurrent claimants wins")
        void concurrentClaimantsHaveOneWinner() throws Exception {
            int contenders = 8;
            ExecutorService pool = Executors.newFixedThreadPool(contenders);
            try {
                CyclicBarrier startTogether = new CyclicBarrier(contenders);
                List<Callable<Boolean>> attempts = new ArrayList<>();
                for (int i = 0; i < contenders; i++) {
                    // A separate instance each, so this tests the database's atomicity rather
                    // than one object's synchronization.
                    JdbcIdempotencyStore contender = newStore(Duration.ofHours(24), Duration.ofMinutes(5));
                    attempts.add(() -> {
                        startTogether.await(10, TimeUnit.SECONDS);
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
                        .as("exactly one claimant may hold an identifier at a time")
                        .isEqualTo(1);
            } finally {
                pool.shutdownNow();
            }
        }
    }

    @Nested
    @DisplayName("expiry")
    class Expiry {

        @Test
        @DisplayName("a claim left by a dead consumer can be taken over")
        void expiredClaimsCanBeTakenOver() throws Exception {
            // The failure a shared store has and an in-process one cannot: a consumer that dies
            // mid-handler leaves its claim behind. Without a lease, every redelivery of that
            // message is discarded as a duplicate forever, and a crash that should have cost one
            // retry has silently deleted the message instead.
            JdbcIdempotencyStore leased = newStore(Duration.ofHours(24), Duration.ofMillis(200));
            assertThat(leased.claim("m-crashed")).isTrue();

            assertThat(leased.claim("m-crashed"))
                    .as("the hold is still good while the lease runs")
                    .isFalse();

            Thread.sleep(300);

            JdbcIdempotencyStore survivor = newStore(Duration.ofHours(24), Duration.ofMillis(200));
            assertThat(survivor.claim("m-crashed"))
                    .as("once the lease has run out another consumer may take the message")
                    .isTrue();
        }

        @Test
        @DisplayName("a confirmation stops binding once retention has passed")
        void confirmationsExpire() throws Exception {
            JdbcIdempotencyStore brief = newStore(Duration.ofMillis(200), Duration.ofMinutes(5));
            brief.claim("m-1");
            brief.confirm("m-1");

            Thread.sleep(300);

            assertThat(brief.isConfirmed("m-1"))
                    .as("retention is the window duplicates are expected in, not a permanent record")
                    .isFalse();
            assertThat(brief.claim("m-1")).isTrue();
        }

        @Test
        @DisplayName("purging removes what has expired and keeps what has not")
        void purgeRemovesOnlyExpiredRows() throws Exception {
            JdbcIdempotencyStore brief = newStore(Duration.ofMillis(200), Duration.ofMinutes(5));
            brief.claim("m-old");
            brief.confirm("m-old");
            brief.claim("m-live");

            Thread.sleep(300);

            assertThat(brief.purgeExpired()).isEqualTo(1);
            assertThat(brief.size())
                    .as("the live claim must survive the purge, or its message would be handled twice")
                    .isEqualTo(1L);
        }

        @Test
        @DisplayName("nothing is purged on the message path")
        void claimingDoesNotPurge() throws Exception {
            JdbcIdempotencyStore brief = newStore(Duration.ofMillis(100), Duration.ofMinutes(5));
            brief.claim("m-1");
            brief.confirm("m-1");
            Thread.sleep(200);

            brief.claim("m-2");

            // Asserting the absence deliberately: tidying up inside claim would make every
            // message on every consumer pay for a delete on a shared table.
            assertThat(brief.size()).isEqualTo(2L);
        }
    }

    @Nested
    @DisplayName("refusals")
    class Refusals {

        @Test
        @DisplayName("a table name that is not a plain identifier is rejected")
        void refusesUnsafeTableNames() {
            // The name is concatenated into SQL because no database binds an identifier as a
            // parameter, so the validation is the only thing standing between it and injection.
            assertThatThrownBy(() -> new JdbcIdempotencyStore(
                    dataSource, Duration.ofHours(1), Duration.ofMinutes(1), "idem; DROP TABLE users"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("plain SQL identifier");
        }

        @Test
        @DisplayName("a non-positive retention or claim timeout is rejected")
        void refusesNonPositiveDurations() {
            assertThatThrownBy(() -> new JdbcIdempotencyStore(
                    dataSource, Duration.ZERO, Duration.ofMinutes(1), "acemq_idempotency"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("retention");

            assertThatThrownBy(() -> new JdbcIdempotencyStore(
                    dataSource, Duration.ofHours(1), Duration.ZERO, "acemq_idempotency"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("claimTimeout");
        }
    }
}
