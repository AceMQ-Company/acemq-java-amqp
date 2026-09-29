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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;

import javax.sql.DataSource;

import org.acemq.amqp.patterns.JdbcIdempotencyStore;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A duplicate claim must be refused on a driver that does not set {@code SQLState}.
 *
 * <p>The store decided what counted as a duplicate by reading {@code SQLState} and looking for
 * class 23, the SQL standard's integrity-constraint class. That reads well and is right on
 * PostgreSQL, H2 and SQL Server — and it is not something a driver is obliged to do.
 * sqlite-jdbc does not: measured on 3.47.1.0, a {@code PRIMARY KEY} violation arrives as
 * {@code SQLState = null} with {@code errorCode = 19}. So on SQLite a second claim of the same
 * message <em>threw</em> instead of returning false, and a consumer met an ordinary duplicate by
 * dead-lettering it rather than skipping it. SQLite is what people develop against even when
 * production is PostgreSQL, so it was most likely to be met on somebody's first afternoon.
 *
 * <p>These tests state the contract rather than the driver: a duplicate is refused whatever the
 * driver says about it, and a failure that is <em>not</em> a duplicate is still raised. The
 * second half matters as much as the first. Treating every failed insert as a duplicate would
 * make a lock timeout look like "somebody already has this message", and the caller would
 * acknowledge a message nothing had handled — a silent loss, arrived at while fixing one.
 */
@DisplayName("claiming against a driver that reports no SQLState")
class ClaimWithoutSqlStateTest {

    private JdbcDataSource h2;

    @BeforeEach
    void setUp() {
        h2 = new JdbcDataSource();
        h2.setURL("jdbc:h2:mem:nostate-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        h2.setUser("sa");
    }

    @Test
    @DisplayName("refuses the second claim rather than throwing")
    void a_duplicate_is_refused_without_sqlstate() {
        DataSource blinded = withoutSqlState(h2);
        JdbcIdempotencyStore store = new JdbcIdempotencyStore(
                blinded, Duration.ofHours(24), Duration.ofMinutes(5), "acemq_idempotency");
        store.createSchemaIfAbsent();

        assertThat(store.claim("m-1")).isTrue();

        assertThat(store.claim("m-1"))
                .as("a duplicate delivery must be refused so the consumer can skip it. Throwing "
                        + "here sends an ordinary duplicate down the retry ladder and into the "
                        + "dead-letter queue, which is the alarm this pattern exists to avoid")
                .isFalse();
    }

    @Test
    @DisplayName("still raises a failure that is not a duplicate")
    void a_real_failure_is_still_raised() {
        // No schema created, so the insert fails because the table is not there. That is not a
        // duplicate and must not be reported as one: answering false would tell the caller
        // somebody else holds the message, and the caller would acknowledge it unhandled.
        DataSource blinded = withoutSqlState(h2);
        JdbcIdempotencyStore store = new JdbcIdempotencyStore(
                blinded, Duration.ofHours(24), Duration.ofMinutes(5), "acemq_idempotency");

        assertThatThrownBy(() -> store.claim("m-1"))
                .as("a missing table is not a duplicate, and swallowing it would acknowledge "
                        + "messages nothing had handled")
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("an expired claim is still taken over")
    void the_lease_still_works_without_sqlstate() {
        DataSource blinded = withoutSqlState(h2);
        JdbcIdempotencyStore store = new JdbcIdempotencyStore(
                blinded, Duration.ofHours(24), Duration.ofMillis(1), "acemq_idempotency");
        store.createSchemaIfAbsent();

        assertThat(store.claim("m-1")).isTrue();
        sleep(30);

        assertThat(store.claim("m-1"))
                .as("the claim of a consumer that died must still be retakeable; refusing the "
                        + "duplicate must not have cost us the lease")
                .isTrue();
    }

    /**
     * The same database, behind a driver that reports no {@code SQLState} — which is what
     * sqlite-jdbc does, and what the store must not depend on.
     *
     * <p>A proxy rather than a second driver on the test path: the contract is "do not rely on
     * SQLState", and a test that says exactly that cannot rot when a driver changes its mind.
     */
    private static DataSource withoutSqlState(DataSource real) {
        return (DataSource) Proxy.newProxyInstance(
                ClaimWithoutSqlStateTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object result = invoke(real, method, args);
                    return result instanceof Connection ? blindConnection((Connection) result) : result;
                });
    }

    private static Connection blindConnection(Connection real) {
        return (Connection) Proxy.newProxyInstance(
                ClaimWithoutSqlStateTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    Object result = invoke(real, method, args);
                    return result instanceof PreparedStatement
                            ? blindStatement((PreparedStatement) result)
                            : result;
                });
    }

    private static PreparedStatement blindStatement(PreparedStatement real) {
        return (PreparedStatement) Proxy.newProxyInstance(
                ClaimWithoutSqlStateTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> {
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof SQLException) {
                            // The same failure, with the one field this store used to rely on
                            // removed. sqlite-jdbc reports null here; H2 reports 23505.
                            SQLException original = (SQLException) cause;
                            throw new SQLException(original.getMessage(), null, original.getErrorCode());
                        }
                        throw cause;
                    }
                });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args)
            throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
