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
package org.acemq.amqp.patterns;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.IdempotencyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An idempotency store in a database table, shared by every consumer that points at it.
 *
 * <p>The one {@link InMemoryIdempotencyStore} cannot replace. An in-process store deduplicates
 * within one JVM, which is enough when duplicates arrive seconds apart on the same instance and
 * useless the moment there are three instances behind one queue: the redelivery lands on a
 * different machine from the original, finds an empty map, and does the work again. Charging a
 * card twice is not a caching problem.
 *
 * <h2>The failure this has and the in-memory one does not</h2>
 *
 * <p>A shared store outlives the process using it, and that cuts both ways. If a consumer claims
 * a message and then dies mid-handler, the claim stays in the table. With a naive implementation
 * every redelivery of that message — forever — is discarded as a duplicate, and a crash that
 * should have cost one retry has silently deleted a message instead. The in-memory store never
 * had this problem only because a crash wiped it.
 *
 * <p>So a claim here is a <strong>lease</strong>, not a lock. It expires after
 * {@code claimTimeout}, after which another consumer may take it over. That makes the timeout a
 * real decision: too short and two consumers process the same message concurrently while the
 * first is still working; too long and a crashed consumer stalls that message for the duration.
 * It should comfortably exceed the slowest handler, and the default is deliberately generous.
 *
 * <p>Confirmations are kept for {@code retention} and then forgotten, because a table that
 * remembers every identifier ever seen is a disk-space incident waiting to happen. A duplicate
 * arriving after retention is handled again — retention is the window within which duplicates
 * are actually expected to arrive, not a permanent record.
 *
 * <h2>What it does not do</h2>
 *
 * <p>This deduplicates the delivery, not the work. If the handler writes to a different database
 * from this table, a crash between the write committing and {@link #confirm} landing leaves the
 * work done and unrecorded, and the redelivery repeats it. Putting this table in the same
 * database as the handler's writes, in the handler's transaction, is what closes that gap — and
 * doing so is the caller's decision, since only the caller knows what the handler touches.
 *
 * <pre>{@code
 * JdbcIdempotencyStore store = new JdbcIdempotencyStore(dataSource);
 * store.createSchemaIfAbsent();          // development and tests only
 *
 * mq.consume("orders.new", OrderPlaced.class,
 *         ConsumerOptions.prefetch(20).idempotent(store),
 *         message -> payments.charge(message.payload()));
 * }</pre>
 *
 * <p>Schedule {@link #purgeExpired()} — nothing here deletes rows on its own, because a store
 * that deletes on the hot path pays for it on every single message.
 */
public final class JdbcIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcIdempotencyStore.class);

    private static final String DEFAULT_TABLE = "acemq_idempotency";

    /** Long enough for a slow handler, short enough that a crash is not a permanent stall. */
    private static final Duration DEFAULT_CLAIM_TIMEOUT = Duration.ofMinutes(5);

    private static final Duration DEFAULT_RETENTION = Duration.ofHours(24);

    private static final String CLAIMED = "CLAIMED";

    private static final String CONFIRMED = "CONFIRMED";

    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    /** SQLState class 23: integrity constraint violation, which here means "someone else won". */
    private static final String INTEGRITY_VIOLATION = "23";

    private final DataSource dataSource;
    private final String table;
    private final Duration retention;
    private final Duration claimTimeout;

    /**
     * Identifies this instance's claims, so {@link #release} can only give up its own.
     *
     * <p>Without it, releasing a failed handler's claim would also delete a claim another node
     * had legitimately taken over after the lease expired, and both would then run.
     */
    private final String nodeId = UUID.randomUUID().toString();

    /**
     * @param dataSource where the table lives
     */
    public JdbcIdempotencyStore(DataSource dataSource) {
        this(dataSource, DEFAULT_RETENTION, DEFAULT_CLAIM_TIMEOUT, DEFAULT_TABLE);
    }

    /**
     * @param dataSource where the table lives
     * @param retention how long a confirmed identifier is remembered; duplicates arriving later
     *     than this are handled again
     * @param claimTimeout how long one consumer may hold a message before another may take it
     *     over. Must exceed the slowest handler, or two consumers will process the same message
     *     at once
     * @param table table name; must be a plain SQL identifier
     */
    public JdbcIdempotencyStore(DataSource dataSource, Duration retention, Duration claimTimeout, String table) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (retention.isNegative() || retention.isZero()) {
            throw new IllegalArgumentException("retention must be positive, was " + retention);
        }
        if (claimTimeout.isNegative() || claimTimeout.isZero()) {
            throw new IllegalArgumentException("claimTimeout must be positive, was " + claimTimeout);
        }
        if (!SAFE_TABLE_NAME.matcher(Objects.requireNonNull(table, "table")).matches()) {
            // A table name reaches SQL by concatenation because no database lets it be bound as a
            // parameter. Rejecting anything but a plain identifier is what keeps that safe.
            throw new IllegalArgumentException("table must be a plain SQL identifier, was '" + table + "'");
        }
        this.retention = retention;
        this.claimTimeout = claimTimeout;
        this.table = table;
    }

    /**
     * Creates the table if it is not already there.
     *
     * <p>For development and tests. In production the table belongs in the migration tool that
     * owns the rest of the schema.
     *
     * @throws AceMqException if the schema cannot be created
     */
    public void createSchemaIfAbsent() {
        // Comments come out before the file is split, not after, or a semicolon inside a comment
        // would hand the remains of an English sentence to the database as SQL.
        String ddl = stripComments(readSchema()).replace("${table}", table);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            for (String piece : ddl.split(";")) {
                String sql = piece.trim();
                if (!sql.isEmpty()) {
                    statement.execute(sql);
                }
            }
        } catch (SQLException e) {
            throw new AceMqException("could not create the idempotency table '" + table + "'", e);
        }
    }

    @Override
    public boolean claim(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        Instant now = Instant.now();

        try (Connection connection = dataSource.getConnection()) {
            // The insert is the claim. A primary key makes it atomic across every process using
            // this table, which is the entire reason a shared store can be trusted: two consumers
            // racing on the same identifier cannot both succeed, whatever else they are doing.
            if (insertClaim(connection, messageId, now)) {
                return true;
            }
            // Somebody already has a row. Taking it over is allowed only if their hold has run
            // out, and the guard lives in the WHERE clause so the check and the take-over are one
            // statement -- a select followed by an update would let two consumers both pass the
            // check and both conclude they had won.
            return takeOverExpired(connection, messageId, now);
        } catch (SQLException e) {
            throw new AceMqException("could not claim message " + messageId + " in '" + table + "'", e);
        }
    }

    private boolean insertClaim(Connection connection, String messageId, Instant now) throws SQLException {
        String sql = "INSERT INTO " + table + " (message_id, state, claimed_by, recorded_at, expires_at)"
                + " VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, messageId);
            statement.setString(2, CLAIMED);
            statement.setString(3, nodeId);
            statement.setObject(4, atUtc(now));
            statement.setObject(5, atUtc(now.plus(claimTimeout)));
            return statement.executeUpdate() == 1;
        } catch (SQLException e) {
            if (isDuplicateKey(e)) {
                return false;
            }
            throw e;
        }
    }

    private boolean takeOverExpired(Connection connection, String messageId, Instant now) throws SQLException {
        String sql = "UPDATE " + table + " SET state = ?, claimed_by = ?, recorded_at = ?, expires_at = ?"
                + " WHERE message_id = ? AND expires_at <= ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, CLAIMED);
            statement.setString(2, nodeId);
            statement.setObject(3, atUtc(now));
            statement.setObject(4, atUtc(now.plus(claimTimeout)));
            statement.setString(5, messageId);
            statement.setObject(6, atUtc(now));
            boolean tookOver = statement.executeUpdate() == 1;
            if (tookOver) {
                // Worth a line in the log: either a consumer died holding this, or a handler is
                // slower than claimTimeout, and the second one means duplicate processing.
                log.warn("took over an expired hold on message {} in '{}'. Either a consumer died holding it, or a"
                        + " handler is slower than the {} claim timeout -- if it is the latter, two consumers are"
                        + " now processing this message at once.", messageId, table, claimTimeout);
            }
            return tookOver;
        }
    }

    @Override
    public void confirm(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        Instant now = Instant.now();
        String sql = "UPDATE " + table + " SET state = ?, claimed_by = NULL, recorded_at = ?, expires_at = ?"
                + " WHERE message_id = ?";

        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, CONFIRMED);
            statement.setObject(2, atUtc(now));
            statement.setObject(3, atUtc(now.plus(retention)));
            statement.setString(4, messageId);
            if (statement.executeUpdate() == 0) {
                // The row was purged, or the lease expired and someone else took it. The work
                // still happened, so it is recorded either way: an unrecorded confirmation is how
                // the same charge gets made twice.
                insertConfirmed(connection, messageId, now);
            }
        } catch (SQLException e) {
            throw new AceMqException("could not confirm message " + messageId + " in '" + table + "'", e);
        }
    }

    private void insertConfirmed(Connection connection, String messageId, Instant now) throws SQLException {
        String sql = "INSERT INTO " + table + " (message_id, state, claimed_by, recorded_at, expires_at)"
                + " VALUES (?, ?, NULL, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, messageId);
            statement.setString(2, CONFIRMED);
            statement.setObject(3, atUtc(now));
            statement.setObject(4, atUtc(now.plus(retention)));
            statement.executeUpdate();
        } catch (SQLException e) {
            if (isDuplicateKey(e)) {
                // Another node inserted between the update and this insert. Its row is a claim on
                // work we have already finished; not worth a third round trip to correct, because
                // the worst case is one repeat of an operation that has to be idempotent anyway.
                log.debug("message {} was re-claimed elsewhere while confirming it", messageId);
                return;
            }
            throw e;
        }
    }

    @Override
    public void release(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        // Only our own live claim, and never a confirmation. Deleting someone else's claim would
        // put two consumers on one message; deleting a confirmation would undo it.
        String sql = "DELETE FROM " + table + " WHERE message_id = ? AND state = ? AND claimed_by = ?";

        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, messageId);
            statement.setString(2, CLAIMED);
            statement.setString(3, nodeId);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new AceMqException("could not release message " + messageId + " in '" + table + "'", e);
        }
    }

    @Override
    public boolean isConfirmed(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        String sql = "SELECT expires_at FROM " + table + " WHERE message_id = ? AND state = ?";

        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, messageId);
            statement.setString(2, CONFIRMED);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return false;
                }
                OffsetDateTime expiresAt = rows.getObject(1, OffsetDateTime.class);
                // Expired rows are answered as false rather than deleted: a read that writes
                // turns every duplicate check into a write on a shared table.
                return expiresAt != null && expiresAt.toInstant().isAfter(Instant.now());
            }
        } catch (SQLException e) {
            throw new AceMqException("could not read message " + messageId + " from '" + table + "'", e);
        }
    }

    /**
     * Deletes rows nobody is bound by any more: confirmations past their retention, and claims
     * whose lease has run out.
     *
     * <p>Schedule this — hourly is ample. Nothing on the message path deletes anything, because
     * a store that tidies up on the hot path makes every message pay for it.
     *
     * @return how many rows were removed
     */
    public int purgeExpired() {
        String sql = "DELETE FROM " + table + " WHERE expires_at <= ?";
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, atUtc(Instant.now()));
            int removed = statement.executeUpdate();
            if (removed > 0) {
                log.debug("purged {} expired rows from '{}'", removed, table);
            }
            return removed;
        } catch (SQLException e) {
            throw new AceMqException("could not purge expired rows from '" + table + "'", e);
        }
    }

    /** @return how many identifiers the table currently holds, expired ones included */
    public long size() {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM " + table);
                ResultSet rows = statement.executeQuery()) {
            return rows.next() ? rows.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new AceMqException("could not count rows in '" + table + "'", e);
        }
    }

    /**
     * @param e the failure to classify
     * @return whether it means the row already existed
     */
    private static boolean isDuplicateKey(SQLException e) {
        // By SQLState rather than by vendor error code, which is what makes this work on
        // PostgreSQL, H2 and everything else without a dialect table. Class 23 covers the
        // constraint violations, and the only constraint on this table is its primary key.
        for (SQLException current = e; current != null; current = current.getNextException()) {
            String state = current.getSQLState();
            if (state != null && state.startsWith(INTEGRITY_VIOLATION)) {
                return true;
            }
        }
        return false;
    }

    private static OffsetDateTime atUtc(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static String stripComments(String sql) {
        StringBuilder cleaned = new StringBuilder();
        for (String line : sql.split("\n")) {
            if (!line.trim().startsWith("--")) {
                cleaned.append(line).append('\n');
            }
        }
        return cleaned.toString().trim();
    }

    private String readSchema() {
        try (InputStream stream = JdbcIdempotencyStore.class.getResourceAsStream("idempotency-schema.sql")) {
            if (stream == null) {
                throw new AceMqException("idempotency-schema.sql is missing from the jar");
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AceMqException("could not read idempotency-schema.sql", e);
        }
    }

    @Override
    public String toString() {
        return "JdbcIdempotencyStore{table=" + table + ", retention=" + retention
                + ", claimTimeout=" + claimTimeout + "}";
    }
}
