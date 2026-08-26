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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.OutboxRecord;
import org.acemq.amqp.api.OutboxStore;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An outbox kept in a relational table, in the same database as the work it accompanies.
 *
 * <p>Two sources of connections, because the two halves of this class run in different worlds.
 * Writing a record belongs to the caller's transaction and borrows its connection through a
 * {@link ConnectionSupplier}; the connection is used and left open, since the transaction that
 * owns it is still running. Everything the relay does — claiming, marking, counting — happens on
 * a background thread with no ambient transaction, so it takes its own connection from a
 * {@link DataSource} and closes it. Collapsing the two into one source is the mistake this
 * separation exists to make impossible: a store that inserted on its own auto-committing
 * connection would give up the atomicity that is the entire point.
 *
 * <p>Claiming is done with a lease rather than {@code SELECT FOR UPDATE}. A held lock lasts as
 * long as its transaction, which means a relay that dies mid-batch either strands its rows until
 * the database notices the connection has gone or holds a transaction open across a network
 * publish, and neither is acceptable. A lease is a timestamp: it expires on its own, no matter
 * how the holder died, and the rows return to circulation without anyone intervening.
 *
 * <p>Claiming happens in two statements, and the second is what makes it safe. Candidate rows are
 * selected, then each is updated with the lease still conditional on being unclaimed. Two relays
 * that select the same candidates will both try to update them; row locking serialises the
 * updates, the loser re-evaluates its condition against the committed row, and matches nothing.
 * The claim is therefore decided by the update's row count, never by the select.
 */
public final class JdbcOutboxStore implements OutboxStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcOutboxStore.class);

    /** Table and column identifiers cannot be bound as parameters, so the name is validated. */
    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    private static final String DEFAULT_TABLE = "acemq_outbox";
    private static final int DEFAULT_MAX_ATTEMPTS = 10;
    private static final int MAX_BATCH_SIZE = 10_000;

    private static final String COLUMNS = "id, exchange_name, routing_key, message_type, payload, correlation_id, causation_id, created_at,"
            + " attempts, last_error";

    private final ConnectionSupplier transactionalConnection;
    private final DataSource relayConnections;
    private final String table;
    private final int maxAttempts;

    /**
     * @param transactionalConnection where {@link #add} gets the caller's transaction from; see
     *     {@link ConnectionSupplier} for what this must and must not be
     * @param relayConnections connections for the relay's own work, taken and closed per call
     */
    public JdbcOutboxStore(ConnectionSupplier transactionalConnection, DataSource relayConnections) {
        this(transactionalConnection, relayConnections, DEFAULT_TABLE, DEFAULT_MAX_ATTEMPTS);
    }

    /**
     * @param transactionalConnection where {@link #add} gets the caller's transaction from
     * @param relayConnections connections for the relay's own work
     * @param table table holding the outbox
     * @param maxAttempts how many failures a record may accumulate before it stops being claimed
     *     and stays for someone to look at
     */
    public JdbcOutboxStore(
            ConnectionSupplier transactionalConnection, DataSource relayConnections, String table, int maxAttempts) {
        this.transactionalConnection = Objects.requireNonNull(transactionalConnection, "transactionalConnection");
        this.relayConnections = Objects.requireNonNull(relayConnections, "relayConnections");
        if (!SAFE_TABLE_NAME.matcher(Objects.requireNonNull(table, "table")).matches()) {
            // A table name reaches SQL by concatenation because no database lets it be bound as a
            // parameter. Rejecting anything but a plain identifier is what keeps that safe.
            throw new IllegalArgumentException(
                    "table must be a plain SQL identifier, was '" + table + "'");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1, was " + maxAttempts);
        }
        this.table = table;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Creates the outbox table if it is not already there.
     *
     * <p>For development and tests. In production the table belongs in the migration tool that
     * owns the rest of the schema: a library that creates tables at start-up has taken a decision
     * about when your database changes that is not its to take.
     *
     * @throws AceMqException if the schema cannot be created
     */
    public void createSchemaIfAbsent() {
        // Comments come out before the file is split, not after. A semicolon inside a comment
        // would otherwise end a statement that had not started, and the remains of an English
        // sentence would be handed to the database as SQL.
        String ddl = stripComments(readSchema()).replace("${table}", table);
        try (Connection connection = relayConnections.getConnection();
                Statement statement = connection.createStatement()) {
            for (String piece : ddl.split(";")) {
                String sql = piece.trim();
                if (!sql.isEmpty()) {
                    statement.execute(sql);
                }
            }
        } catch (SQLException e) {
            throw new AceMqException("could not create the outbox table '" + table + "'", e);
        }
    }

    @Override
    public void add(OutboxRecord record) {
        Objects.requireNonNull(record, "record");
        String sql = "INSERT INTO " + table + " (" + COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Connection connection;
        try {
            connection = transactionalConnection.get();
        } catch (SQLException e) {
            throw new AceMqException("no transactional connection is available to write the outbox record", e);
        }
        if (connection == null) {
            throw new AceMqException("the connection supplier returned null; the outbox needs the caller's"
                    + " transaction to write into");
        }

        // Deliberately not closed and not committed. The connection belongs to the caller's
        // transaction, and this insert becomes durable exactly when that transaction does — which
        // is the guarantee the whole pattern rests on.
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, record.id());
            statement.setString(2, record.exchange());
            statement.setString(3, record.routingKey());
            statement.setString(4, record.type());
            statement.setString(5, record.payload());
            statement.setString(6, record.correlationId().orElse(null));
            statement.setString(7, record.causationId().orElse(null));
            statement.setObject(8, atUtc(record.createdAt()));
            statement.setInt(9, record.attempts());
            statement.setString(10, record.lastError().orElse(null));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new AceMqException("could not write outbox record " + record.id(), e);
        }
    }

    @Override
    public List<OutboxRecord> claimBatch(int batchSize, Duration lease) {
        if (batchSize < 1 || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException("batchSize must be between 1 and " + MAX_BATCH_SIZE + ", was "
                    + batchSize);
        }
        if (lease.isNegative() || lease.isZero()) {
            throw new IllegalArgumentException("lease must be positive, was " + lease);
        }

        String claimToken = UUID.randomUUID().toString();
        Instant now = Instant.now();

        try (Connection connection = relayConnections.getConnection()) {
            List<String> candidates = selectCandidates(connection, batchSize, now);
            if (candidates.isEmpty()) {
                return Collections.emptyList();
            }
            int claimed = takeLease(connection, candidates, claimToken, now.plus(lease), now);
            if (claimed == 0) {
                // Every candidate went to another relay between the select and the update. Not an
                // error, and not worth a second round trip: the next poll will find more.
                return Collections.emptyList();
            }
            return readClaimed(connection, claimToken);
        } catch (SQLException e) {
            throw new AceMqException("could not claim a batch from the outbox table '" + table + "'", e);
        }
    }

    private List<String> selectCandidates(Connection connection, int batchSize, Instant now) throws SQLException {
        // batchSize is bounded and integral, so interpolating it cannot carry anything but a
        // number; no database accepts a parameter in FETCH FIRST portably.
        String sql = "SELECT id FROM " + table
                + " WHERE published_at IS NULL AND attempts < ? AND (locked_until IS NULL OR locked_until < ?)"
                + " ORDER BY created_at, id FETCH FIRST " + batchSize + " ROWS ONLY";
        List<String> ids = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, maxAttempts);
            statement.setObject(2, atUtc(now));
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    ids.add(rows.getString(1));
                }
            }
        }
        return ids;
    }

    private int takeLease(Connection connection, List<String> ids, String token, Instant until, Instant now)
            throws SQLException {
        String sql = "UPDATE " + table + " SET locked_by = ?, locked_until = ?"
                + " WHERE id = ? AND published_at IS NULL AND (locked_until IS NULL OR locked_until < ?)";
        int claimed = 0;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (String id : ids) {
                statement.setString(1, token);
                statement.setObject(2, atUtc(until));
                statement.setString(3, id);
                statement.setObject(4, atUtc(now));
                claimed += statement.executeUpdate();
            }
        }
        return claimed;
    }

    private List<OutboxRecord> readClaimed(Connection connection, String token) throws SQLException {
        String sql = "SELECT " + COLUMNS + " FROM " + table
                + " WHERE locked_by = ? AND published_at IS NULL ORDER BY created_at, id";
        List<OutboxRecord> records = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, token);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    records.add(readRecord(rows));
                }
            }
        }
        return records;
    }

    private static OutboxRecord readRecord(ResultSet rows) throws SQLException {
        OffsetDateTime createdAt = rows.getObject(8, OffsetDateTime.class);
        return new OutboxRecord(
                rows.getString(1),
                rows.getString(2),
                rows.getString(3),
                rows.getString(4),
                rows.getString(5),
                rows.getString(6),
                rows.getString(7),
                createdAt == null ? Instant.now() : createdAt.toInstant(),
                rows.getInt(9),
                rows.getString(10));
    }

    @Override
    public void markPublished(String id) {
        // The lease is cleared along with the mark so that the row reads plainly afterwards: a
        // published row still showing a lock confuses anyone reading the table during an incident.
        String sql = "UPDATE " + table + " SET published_at = ?, locked_by = NULL, locked_until = NULL WHERE id = ?";
        try (Connection connection = relayConnections.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, atUtc(Instant.now()));
            statement.setString(2, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new AceMqException("could not mark outbox record " + id + " as published", e);
        }
    }

    @Override
    public void markFailed(String id, String reason) {
        String sql = "UPDATE " + table
                + " SET attempts = attempts + 1, last_error = ?, locked_by = NULL, locked_until = NULL WHERE id = ?";
        try (Connection connection = relayConnections.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, truncate(reason));
            statement.setString(2, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            // Swallowed on purpose. This is already the failure path, and a relay that dies here
            // would leave the record leased rather than free; letting the lease expire is the
            // gentler outcome, and the message is still in the table either way.
            log.warn("could not record the failure of outbox record {}: {}", id, e.getMessage());
        }
    }

    @Override
    public long pendingCount() {
        String sql = "SELECT COUNT(*) FROM " + table + " WHERE published_at IS NULL";
        try (Connection connection = relayConnections.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet rows = statement.executeQuery()) {
            return rows.next() ? rows.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new AceMqException("could not count pending outbox records", e);
        }
    }

    /**
     * Removes published records older than the given age.
     *
     * <p>Nothing deletes them otherwise, and an outbox table that only grows eventually makes the
     * relay's own query slow. How long to keep them is a judgement about auditing rather than
     * about messaging, so it is the caller's to make.
     *
     * @param olderThan how long a published record is kept
     * @return how many records were removed
     */
    public int purgePublished(Duration olderThan) {
        String sql = "DELETE FROM " + table + " WHERE published_at IS NOT NULL AND published_at < ?";
        try (Connection connection = relayConnections.getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, atUtc(Instant.now().minus(olderThan)));
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new AceMqException("could not purge published outbox records", e);
        }
    }

    private static OffsetDateTime atUtc(Instant instant) {
        // Stored with an explicit offset rather than as a naive timestamp: a relay and a database
        // in different zones is common, and a naive column makes that difference invisible until
        // it produces an hour of messages nobody claims.
        return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static @Nullable String truncate(@Nullable String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 1000 ? reason : reason.substring(0, 997) + "...";
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
        try (InputStream stream = JdbcOutboxStore.class.getResourceAsStream("outbox-schema.sql")) {
            if (stream == null) {
                throw new AceMqException("outbox-schema.sql is missing from the jar");
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AceMqException("could not read outbox-schema.sql", e);
        }
    }

    @Override
    public String toString() {
        return "JdbcOutboxStore{table=" + table + ", maxAttempts=" + maxAttempts + "}";
    }
}
