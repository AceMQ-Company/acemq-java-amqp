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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.acemq.amqp.api.AceMqException;
import org.acemq.amqp.api.SchemaDefinition;
import org.acemq.amqp.api.SchemaRegistry;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A schema registry in a database table.
 *
 * <p>The in-memory one is fine for a test and useless for anything else: identifiers must be
 * stable <em>forever</em>, because a message published today may be read next year by a consumer
 * looking up the schema it was written with. A registry that hands out fresh identifiers on
 * restart makes every message written before the restart unreadable, and does it silently.
 *
 * <p>This is the smallest thing that fixes that. It is not Confluent's registry and does not try
 * to be: there is no compatibility checking, no versioning UI, no REST API. What it does is
 * remember which integer stands for which schema, across restarts and across instances, which is
 * the part the wire format depends on.
 *
 * <pre>{@code
 * JdbcSchemaRegistry registry = new JdbcSchemaRegistry(dataSource);
 * registry.createSchemaIfAbsent();
 *
 * mq.publisher("orders", "order.placed", GenericRecord.class)
 *         .as(AvroCodec.registered(registry))
 *         .send(order);
 * }</pre>
 *
 * <p>Both directions are cached in memory and never invalidated, because neither answer can
 * change: an identifier stands for one schema forever, and a schema keeps the identifier it was
 * given. Without the cache this would be a database round trip per message, which is not a
 * registry anyone would keep.
 */
public final class JdbcSchemaRegistry implements SchemaRegistry {

    private static final Logger log = LoggerFactory.getLogger(JdbcSchemaRegistry.class);

    private static final String DEFAULT_TABLE = "acemq_schema_registry";

    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}");

    /** SQLState class 23: integrity constraint violation, which here means another instance won. */
    private static final String INTEGRITY_VIOLATION = "23";

    /** Two is enough to settle the fingerprint race; the rest are for a database having a day. */
    private static final int REGISTRATION_ATTEMPTS = 5;

    private final DataSource dataSource;
    private final String table;

    /** Schema fingerprint to identifier. Neither side of this mapping ever changes. */
    private final Map<String, Integer> idsByFingerprint = new ConcurrentHashMap<>();

    private final Map<Integer, SchemaDefinition> schemasById = new ConcurrentHashMap<>();

    /**
     * @param dataSource where the table lives
     */
    public JdbcSchemaRegistry(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE);
    }

    /**
     * @param dataSource where the table lives
     * @param table table name; must be a plain SQL identifier
     */
    public JdbcSchemaRegistry(DataSource dataSource, String table) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        if (!SAFE_TABLE_NAME.matcher(Objects.requireNonNull(table, "table")).matches()) {
            // A table name reaches SQL by concatenation because no database lets it be bound as
            // a parameter. Rejecting anything but a plain identifier is what keeps that safe.
            throw new IllegalArgumentException("table must be a plain SQL identifier, was '" + table + "'");
        }
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
            throw new AceMqException("could not create the schema registry table '" + table + "'", e);
        }
        seedCounter();
    }

    /**
     * Puts the single counter row there if it is not already, so the first registration has
     * something to lock rather than something to create.
     */
    private void seedCounter() {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + table + "_seq (only_row, last_id) VALUES (1, 0)")) {
            insert.executeUpdate();
        } catch (SQLException e) {
            if (e.getSQLState() != null && e.getSQLState().startsWith(INTEGRITY_VIOLATION)) {
                // Already seeded, by an earlier call or by another instance starting alongside
                // this one. Both are the normal case; the primary key is what makes it safe to
                // let everybody try.
                return;
            }
            throw new AceMqException("could not seed the schema registry counter for '" + table + "'", e);
        }
    }

    @Override
    public int idFor(SchemaDefinition schema) {
        Objects.requireNonNull(schema, "schema");
        String fingerprint = schema.fingerprint();

        Integer cached = idsByFingerprint.get(fingerprint);
        if (cached != null) {
            return cached;
        }

        // The loop is for one race: two writers registering the same schema at the same moment.
        // The unique index lets exactly one of them insert, and the loser comes back round to
        // read the winner's identifier rather than inventing a second one for the same bytes.
        // Two writers registering *different* schemas do not race at all, because the counter
        // row hands out identifiers one at a time.
        for (int attempt = 1; attempt <= REGISTRATION_ATTEMPTS; attempt++) {
            Integer existing = lookupByFingerprint(fingerprint);
            if (existing != null) {
                remember(existing, schema);
                return existing;
            }

            // The identifier is one past the highest, read and written in one transaction. A
            // sequence would be tidier and is not portable across every database this runs on.
            try (Connection connection = dataSource.getConnection()) {
                boolean autoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    int id = nextId(connection);
                    insert(connection, id, schema);
                    connection.commit();
                    remember(id, schema);
                    log.info("registered {} as schema id {}", schema.subject(), id);
                    return id;
                } catch (SQLException e) {
                    connection.rollback();
                    if (e.getSQLState() == null || !e.getSQLState().startsWith(INTEGRITY_VIOLATION)) {
                        throw new AceMqException(
                                "could not register the schema for '" + schema.subject() + "'", e);
                    }
                    log.debug("schema registration for {} collided, attempt {}", schema.subject(), attempt);
                } finally {
                    connection.setAutoCommit(autoCommit);
                }
            } catch (SQLException e) {
                throw new AceMqException("could not register the schema for '" + schema.subject() + "'", e);
            }
        }
        throw new AceMqException("could not register the schema for '" + schema.subject() + "' after "
                + REGISTRATION_ATTEMPTS + " attempts. Every insert was refused for breaking a constraint"
                + " and no matching fingerprint was there afterwards, so the table has a constraint this"
                + " registry does not know about -- check what a migration added to '" + table + "'.");
    }

    @Override
    public SchemaDefinition schemaFor(int id) {
        SchemaDefinition cached = schemasById.get(id);
        if (cached != null) {
            return cached;
        }

        try (Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement(
                        "SELECT format, subject, definition FROM " + table + " WHERE id = ?")) {
            query.setInt(1, id);
            try (ResultSet rows = query.executeQuery()) {
                if (!rows.next()) {
                    // The message names a schema this registry has never held. Usually one
                    // environment reading another's messages, which is worth saying plainly
                    // because the alternative guess -- a corrupt message -- sends people looking
                    // in the wrong place.
                    throw new AceMqException("schema id " + id + " is not in this registry. A message was"
                            + " written against a schema registered somewhere else, so either it came from"
                            + " another environment or this registry is not the one that wrote it.");
                }
                SchemaDefinition schema = new SchemaDefinition(
                        rows.getString("format"), rows.getString("subject"), rows.getString("definition"));
                remember(id, schema);
                return schema;
            }
        } catch (SQLException e) {
            throw new AceMqException("could not read schema id " + id, e);
        }
    }

    /** @return how many schemas this registry holds */
    public int size() {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM " + table)) {
            return rows.next() ? rows.getInt(1) : 0;
        } catch (SQLException e) {
            throw new AceMqException("could not count the schemas in '" + table + "'", e);
        }
    }

    private @Nullable Integer lookupByFingerprint(String fingerprint) {
        try (Connection connection = dataSource.getConnection();
                PreparedStatement query = connection.prepareStatement(
                        "SELECT id FROM " + table + " WHERE fingerprint = ?")) {
            query.setString(1, fingerprint);
            try (ResultSet rows = query.executeQuery()) {
                return rows.next() ? rows.getInt(1) : null;
            }
        } catch (SQLException e) {
            throw new AceMqException("could not look up a schema by fingerprint", e);
        }
    }

    /**
     * Takes the next identifier, holding the counter row until the caller's transaction ends.
     *
     * <p>The update is what serialises writers: everyone waits on the same row, so no two of
     * them can leave with the same number. Identifiers start at 1, because zero is what a
     * caller reads from an {@code int} nobody set — leaving it unused makes "schema 0" always
     * a bug rather than sometimes a real schema.
     */
    private int nextId(Connection connection) throws SQLException {
        try (Statement bump = connection.createStatement()) {
            int updated = bump.executeUpdate(
                    "UPDATE " + table + "_seq SET last_id = last_id + 1 WHERE only_row = 1");
            if (updated != 1) {
                throw new AceMqException("the counter row for '" + table + "' is missing. Either"
                        + " createSchemaIfAbsent() has not run, or a migration created the registry table"
                        + " without " + table + "_seq alongside it.");
            }
        }
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(
                        "SELECT last_id FROM " + table + "_seq WHERE only_row = 1")) {
            if (!rows.next()) {
                throw new AceMqException("the counter row for '" + table + "' vanished mid-transaction");
            }
            return rows.getInt(1);
        }
    }

    private void insert(Connection connection, int id, SchemaDefinition schema) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement("INSERT INTO " + table
                + " (id, fingerprint, format, subject, definition, registered_at) VALUES (?, ?, ?, ?, ?, ?)")) {
            insert.setInt(1, id);
            insert.setString(2, schema.fingerprint());
            insert.setString(3, schema.format());
            insert.setString(4, schema.subject());
            insert.setString(5, schema.definition());
            insert.setObject(6, Instant.now().atOffset(ZoneOffset.UTC));
            insert.executeUpdate();
        }
    }

    private void remember(int id, SchemaDefinition schema) {
        idsByFingerprint.put(schema.fingerprint(), id);
        schemasById.put(id, schema);
    }

    private String readSchema() {
        try (InputStream stream = JdbcSchemaRegistry.class.getResourceAsStream("schema-registry-schema.sql")) {
            if (stream == null) {
                throw new AceMqException("schema-registry-schema.sql is missing from the jar");
            }
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AceMqException("could not read schema-registry-schema.sql", e);
        }
    }

    private static String stripComments(String sql) {
        // Before splitting on semicolons, not after: a semicolon inside a comment would
        // otherwise hand the remains of an English sentence to the database as SQL.
        StringBuilder cleaned = new StringBuilder();
        for (String line : sql.split("\n")) {
            if (!line.trim().startsWith("--")) {
                cleaned.append(line).append('\n');
            }
        }
        return cleaned.toString().trim();
    }

    @Override
    public String toString() {
        return "JdbcSchemaRegistry{table=" + table + "}";
    }
}
