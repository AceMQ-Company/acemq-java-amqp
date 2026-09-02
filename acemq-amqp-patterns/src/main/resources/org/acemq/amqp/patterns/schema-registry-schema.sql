-- Shared schema registry, portable across PostgreSQL and H2.
--
-- Shipped for development and tests. In production this table belongs in whatever
-- migration tool already owns the schema, for the same reason as the other two: a
-- library creating tables at start-up decides when your database changes.
--
-- The table name is substituted, so it is validated before it reaches here.

CREATE TABLE IF NOT EXISTS ${table} (
    id           INTEGER      NOT NULL PRIMARY KEY,
    fingerprint  VARCHAR(64)  NOT NULL,
    format       VARCHAR(32)  NOT NULL,
    subject      VARCHAR(255) NOT NULL,
    definition   CLOB         NOT NULL,
    registered_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- The fingerprint is what makes registration idempotent: the same schema offered
-- twice must come back with the same id, from any instance, forever. Unique rather
-- than merely indexed, so two instances racing to register the same schema end with
-- one row and one loser that re-reads instead of a second id for the same bytes.
CREATE UNIQUE INDEX IF NOT EXISTS ${table}_fingerprint ON ${table} (fingerprint);

-- One row, holding the last identifier handed out.
--
-- A sequence would be the obvious tool and is spelled differently on every database
-- this has to run on. Taking the highest id and adding one needs no sequence and is
-- wrong under load: two writers registering two different schemas at the same moment
-- compute the same next id, and one of them loses a race it cannot win by retrying,
-- because the writer it lost to is doing the same arithmetic. Updating this row takes
-- a row lock, so writers queue for an instant and every one of them gets a number.
--
-- Registration is rare -- once per schema in the lifetime of a system -- so serialising
-- it costs nothing worth measuring.
CREATE TABLE IF NOT EXISTS ${table}_seq (
    only_row INTEGER NOT NULL PRIMARY KEY,
    last_id  INTEGER NOT NULL
);
