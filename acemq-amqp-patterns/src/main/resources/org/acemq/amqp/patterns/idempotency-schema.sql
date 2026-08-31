-- Shared idempotency schema, portable across PostgreSQL and H2.
--
-- Shipped for development and tests. In production this table belongs in whatever
-- migration tool already owns the schema; a library creating tables at start-up is
-- a library deciding when your database changes, which is not its decision to make.
--
-- The table name is substituted, so it is validated before it reaches here.

CREATE TABLE IF NOT EXISTS ${table} (
    message_id  VARCHAR(255)             NOT NULL PRIMARY KEY,
    state       VARCHAR(16)              NOT NULL,
    claimed_by  VARCHAR(64),
    recorded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    expires_at  TIMESTAMP WITH TIME ZONE NOT NULL
);

-- One column carries both deadlines because they are never both meaningful: while a
-- row is CLAIMED it holds the lease expiry, and once CONFIRMED it holds the retention
-- expiry. Both are answered by the same question -- "is this row still binding?" --
-- so one index serves the claim path and the purge.
CREATE INDEX IF NOT EXISTS ${table}_expiry ON ${table} (expires_at);
