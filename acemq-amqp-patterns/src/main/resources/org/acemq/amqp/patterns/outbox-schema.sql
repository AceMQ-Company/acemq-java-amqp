-- Outbox schema, portable across PostgreSQL and H2.
--
-- Shipped for development and tests. In production this table belongs in whatever
-- migration tool already owns the schema, alongside the business tables it commits
-- with; a library creating tables at start-up is a library deciding when your
-- database changes, which is not its decision to make.
--
-- The table name is substituted, so it is validated before it reaches here.

CREATE TABLE IF NOT EXISTS ${table} (
    id             VARCHAR(64)              NOT NULL PRIMARY KEY,
    exchange_name  VARCHAR(255)             NOT NULL,
    routing_key    VARCHAR(255)             NOT NULL,
    message_type   VARCHAR(255)             NOT NULL,
    payload        TEXT                     NOT NULL,
    correlation_id VARCHAR(64),
    causation_id   VARCHAR(64),
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    published_at   TIMESTAMP WITH TIME ZONE,
    attempts       INTEGER                  NOT NULL DEFAULT 0,
    last_error     VARCHAR(1000),
    locked_by      VARCHAR(64),
    locked_until   TIMESTAMP WITH TIME ZONE
);

-- The relay's only query is "oldest unpublished first", so that is what is indexed.
CREATE INDEX IF NOT EXISTS ${table}_pending ON ${table} (published_at, created_at);
