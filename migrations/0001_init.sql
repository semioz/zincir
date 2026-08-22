-- zincir v0.1 schema for a single-machine SQLite runtime.
-- WAL and busy_timeout are enabled by the Rust connection configuration.

CREATE TABLE agent_runs (
    id              BLOB    PRIMARY KEY,
    parent_run_id   BLOB    NULL REFERENCES agent_runs(id),
    role            TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    provider        TEXT    NOT NULL,
    config          TEXT    NOT NULL DEFAULT '{}',
    created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_agent_runs_parent ON agent_runs(parent_run_id);
CREATE INDEX idx_agent_runs_status ON agent_runs(status, created_at)
    WHERE status IN ('pending', 'running');

CREATE TABLE events (
    id              INTEGER PRIMARY KEY,
    run_id          BLOB    NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    event_type      TEXT    NOT NULL
        CHECK (event_type IN ('llm_call', 'tool_call', 'tool_result', 'state_transition')),
    payload         TEXT    NOT NULL,
    idempotency_key TEXT    NULL,
    created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, seq)
);

CREATE UNIQUE INDEX idx_events_idem ON events(run_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Reserved for durable multi-agent coordination.
CREATE TABLE messages (
    id              INTEGER PRIMARY KEY,
    from_run_id     BLOB    NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    to_run_id       BLOB    NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    payload         TEXT    NOT NULL,
    delivered       INTEGER NOT NULL DEFAULT 0 CHECK (delivered IN (0, 1)),
    created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_messages_undelivered ON messages(to_run_id, delivered)
    WHERE delivered = 0;
