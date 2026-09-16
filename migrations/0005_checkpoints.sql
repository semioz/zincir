DROP INDEX idx_events_idem;
ALTER TABLE events RENAME TO events_v1;

CREATE TABLE events (
    id              INTEGER PRIMARY KEY,
    run_id          BLOB    NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    event_type      TEXT    NOT NULL
        CHECK (event_type IN (
            'llm_call', 'tool_call', 'tool_result', 'state_transition',
            'checkpoint_proposed', 'checkpoint_accepted', 'checkpoint_rejected'
        )),
    payload         TEXT    NOT NULL,
    idempotency_key TEXT    NULL,
    created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(run_id, seq)
);

INSERT INTO events (id, run_id, seq, event_type, payload, idempotency_key, created_at)
SELECT id, run_id, seq, event_type, payload, idempotency_key, created_at FROM events_v1;
DROP TABLE events_v1;

CREATE UNIQUE INDEX idx_events_idem ON events(run_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE checkpoints (
    id                  INTEGER PRIMARY KEY,
    run_id              BLOB    NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    round               INTEGER NOT NULL,
    tool_call_id        TEXT    NOT NULL,
    based_on_event_seq  INTEGER NOT NULL,
    decision_event_seq  INTEGER NULL,
    status              TEXT    NOT NULL CHECK (status IN ('candidate', 'accepted', 'rejected')),
    state               TEXT    NOT NULL,
    verification        TEXT    NULL,
    created_at          TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    decided_at          TEXT    NULL,
    UNIQUE(run_id, round),
    UNIQUE(run_id, tool_call_id)
);

CREATE INDEX idx_checkpoints_status ON checkpoints(run_id, status, round);
