-- Durable one-shot timers. wake_at_ms is an absolute Unix timestamp in milliseconds.
CREATE TABLE timers (
    run_id BLOB NOT NULL REFERENCES agent_runs (id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 255),
    wake_at_ms INTEGER NOT NULL,
    completed_at TEXT NULL,
    PRIMARY KEY (run_id, name)
);
