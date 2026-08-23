-- Durable named workflow steps. A completed result is reused on resume.
CREATE TABLE steps (
    run_id BLOB NOT NULL REFERENCES agent_runs (id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 255),
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed')),
    owner_id BLOB NULL,
    result TEXT NULL,
    started_at TEXT NOT NULL DEFAULT current_timestamp,
    completed_at TEXT NULL,
    PRIMARY KEY (run_id, name)
);
