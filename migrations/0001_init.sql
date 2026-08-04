-- zincir v0.1 schema
-- Three tables: agent_runs (actors), events (replay log), messages (inter-agent).

-- Extension for gen_random_uuid() if not already enabled.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================================
-- agent_runs: one row per agent instance (an "actor").
-- ============================================================================
CREATE TABLE agent_runs (
    id              UUID        PRIMARY KEY,
    parent_run_id   UUID        NULL REFERENCES agent_runs(id),
    role            TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    provider        TEXT        NOT NULL,
    config          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- "all children of supervisor X" is the hot fan-out query.
CREATE INDEX idx_agent_runs_parent ON agent_runs(parent_run_id);

-- Find in-flight runs on resume: status IN (running, pending) ordered by age.
CREATE INDEX idx_agent_runs_status ON agent_runs(status, created_at)
    WHERE status IN ('pending', 'running');

-- ============================================================================
-- events: append-only replay log per agent. The source of truth.
--
-- tool_call and tool_result are SEPARATE rows. The gap between them is the
-- crash window; that gap is the whole point of this project. On resume:
--   - tool_call row with no matching tool_result  -> maybe-ran, re-check
--     idempotency_key before re-issuing.
--   - tool_call row WITH tool_result              -> skip, already done.
-- ============================================================================
CREATE TABLE events (
    id              BIGSERIAL   PRIMARY KEY,
    run_id          UUID        NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    seq             INT         NOT NULL,
    event_type      TEXT        NOT NULL
        CHECK (event_type IN ('llm_call', 'tool_call', 'tool_result', 'state_transition')),
    payload         JSONB       NOT NULL,
    idempotency_key TEXT        NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(run_id, seq)
);

-- Unique constraint above already indexes (run_id, seq), the replay order.
-- idempotency_key must be unique within a run: one intent, one issuance.
CREATE UNIQUE INDEX idx_events_idem ON events(run_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- ============================================================================
-- messages: durable inter-agent coordination. No direct agent-to-agent calls.
-- The table is the source of truth; LISTEN/NOTIFY is only a wakeup hint.
-- Channel name derived from to_run_id: 'agent_' || to_run_id::text.
-- ============================================================================
CREATE TABLE messages (
    id              BIGSERIAL   PRIMARY KEY,
    from_run_id     UUID        NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    to_run_id       UUID        NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
    payload         JSONB       NOT NULL,
    delivered       BOOLEAN     NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- "undelivered messages for this agent" is the wakeup query.
CREATE INDEX idx_messages_undelivered ON messages(to_run_id, delivered)
    WHERE delivered = false;
