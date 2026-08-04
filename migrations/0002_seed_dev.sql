-- seed a supervisor run for local dev. Delete or replace before real use.
INSERT INTO agent_runs (id, parent_run_id, role, status, provider, config)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    NULL,
    'supervisor',
    'pending',
    'claude',
    '{"model": "claude-sonnet-4-5", "temperature": 0}"::jsonb
);
