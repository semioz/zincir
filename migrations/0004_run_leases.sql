ALTER TABLE agent_runs ADD COLUMN lease_owner BLOB NULL;
ALTER TABLE agent_runs ADD COLUMN lease_epoch INTEGER NOT NULL DEFAULT 0;
ALTER TABLE agent_runs ADD COLUMN lease_expires_at_ms INTEGER NULL;
