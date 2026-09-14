-- Dismiss a research activity without deleting its evidence or saved suppliers.
ALTER TABLE agent_runs ADD COLUMN dismissed_at timestamptz;
