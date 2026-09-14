ALTER TABLE conversations ADD COLUMN archived_at timestamptz;
CREATE INDEX conversations_archived ON conversations(workspace_id,archived_at DESC,id DESC) WHERE archived_at IS NOT NULL;
CREATE INDEX conversation_runs_order ON agent_runs(workspace_id,conversation_id,created_at DESC,id DESC);
