-- Stable agent identities are separate from individual chat runs.
CREATE TABLE scoped_agents (
    workspace_id text NOT NULL REFERENCES workspaces(id),
    role text NOT NULL CHECK (role IN ('sales','inventory')),
    enabled boolean NOT NULL DEFAULT true,
    checked_revision bigint NOT NULL DEFAULT -1,
    last_checked_at timestamptz,
    observation jsonb,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id,role)
);
CREATE TABLE agent_data_revisions (
    workspace_id text NOT NULL REFERENCES workspaces(id),
    role text NOT NULL CHECK (role IN ('sales','inventory')),
    revision bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (workspace_id,role)
);
CREATE TABLE agent_monitor_instances (
    id uuid PRIMARY KEY,
    workspace_id text NOT NULL REFERENCES workspaces(id),
    heartbeat_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX agent_monitor_workspace ON agent_monitor_instances(workspace_id,heartbeat_at);
CREATE TABLE scoped_agent_checks (
    id uuid PRIMARY KEY,
    workspace_id text NOT NULL,
    role text NOT NULL,
    revision bigint NOT NULL,
    observation jsonb NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (workspace_id,role) REFERENCES scoped_agents(workspace_id,role),
    UNIQUE (workspace_id,role,revision)
);

INSERT INTO scoped_agents(workspace_id,role)
SELECT w.id,r.role FROM workspaces w CROSS JOIN (VALUES ('sales'),('inventory')) r(role);
INSERT INTO agent_data_revisions(workspace_id,role)
SELECT workspace_id,role FROM scoped_agents;

CREATE FUNCTION initialize_scoped_agents() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO scoped_agents(workspace_id,role) VALUES(NEW.id,'sales'),(NEW.id,'inventory');
    INSERT INTO agent_data_revisions(workspace_id,role) VALUES(NEW.id,'sales'),(NEW.id,'inventory');
    RETURN NEW;
END;
$$;
CREATE TRIGGER workspace_agents AFTER INSERT ON workspaces FOR EACH ROW EXECUTE FUNCTION initialize_scoped_agents();

-- One revision per affected workspace per statement, including bulk imports.
-- Revisions commit atomically with data; rollbacks cannot wake an agent.
CREATE FUNCTION mark_agent_data_changed() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE affected text[];
BEGIN
    IF TG_OP = 'INSERT' THEN
        SELECT array_agg(DISTINCT workspace_id) INTO affected FROM new_rows;
    ELSIF TG_OP = 'DELETE' THEN
        SELECT array_agg(DISTINCT workspace_id) INTO affected FROM old_rows;
    ELSE
        SELECT array_agg(workspace_id) INTO affected FROM (
            SELECT workspace_id FROM old_rows UNION SELECT workspace_id FROM new_rows
        ) w;
    END IF;
    UPDATE agent_data_revisions SET revision=revision+1
    WHERE workspace_id=ANY(affected) AND role=ANY(string_to_array(TG_ARGV[0],','));
    RETURN NULL;
END;
$$;
DO $$
DECLARE entry record;
BEGIN
    FOR entry IN SELECT * FROM (VALUES
        ('inventory_items','inventory'),('inventory_movements','inventory'),
        ('sales_tickets','sales'),('sales_lines','sales'),('dataset_imports','sales,inventory')
    ) AS t(table_name,roles) LOOP
        EXECUTE format('CREATE TRIGGER agent_change_insert AFTER INSERT ON %I REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION mark_agent_data_changed(%L)',entry.table_name,entry.roles);
        EXECUTE format('CREATE TRIGGER agent_change_update AFTER UPDATE ON %I REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION mark_agent_data_changed(%L)',entry.table_name,entry.roles);
        EXECUTE format('CREATE TRIGGER agent_change_delete AFTER DELETE ON %I REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION mark_agent_data_changed(%L)',entry.table_name,entry.roles);
    END LOOP;
END;
$$;
