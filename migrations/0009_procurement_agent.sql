ALTER TABLE scoped_agents DROP CONSTRAINT scoped_agents_role_check;
ALTER TABLE scoped_agents ADD CONSTRAINT scoped_agents_role_check CHECK (role IN ('sales','inventory','procurement'));
ALTER TABLE agent_data_revisions DROP CONSTRAINT agent_data_revisions_role_check;
ALTER TABLE agent_data_revisions ADD CONSTRAINT agent_data_revisions_role_check CHECK (role IN ('sales','inventory','procurement'));
INSERT INTO scoped_agents(workspace_id,role,checked_revision) SELECT id,'procurement',0 FROM workspaces;
INSERT INTO agent_data_revisions(workspace_id,role) SELECT id,'procurement' FROM workspaces;
CREATE OR REPLACE FUNCTION initialize_scoped_agents() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO scoped_agents(workspace_id,role,enabled,checked_revision) VALUES(NEW.id,'sales',false,-1),(NEW.id,'inventory',true,-1),(NEW.id,'procurement',true,0);
 INSERT INTO agent_data_revisions(workspace_id,role) VALUES(NEW.id,'sales'),(NEW.id,'inventory'),(NEW.id,'procurement');
 RETURN NEW;
END;
$$;
