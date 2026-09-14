ALTER TABLE agent_runs DROP CONSTRAINT agent_runs_kind_check;
ALTER TABLE agent_runs ADD CONSTRAINT agent_runs_kind_check CHECK (kind IN ('chat','inventory_review','vendor_research','supplier_reply'));
ALTER TABLE vendors DROP CONSTRAINT vendors_source_check;
ALTER TABLE vendors ADD CONSTRAINT vendors_source_check CHECK (source IN ('snapshot','local_fixture','manual','web_research'));
ALTER TABLE vendors ADD COLUMN evidence jsonb NOT NULL DEFAULT '[]';
ALTER TABLE vendors ADD COLUMN website text;
CREATE TABLE business_settings (
 workspace_id text PRIMARY KEY REFERENCES workspaces(id),
 business_name text NOT NULL DEFAULT 'Backhaus', city text NOT NULL DEFAULT '', country text NOT NULL DEFAULT '',
 reply_to text NOT NULL DEFAULT '', auto_reply boolean NOT NULL DEFAULT true,
 version integer NOT NULL DEFAULT 1, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE vendor_searches (
 id uuid PRIMARY KEY, workspace_id text NOT NULL REFERENCES workspaces(id), run_id uuid NOT NULL REFERENCES agent_runs(id) ON DELETE CASCADE,
 query text NOT NULL, sources jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE supplier_threads (
 id uuid PRIMARY KEY, workspace_id text NOT NULL, vendor_id uuid NOT NULL, item_ids jsonb NOT NULL,
 subject text NOT NULL, initial_body text NOT NULL, status text NOT NULL DEFAULT 'draft',
 reply_to text NOT NULL DEFAULT '', created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(workspace_id,vendor_id) REFERENCES vendors(workspace_id,id) ON DELETE CASCADE
);
CREATE TABLE supplier_messages (
 id uuid PRIMARY KEY, thread_id uuid NOT NULL REFERENCES supplier_threads(id) ON DELETE CASCADE,
 workspace_id text NOT NULL REFERENCES workspaces(id), direction text NOT NULL CHECK(direction IN ('inbound','outbound')),
 channel text NOT NULL CHECK(channel IN ('email','whatsapp')), body text NOT NULL, subject text NOT NULL,
 status text NOT NULL, provider_id text UNIQUE, internet_id text, reply_to_id text,
 review text, error text, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 sent_at timestamptz, received_at timestamptz, source_message_id uuid UNIQUE REFERENCES supplier_messages(id)
);
CREATE INDEX supplier_threads_recent ON supplier_threads(workspace_id,updated_at DESC);
CREATE INDEX supplier_messages_pending ON supplier_messages(workspace_id,status,created_at);
CREATE INDEX vendor_searches_run ON vendor_searches(workspace_id,run_id);
UPDATE scoped_agents SET enabled=false,updated_at=now() WHERE role='sales';
UPDATE purchasing_policies SET auto_approve_limit=0,version=version+1,updated_at=now();
ALTER TABLE supplier_messages ADD COLUMN review_run_id uuid REFERENCES agent_runs(id) ON DELETE SET NULL;
ALTER TABLE supplier_messages ADD COLUMN send_payload jsonb;
ALTER TABLE supplier_messages ADD COLUMN first_attempt_at timestamptz;
ALTER TABLE supplier_threads ADD COLUMN recipient_email text;
ALTER TABLE supplier_threads ADD COLUMN recipient_phone text;
ALTER TABLE vendors ADD COLUMN vetting jsonb NOT NULL DEFAULT '{}';
ALTER TABLE vendors ADD COLUMN research_status text NOT NULL DEFAULT 'unreviewed' CHECK (research_status IN ('unreviewed','shortlisted','discarded'));
CREATE OR REPLACE FUNCTION initialize_scoped_agents() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO scoped_agents(workspace_id,role,enabled) VALUES(NEW.id,'sales',false),(NEW.id,'inventory',true);
 INSERT INTO agent_data_revisions(workspace_id,role) VALUES(NEW.id,'sales'),(NEW.id,'inventory');
 RETURN NEW;
END;
$$;
