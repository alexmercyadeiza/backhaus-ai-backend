CREATE TABLE workspaces (
    id text PRIMARY KEY,
    name text NOT NULL,
    currency text NOT NULL DEFAULT 'NGN' CHECK (currency = 'NGN'),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE dataset_imports (
    id uuid PRIMARY KEY,
    workspace_id text NOT NULL REFERENCES workspaces(id),
    sha256 text NOT NULL,
    metadata jsonb NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id, sha256)
);
CREATE TABLE inventory_items (
    workspace_id text NOT NULL REFERENCES workspaces(id),
    id text NOT NULL,
    name text NOT NULL,
    category text,
    unit text,
    par_level numeric,
    current_balance numeric NOT NULL,
    unit_cost numeric,
    supplier text,
    needs_review boolean NOT NULL DEFAULT false,
    source jsonb NOT NULL,
    PRIMARY KEY (workspace_id, id)
);
CREATE TABLE inventory_movements (
    workspace_id text NOT NULL,
    id text NOT NULL,
    item_id text NOT NULL,
    business_date date NOT NULL,
    movement_type text NOT NULL,
    quantity_delta numeric NOT NULL,
    source jsonb NOT NULL,
    PRIMARY KEY (workspace_id, id),
    FOREIGN KEY (workspace_id, item_id) REFERENCES inventory_items(workspace_id, id)
);
CREATE TABLE sales_tickets (
    workspace_id text NOT NULL REFERENCES workspaces(id),
    id bigint NOT NULL,
    ticket_number text NOT NULL,
    business_date date NOT NULL,
    total_amount numeric NOT NULL,
    source jsonb NOT NULL,
    PRIMARY KEY (workspace_id, id)
);
CREATE INDEX sales_tickets_date ON sales_tickets (workspace_id, business_date);
CREATE TABLE sales_lines (
    workspace_id text NOT NULL,
    id bigint NOT NULL,
    ticket_id bigint NOT NULL,
    item_name text NOT NULL,
    category text,
    quantity numeric NOT NULL,
    unit_price numeric NOT NULL,
    billable boolean NOT NULL,
    source jsonb NOT NULL,
    PRIMARY KEY (workspace_id, id),
    FOREIGN KEY (workspace_id, ticket_id) REFERENCES sales_tickets(workspace_id,id)
);
CREATE INDEX sales_lines_ticket ON sales_lines(workspace_id,ticket_id);
CREATE TABLE conversations (
    id uuid PRIMARY KEY,
    workspace_id text NOT NULL REFERENCES workspaces(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id,id)
);
CREATE TABLE agent_runs (
    id uuid PRIMARY KEY,
    workspace_id text NOT NULL REFERENCES workspaces(id),
    conversation_id uuid NOT NULL,
    request_key text NOT NULL,
    input jsonb NOT NULL,
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','paused','completed','failed','cancelled')),
    attempt integer NOT NULL DEFAULT 0,
    max_attempts integer NOT NULL DEFAULT 3,
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_until timestamptz,
    result jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (workspace_id,conversation_id) REFERENCES conversations(workspace_id,id),
    UNIQUE (workspace_id,request_key)
);
CREATE INDEX agent_runs_queue ON agent_runs(created_at) WHERE status='queued';
CREATE TABLE agent_events (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES agent_runs(id),
    event_type text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX agent_events_run ON agent_events(run_id,id);
CREATE TABLE artifacts (
    id uuid PRIMARY KEY,
    workspace_id text NOT NULL REFERENCES workspaces(id),
    run_id uuid REFERENCES agent_runs(id),
    request_hash text NOT NULL,
    filename text NOT NULL,
    media_type text NOT NULL,
    bytes bytea NOT NULL CHECK (octet_length(bytes) <= 10485760),
    metadata jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id,run_id,request_hash)
);
-- First tools only read data and create report artifacts. Future vendor/PO
-- mutations must introduce policy, approval, and transactional outbox tables.
CREATE UNIQUE INDEX one_active_run_per_conversation ON agent_runs(workspace_id,conversation_id) WHERE status IN ('queued','running','paused');
