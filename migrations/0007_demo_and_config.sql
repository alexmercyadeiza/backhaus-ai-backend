-- Synthetic demo ownership marker: only workspaces created by the demo
-- generator may be reset without the explicit replace-imported flag.
ALTER TABLE workspaces ADD COLUMN fixture text CHECK (fixture IS NULL OR fixture IN ('synthetic_demo'));

-- Version-aware purchasing configuration with an audit trail.
ALTER TABLE vendors ADD COLUMN version integer NOT NULL DEFAULT 1;
ALTER TABLE vendor_items ADD COLUMN version integer NOT NULL DEFAULT 1;
ALTER TABLE purchasing_policies ADD COLUMN version integer NOT NULL DEFAULT 1;
CREATE TABLE purchasing_config_events (
    id bigserial PRIMARY KEY,
    workspace_id text NOT NULL REFERENCES workspaces(id),
    entity text NOT NULL CHECK (entity IN ('vendor','vendor_item','policy')),
    entity_id text NOT NULL,
    action text NOT NULL,
    actor text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX purchasing_config_events_workspace ON purchasing_config_events(workspace_id,id DESC);

-- Decisions keep the vendor details they were made with; later edits do not rewrite them.
ALTER TABLE purchase_orders ADD COLUMN vendor_snapshot jsonb;

-- Stock adjustments recorded through the dashboard.
ALTER TABLE inventory_movements ADD COLUMN reason text;
ALTER TABLE inventory_movements ADD COLUMN balance_after numeric;
ALTER TABLE inventory_movements ADD COLUMN recorded_at timestamptz NOT NULL DEFAULT now();
CREATE TABLE inventory_movement_requests (
    workspace_id text NOT NULL REFERENCES workspaces(id),
    request_key text NOT NULL,
    request_hash text NOT NULL,
    movement_id text NOT NULL,
    response jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id,request_key)
);

-- Purchase-order PDFs are artifacts without a run; keep one per order version.
CREATE UNIQUE INDEX artifacts_standalone_hash ON artifacts(workspace_id,request_hash) WHERE run_id IS NULL;
