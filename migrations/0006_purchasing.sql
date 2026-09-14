-- Background agent runs (inventory reviews) have no conversation.
ALTER TABLE agent_runs ADD COLUMN kind text NOT NULL DEFAULT 'chat' CHECK (kind IN ('chat','inventory_review'));
ALTER TABLE agent_runs ALTER COLUMN conversation_id DROP NOT NULL;
CREATE INDEX agent_runs_kind_recent ON agent_runs(workspace_id,kind,created_at DESC,id DESC);

-- Vendors and the purchasing configuration needed to prepare an order.
CREATE TABLE vendors (
    workspace_id text NOT NULL REFERENCES workspaces(id),
    id uuid NOT NULL,
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 200),
    contact_name text,
    email text,
    phone text,
    notes text,
    source text NOT NULL DEFAULT 'manual' CHECK (source IN ('snapshot','local_fixture','manual')),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id,id),
    UNIQUE (workspace_id,name)
);
CREATE TABLE vendor_items (
    workspace_id text NOT NULL,
    item_id text NOT NULL,
    vendor_id uuid NOT NULL,
    supplier_reference text,
    order_unit text NOT NULL,
    -- Inventory base units contained in one order unit (pack, carton, kg...).
    units_per_pack numeric NOT NULL CHECK (units_per_pack > 0),
    -- Price per order unit in NGN. NULL means the price is unknown and no line can be costed.
    pack_price numeric CHECK (pack_price IS NULL OR pack_price >= 0),
    currency text NOT NULL DEFAULT 'NGN' CHECK (currency = 'NGN'),
    -- Minimum packs per order and the stock level (base units) to order up to. NULL target = par level.
    minimum_order_quantity numeric NOT NULL DEFAULT 1 CHECK (minimum_order_quantity > 0),
    reorder_target numeric CHECK (reorder_target IS NULL OR reorder_target > 0),
    preferred boolean NOT NULL DEFAULT true,
    source text NOT NULL DEFAULT 'manual' CHECK (source IN ('snapshot','local_fixture','manual')),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id,item_id,vendor_id),
    FOREIGN KEY (workspace_id,item_id) REFERENCES inventory_items(workspace_id,id) ON DELETE CASCADE,
    FOREIGN KEY (workspace_id,vendor_id) REFERENCES vendors(workspace_id,id)
);
CREATE UNIQUE INDEX vendor_items_preferred ON vendor_items(workspace_id,item_id) WHERE preferred;
CREATE TABLE purchasing_policies (
    workspace_id text PRIMARY KEY REFERENCES workspaces(id),
    -- Orders at or below this NGN total are approved automatically; 0 disables automatic approval.
    auto_approve_limit numeric NOT NULL DEFAULT 0 CHECK (auto_approve_limit >= 0),
    -- Largest NGN total a dashboard approval may authorize; NULL means no limit is configured.
    approval_limit numeric CHECK (approval_limit IS NULL OR approval_limit > 0),
    currency text NOT NULL DEFAULT 'NGN' CHECK (currency = 'NGN'),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE purchase_order_counters (
    workspace_id text PRIMARY KEY REFERENCES workspaces(id),
    next_number integer NOT NULL DEFAULT 1
);

-- Purchase orders prepared by the inventory agent. Sending and payment are not implemented.
CREATE TABLE purchase_orders (
    id uuid PRIMARY KEY,
    workspace_id text NOT NULL REFERENCES workspaces(id),
    number text NOT NULL,
    vendor_id uuid NOT NULL,
    status text NOT NULL CHECK (status IN ('draft','approved','rejected','withdrawn')),
    approval_kind text CHECK (approval_kind IN ('automatic','manual')),
    approval_reason text NOT NULL,
    subtotal numeric NOT NULL CHECK (subtotal >= 0),
    currency text NOT NULL DEFAULT 'NGN' CHECK (currency = 'NGN'),
    line_count integer NOT NULL CHECK (line_count > 0),
    attention jsonb NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(attention) = 'array'),
    revision bigint NOT NULL,
    version integer NOT NULL DEFAULT 1,
    prepared_by text NOT NULL DEFAULT 'inventory_agent',
    decided_at timestamptz,
    decided_by text,
    decision_note text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (workspace_id,number),
    FOREIGN KEY (workspace_id,vendor_id) REFERENCES vendors(workspace_id,id)
);
-- At most one open draft per vendor: repeated checks revise it instead of duplicating it.
CREATE UNIQUE INDEX purchase_orders_one_draft_per_vendor ON purchase_orders(workspace_id,vendor_id) WHERE status='draft';
CREATE INDEX purchase_orders_list ON purchase_orders(workspace_id,(CASE status WHEN 'draft' THEN 0 ELSE 1 END),created_at DESC,number DESC);
CREATE TABLE purchase_order_lines (
    id uuid PRIMARY KEY,
    purchase_order_id uuid NOT NULL REFERENCES purchase_orders(id) ON DELETE CASCADE,
    workspace_id text NOT NULL,
    item_id text NOT NULL,
    item_name text NOT NULL,
    supplier_reference text,
    order_unit text NOT NULL,
    units_per_pack numeric NOT NULL,
    quantity_packs numeric NOT NULL CHECK (quantity_packs > 0),
    quantity_units numeric NOT NULL CHECK (quantity_units > 0),
    pack_price numeric NOT NULL CHECK (pack_price >= 0),
    line_total numeric NOT NULL CHECK (line_total >= 0),
    current_balance numeric NOT NULL,
    par_level numeric NOT NULL,
    reorder_target numeric NOT NULL,
    on_order_units numeric NOT NULL DEFAULT 0,
    position integer NOT NULL,
    FOREIGN KEY (workspace_id,item_id) REFERENCES inventory_items(workspace_id,id)
);
CREATE INDEX purchase_order_lines_order ON purchase_order_lines(purchase_order_id,position);
CREATE INDEX purchase_order_lines_item ON purchase_order_lines(workspace_id,item_id);
CREATE TABLE purchase_order_events (
    id bigserial PRIMARY KEY,
    purchase_order_id uuid NOT NULL REFERENCES purchase_orders(id) ON DELETE CASCADE,
    workspace_id text NOT NULL,
    event_type text NOT NULL,
    actor text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX purchase_order_events_order ON purchase_order_events(purchase_order_id,id);

-- Vendor and policy changes alter what can be ordered: wake the inventory agent.
DO $$
DECLARE entry text;
BEGIN
    FOREACH entry IN ARRAY ARRAY['vendors','vendor_items','purchasing_policies'] LOOP
        EXECUTE format('CREATE TRIGGER agent_change_insert AFTER INSERT ON %I REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION mark_agent_data_changed(%L)',entry,'inventory');
        EXECUTE format('CREATE TRIGGER agent_change_update AFTER UPDATE ON %I REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION mark_agent_data_changed(%L)',entry,'inventory');
        EXECUTE format('CREATE TRIGGER agent_change_delete AFTER DELETE ON %I REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION mark_agent_data_changed(%L)',entry,'inventory');
    END LOOP;
END;
$$;
