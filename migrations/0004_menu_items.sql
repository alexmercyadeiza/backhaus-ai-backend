CREATE TABLE menu_items (
    workspace_id text NOT NULL REFERENCES workspaces(id),
    id bigint NOT NULL,
    name text NOT NULL,
    category text,
    portions jsonb NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(portions)='array'),
    price_min numeric,
    price_max numeric,
    source jsonb NOT NULL,
    PRIMARY KEY(workspace_id,id)
);
CREATE INDEX menu_items_name ON menu_items(workspace_id,name,id);
CREATE INDEX inventory_items_name ON inventory_items(workspace_id,name,id);
