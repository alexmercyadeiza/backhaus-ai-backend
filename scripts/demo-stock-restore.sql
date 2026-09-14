-- Demo reset: reverses scripts/demo-stock-drop.sql. Restricted to backhaus_ai_dev / org_default.
--   psql postgres://apple@127.0.0.1/backhaus_ai_dev -v ON_ERROR_STOP=1 -f scripts/demo-stock-restore.sql
-- Removes the demo movement, restores the 11 bottles, and withdraws any open or
-- approved purchase order that contains only the demo item so the demo can be
-- repeated. Audit events are kept; nothing is deleted from the order history.
BEGIN;
DO $$
DECLARE item text; withdrawn int := 0; o record;
BEGIN
    IF current_database() <> 'backhaus_ai_dev' THEN
        RAISE EXCEPTION 'Demo scripts are restricted to backhaus_ai_dev';
    END IF;
    SELECT id INTO item FROM inventory_items WHERE workspace_id='org_default' AND name='Absolut Vodka';
    IF NOT EXISTS (SELECT 1 FROM inventory_movements WHERE workspace_id='org_default' AND id='demo-absolut-issue') THEN
        RAISE NOTICE 'Demo movement not present; nothing to restore';
    ELSE
        DELETE FROM inventory_movements WHERE workspace_id='org_default' AND id='demo-absolut-issue';
        UPDATE inventory_items SET current_balance=current_balance+11 WHERE workspace_id='org_default' AND id=item;
    END IF;
    FOR o IN SELECT p.id, p.number FROM purchase_orders p WHERE p.workspace_id='org_default' AND p.status IN ('draft','approved')
        AND NOT EXISTS (SELECT 1 FROM purchase_order_lines l WHERE l.purchase_order_id=p.id AND l.item_id<>item)
        AND EXISTS (SELECT 1 FROM purchase_order_lines l WHERE l.purchase_order_id=p.id AND l.item_id=item)
    LOOP
        UPDATE purchase_orders SET status='withdrawn', decided_at=now(), decided_by='demo-reset', decision_note='Withdrawn by scripts/demo-stock-restore.sql', version=version+1, updated_at=now() WHERE id=o.id;
        INSERT INTO purchase_order_events(purchase_order_id,workspace_id,event_type,actor,payload) VALUES (o.id,'org_default','withdrawn','demo-reset','{"reason":"demo_reset"}');
        withdrawn := withdrawn + 1;
        RAISE NOTICE 'Withdrew demo order %', o.number;
    END LOOP;
    RAISE NOTICE 'Restored Absolut Vodka; % demo order(s) withdrawn', withdrawn;
END;
$$;
COMMIT;
SELECT name, current_balance, par_level FROM inventory_items WHERE workspace_id='org_default' AND name='Absolut Vodka';
