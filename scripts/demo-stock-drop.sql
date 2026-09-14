-- Demo step 1: a reversible stock change. Restricted to backhaus_ai_dev / org_default.
--   psql postgres://apple@127.0.0.1/backhaus_ai_dev -v ON_ERROR_STOP=1 -f scripts/demo-stock-drop.sql
-- Issues 11 bottles of Absolut Vodka (par level 3) so the item falls below par.
-- The movement id is fixed so scripts/demo-stock-restore.sql can reverse it exactly.
-- The inventory agent detects the committed change and prepares a purchase-order
-- draft for the item's vendor; nothing is sent or paid.
BEGIN;
DO $$
DECLARE item text; balance numeric; par numeric;
BEGIN
    IF current_database() <> 'backhaus_ai_dev' THEN
        RAISE EXCEPTION 'Demo scripts are restricted to backhaus_ai_dev';
    END IF;
    SELECT id, current_balance, par_level INTO item, balance, par FROM inventory_items WHERE workspace_id='org_default' AND name='Absolut Vodka';
    IF item IS NULL THEN RAISE EXCEPTION 'Absolut Vodka is not in the local inventory fixture'; END IF;
    IF EXISTS (SELECT 1 FROM inventory_movements WHERE workspace_id='org_default' AND id='demo-absolut-issue') THEN
        RAISE NOTICE 'Demo movement already applied; run demo-stock-restore.sql first to repeat it';
        RETURN;
    END IF;
    IF balance < 11 THEN RAISE EXCEPTION 'Absolut Vodka balance % is below the demo issue of 11', balance; END IF;
    INSERT INTO inventory_movements(workspace_id,id,item_id,business_date,movement_type,quantity_delta,source)
    VALUES ('org_default','demo-absolut-issue',item,CURRENT_DATE,'issue',-11,'{"demo":"reversible local stock change; see scripts/demo-stock-restore.sql"}');
    UPDATE inventory_items SET current_balance=current_balance-11 WHERE workspace_id='org_default' AND id=item;
    RAISE NOTICE 'Absolut Vodka: % -> % (par %)', balance, balance-11, par;
END;
$$;
COMMIT;
SELECT name, current_balance, par_level, CASE WHEN current_balance<par_level THEN 'Below par' ELSE 'In stock' END AS status
FROM inventory_items WHERE workspace_id='org_default' AND name='Absolut Vodka';
