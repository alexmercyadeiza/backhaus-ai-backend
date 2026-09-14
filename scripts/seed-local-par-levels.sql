-- Local development seed only. Keeps valid imported thresholds and all balances.
-- Values below are test fixtures, not recommended operating stock levels.
-- Run: psql postgres://apple@127.0.0.1/backhaus_ai_dev -v ON_ERROR_STOP=1 -f scripts/seed-local-par-levels.sql
BEGIN;
DO $$
BEGIN
    IF current_database() <> 'backhaus_ai_dev' THEN
        RAISE EXCEPTION 'Par-level seed is restricted to backhaus_ai_dev';
    END IF;
    IF (SELECT COUNT(*) FROM inventory_items WHERE workspace_id='org_default') <> 50 THEN
        RAISE EXCEPTION 'Expected the local 50-item inventory fixture';
    END IF;
END;
$$;
UPDATE inventory_items
SET par_level = CASE
    WHEN lower(trim(unit)) = 'kg' THEN 5
    WHEN lower(trim(unit)) IN ('bottle','bottles') THEN 2
    WHEN lower(trim(unit)) IN ('pack','packs','packet') THEN 3
    ELSE 2
END
WHERE workspace_id='org_default' AND (par_level IS NULL OR par_level<=0);
DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM inventory_items WHERE workspace_id='org_default' AND (par_level IS NULL OR par_level<=0)) THEN
        RAISE EXCEPTION 'All fixture items must have a positive par level';
    END IF;
    IF NOT EXISTS(SELECT 1 FROM inventory_items WHERE workspace_id='org_default' AND current_balance<par_level)
        OR NOT EXISTS(SELECT 1 FROM inventory_items WHERE workspace_id='org_default' AND current_balance>=par_level) THEN
        RAISE EXCEPTION 'Expected a mix of below-par and stocked items';
    END IF;
END;
$$;
COMMIT;
SELECT COUNT(*) AS items,
       COUNT(*) FILTER(WHERE par_level>0) AS par_levels_set,
       COUNT(*) FILTER(WHERE current_balance<par_level) AS below_par,
       COUNT(*) FILTER(WHERE current_balance>=par_level) AS at_or_above_par
FROM inventory_items WHERE workspace_id='org_default';
