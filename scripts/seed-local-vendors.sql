-- Local development purchasing fixture. Restricted to backhaus_ai_dev / org_default.
-- Run after the snapshot import and par-level seed:
--   psql postgres://apple@127.0.0.1/backhaus_ai_dev -v ON_ERROR_STOP=1 -f scripts/seed-local-vendors.sql
--
-- Two vendors come from the imported snapshot (supplier name and unit cost on the
-- inventory record). Every other vendor, price, pack size, minimum and contact is a
-- documented LOCAL FIXTURE: placeholder values for testing the ordering workflow,
-- not real suppliers, agreements or negotiated prices. Fixture contacts use the
-- reserved .invalid domain so they can never reach anyone.
-- Five uncategorized items are intentionally left without a vendor, and one item
-- (Digital Scale) has a vendor but no price, so the agent's missing-information
-- handling is exercised. Re-running the script changes nothing.
BEGIN;
DO $$
BEGIN
    IF current_database() <> 'backhaus_ai_dev' THEN
        RAISE EXCEPTION 'Vendor seed is restricted to backhaus_ai_dev';
    END IF;
    IF (SELECT COUNT(*) FROM inventory_items WHERE workspace_id='org_default') <> 50 THEN
        RAISE EXCEPTION 'Expected the local 50-item inventory fixture';
    END IF;
END;
$$;

INSERT INTO purchasing_policies(workspace_id,auto_approve_limit,approval_limit)
VALUES ('org_default', 20000, 2000000)
ON CONFLICT (workspace_id) DO NOTHING;

CREATE TEMP TABLE seed_vendors (key text PRIMARY KEY, id uuid NOT NULL, name text NOT NULL, contact_name text, email text, phone text, notes text, source text NOT NULL) ON COMMIT DROP;
INSERT INTO seed_vendors VALUES
    ('shayo',     '2a2f2a4e-0001-4a00-8000-000000000001', 'Shayo Nation', NULL, NULL, NULL, 'Supplier name and price taken from the imported inventory record. Contact details are not known.', 'snapshot'),
    ('snapsea',   '2a2f2a4e-0001-4a00-8000-000000000002', 'Snapseafood', NULL, NULL, NULL, 'Supplier name and price taken from the imported inventory record. Contact details are not known.', 'snapshot'),
    ('spirits',   '2a2f2a4e-0001-4a00-8000-000000000003', 'Fixture Spirits & Wine (local dev)', 'Fixture contact', 'orders@fixture-spirits.invalid', NULL, 'Local development fixture. Placeholder prices and contact; not a real supplier.', 'local_fixture'),
    ('bar',       '2a2f2a4e-0001-4a00-8000-000000000004', 'Fixture Bar Supplies (local dev)', 'Fixture contact', 'orders@fixture-bar.invalid', NULL, 'Local development fixture. Placeholder prices and contact; not a real supplier.', 'local_fixture'),
    ('produce',   '2a2f2a4e-0001-4a00-8000-000000000005', 'Fixture Fresh Produce (local dev)', 'Fixture contact', 'orders@fixture-produce.invalid', NULL, 'Local development fixture. Placeholder prices and contact; not a real supplier.', 'local_fixture'),
    ('proteins',  '2a2f2a4e-0001-4a00-8000-000000000006', 'Fixture Proteins (local dev)', 'Fixture contact', 'orders@fixture-proteins.invalid', NULL, 'Local development fixture. Placeholder prices and contact; not a real supplier.', 'local_fixture'),
    ('provisions','2a2f2a4e-0001-4a00-8000-000000000007', 'Fixture Provisions (local dev)', 'Fixture contact', 'orders@fixture-provisions.invalid', NULL, 'Local development fixture. Placeholder prices and contact; not a real supplier.', 'local_fixture'),
    ('facilities','2a2f2a4e-0001-4a00-8000-000000000008', 'Fixture Cleaning & Facilities (local dev)', 'Fixture contact', 'orders@fixture-facilities.invalid', NULL, 'Local development fixture. Placeholder prices and contact; not a real supplier.', 'local_fixture');
INSERT INTO vendors(workspace_id,id,name,contact_name,email,phone,notes,source)
SELECT 'org_default', id, name, contact_name, email, phone, notes, source FROM seed_vendors
ON CONFLICT (workspace_id,name) DO NOTHING;

-- item name, vendor key, order unit, units per order unit, price per order unit (NGN, NULL = unknown), minimum packs, source
CREATE TEMP TABLE seed_items (item_name text PRIMARY KEY, vendor_key text NOT NULL, order_unit text NOT NULL, units_per_pack numeric NOT NULL, pack_price numeric, minimum numeric NOT NULL, source text NOT NULL) ON COMMIT DROP;
INSERT INTO seed_items VALUES
    ('Amabile Rosa Red', 'shayo', 'bottle', 1, 8166, 1, 'snapshot'),
    ('Calamari Rings', 'snapsea', 'kg', 1, 16000, 1, 'snapshot'),
    ('Absolut Vodka', 'spirits', 'bottle', 1, 38000, 1, 'local_fixture'),
    ('4th Street Red', 'spirits', 'bottle', 1, 9500, 1, 'local_fixture'),
    ('Angostura Orange', 'bar', 'bottle', 1, 12000, 1, 'local_fixture'),
    ('Chivita Pineapple', 'bar', 'pack', 1, 1500, 6, 'local_fixture'),
    ('Apple Fruit', 'produce', 'piece', 1, 500, 6, 'local_fixture'),
    ('Asparagus', 'produce', 'bunch', 1, 3500, 1, 'local_fixture'),
    ('Avocado Fruit', 'produce', 'piece', 1, 800, 6, 'local_fixture'),
    ('Beetroot', 'produce', 'kg', 1, 2500, 1, 'local_fixture'),
    ('Chicken Breast', 'proteins', 'kg', 1, 6500, 2, 'local_fixture'),
    ('Chicken Minced', 'proteins', 'kg', 1, 6000, 2, 'local_fixture'),
    ('Chicken Thighs', 'proteins', 'kg', 1, 5500, 2, 'local_fixture'),
    ('Agar Agar', 'provisions', 'packet', 1, 4500, 1, 'local_fixture'),
    ('Apple Cidar Vinegar', 'provisions', 'bottle', 1, 3200, 1, 'local_fixture'),
    ('Bama', 'provisions', 'jar', 1, 4800, 1, 'local_fixture'),
    ('Beans', 'provisions', 'kg', 1, 2200, 5, 'local_fixture'),
    ('Candle (Party Cake Candle)', 'provisions', 'pack', 1, 1500, 1, 'local_fixture'),
    ('HB Candles', 'provisions', 'pack', 1, 1800, 1, 'local_fixture'),
    ('Perfumes', 'provisions', 'piece', 1, 9000, 1, 'local_fixture'),
    ('Room Spray', 'provisions', 'piece', 1, 4500, 1, 'local_fixture'),
    ('Baking Paper', 'provisions', 'pack', 1, 3500, 1, 'local_fixture'),
    ('Bodrum (Jerkins)', 'provisions', 'bottle', 1, 2800, 1, 'local_fixture'),
    ('Bundle of Twin', 'provisions', 'piece', 1, 1200, 1, 'local_fixture'),
    ('Digital Scale', 'provisions', 'piece', 1, NULL, 1, 'local_fixture'),
    ('3in1 Trash Bag', 'facilities', 'pack', 1, 2500, 1, 'local_fixture'),
    ('Big Trash Bag(Bin Bag)', 'facilities', 'piece', 1, 400, 10, 'local_fixture'),
    ('Hair Net', 'facilities', 'pack', 1, 1500, 1, 'local_fixture'),
    ('Hand Sanitizer', 'facilities', 'piece', 1, 2200, 1, 'local_fixture'),
    ('Nose Mask', 'facilities', 'pack', 1, 1800, 1, 'local_fixture'),
    ('Candle Diffuser', 'facilities', 'piece', 1, 6000, 1, 'local_fixture'),
    ('Candle Diffuser Oil', 'facilities', 'piece', 1, 4500, 1, 'local_fixture'),
    ('Flags', 'facilities', 'piece', 1, 3000, 1, 'local_fixture'),
    ('Battery', 'facilities', 'piece', 1, 800, 4, 'local_fixture'),
    ('Butt Washer', 'facilities', 'piece', 1, 7500, 1, 'local_fixture'),
    ('Multiple Plug', 'facilities', 'piece', 1, 6500, 1, 'local_fixture'),
    ('TV Guard', 'facilities', 'piece', 1, 12000, 1, 'local_fixture'),
    ('Bakhoo', 'facilities', 'piece', 1, 3000, 1, 'local_fixture'),
    ('Blueberry Flavour', 'facilities', 'piece', 1, 4500, 1, 'local_fixture'),
    ('Milk Flavour', 'facilities', 'piece', 1, 4500, 1, 'local_fixture'),
    ('Shisha Charcoal', 'facilities', 'piece', 1, 2500, 1, 'local_fixture'),
    ('A4 Paper', 'facilities', 'rim', 1, 7500, 1, 'local_fixture'),
    ('Big Sticky Note', 'facilities', 'piece', 1, 1200, 1, 'local_fixture'),
    ('Biro', 'facilities', 'pack', 1, 1500, 1, 'local_fixture'),
    ('File Jacket (Clear Bag)', 'facilities', 'piece', 1, 300, 12, 'local_fixture');
DO $$
DECLARE missing text;
BEGIN
    SELECT string_agg(item_name, ', ') INTO missing FROM seed_items s
    WHERE NOT EXISTS (SELECT 1 FROM inventory_items i WHERE i.workspace_id='org_default' AND i.name=s.item_name);
    IF missing IS NOT NULL THEN
        RAISE EXCEPTION 'Seed refers to unknown inventory items: %', missing;
    END IF;
END;
$$;
INSERT INTO vendor_items(workspace_id,item_id,vendor_id,supplier_reference,order_unit,units_per_pack,pack_price,minimum_order_quantity,reorder_target,preferred,source)
SELECT 'org_default', i.id, v.id, NULL, s.order_unit, s.units_per_pack, s.pack_price, s.minimum, NULL, true, s.source
FROM seed_items s
JOIN inventory_items i ON i.workspace_id='org_default' AND i.name=s.item_name
JOIN seed_vendors sv ON sv.key=s.vendor_key
JOIN vendors v ON v.workspace_id='org_default' AND v.name=sv.name
ON CONFLICT (workspace_id,item_id,vendor_id) DO NOTHING;
COMMIT;
SELECT v.name AS vendor, v.source, COUNT(vi.item_id) AS items,
       COUNT(vi.item_id) FILTER (WHERE vi.pack_price IS NULL) AS unpriced
FROM vendors v LEFT JOIN vendor_items vi ON vi.workspace_id=v.workspace_id AND vi.vendor_id=v.id
WHERE v.workspace_id='org_default' GROUP BY v.name, v.source ORDER BY v.source, v.name;
SELECT COUNT(*) AS items_without_vendor FROM inventory_items i
WHERE i.workspace_id='org_default' AND NOT EXISTS (SELECT 1 FROM vendor_items vi WHERE vi.workspace_id=i.workspace_id AND vi.item_id=i.id);
