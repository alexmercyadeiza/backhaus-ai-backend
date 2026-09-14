-- Curated local menu: 12 food items and 8 drinks, using imported IDs/prices.
-- Run after importing the snapshot/menu:
-- psql postgres://apple@127.0.0.1/backhaus_ai_dev -v ON_ERROR_STOP=1 -f scripts/seed-local-menu.sql
BEGIN;
DO $$
BEGIN
    IF current_database() <> 'backhaus_ai_dev' THEN
        RAISE EXCEPTION 'Menu seed is restricted to backhaus_ai_dev';
    END IF;
END;
$$;
CREATE TEMP TABLE selected_menu (id bigint PRIMARY KEY, category text NOT NULL) ON COMMIT DROP;
INSERT INTO selected_menu VALUES
    (9591, 'Food'), -- Greek salad
    (9586, 'Food'), -- Chicken pepper soup with steamed yam
    (9612, 'Food'), -- Chicken wings
    (9594, 'Food'), -- Garlic butter prawns
    (9592, 'Food'), -- Chicken suya skewers
    (9599, 'Food'), -- Chicken Alfredo
    (9603, 'Food'), -- Charcoal grilled chicken
    (8467, 'Food'), -- Grilled salmon
    (8464, 'Food'), -- Suya beef flatbread
    (8479, 'Food'), -- Jollof rice
    (9606, 'Food'), -- French fries
    (9609, 'Food'), -- Ice cream sundae
    (8541, 'Drinks'), -- Water
    (8585, 'Drinks'), -- Coca Cola
    (9589, 'Drinks'), -- Sprite
    (9634, 'Drinks'), -- Orange juice
    (9635, 'Drinks'), -- Pineapple juice
    (8542, 'Drinks'), -- Cranberry juice
    (8504, 'Drinks'), -- Mojito
    (8502, 'Drinks'); -- Margarita
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM menu_items m JOIN selected_menu s USING(id)
        WHERE m.workspace_id='org_default' AND m.price_min>0
        AND jsonb_array_length(m.portions)>0) <> 20 THEN
        RAISE EXCEPTION 'Expected all 20 selected menu items with portions and prices';
    END IF;
END;
$$;
UPDATE menu_items m SET category=s.category, name=trim(m.name)
FROM selected_menu s WHERE m.workspace_id='org_default' AND m.id=s.id;
DELETE FROM menu_items m WHERE m.workspace_id='org_default'
AND NOT EXISTS(SELECT 1 FROM selected_menu s WHERE s.id=m.id);
COMMIT;
SELECT category, COUNT(*) AS items FROM menu_items
WHERE workspace_id='org_default' GROUP BY category ORDER BY category;
