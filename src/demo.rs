//! Synthetic demo dataset for a fictional restaurant. Authored independently
//! of any production snapshot; every value is invented. Deterministic for a
//! given seed and business-date anchor, so tests and demos are reproducible.
//! `init` never destroys data; `reset` is explicit and guarded.
use anyhow::{Context, Result, bail, ensure};
use chrono::{Datelike, Duration, NaiveDate, Weekday};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

pub const RESTAURANT_NAME: &str = "Harmattan House (fictional demo restaurant)";
pub const DEFAULT_SEED: u64 = 20260914;
pub const SALES_DAYS: i64 = 61;
pub const POLICY_AUTO_APPROVE: &str = "20000";
pub const POLICY_APPROVAL_LIMIT: &str = "250000";

/// SplitMix64: tiny, dependency-free, deterministic.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    /// Weighted index over `weights`.
    fn weighted(&mut self, weights: &[u32]) -> usize {
        let total: u64 = weights.iter().map(|w| *w as u64).sum();
        let mut pick = self.below(total);
        for (i, w) in weights.iter().enumerate() {
            if pick < *w as u64 {
                return i;
            }
            pick -= *w as u64;
        }
        weights.len() - 1
    }
}

struct MenuItem {
    id: i64,
    name: &'static str,
    category: &'static str,
    price: &'static str,
    weight: u32,
}
const MENU: &[MenuItem] = &[
    MenuItem {
        id: 1001,
        name: "Jollof Rice & Grilled Chicken",
        category: "Food",
        price: "6500",
        weight: 22,
    },
    MenuItem {
        id: 1002,
        name: "Beef Suya Platter",
        category: "Food",
        price: "8500",
        weight: 12,
    },
    MenuItem {
        id: 1003,
        name: "Grilled Catfish",
        category: "Food",
        price: "9500",
        weight: 9,
    },
    MenuItem {
        id: 1004,
        name: "Egusi Soup & Pounded Yam",
        category: "Food",
        price: "7000",
        weight: 11,
    },
    MenuItem {
        id: 1005,
        name: "Chicken Shawarma",
        category: "Food",
        price: "4500",
        weight: 14,
    },
    MenuItem {
        id: 1006,
        name: "Goat Pepper Soup",
        category: "Food",
        price: "5500",
        weight: 8,
    },
    MenuItem {
        id: 1007,
        name: "Fried Rice & Turkey",
        category: "Food",
        price: "7500",
        weight: 13,
    },
    MenuItem {
        id: 1008,
        name: "Yam Chips & Pepper Sauce",
        category: "Food",
        price: "3500",
        weight: 10,
    },
    MenuItem {
        id: 1009,
        name: "Puff Puff Basket",
        category: "Food",
        price: "2000",
        weight: 9,
    },
    MenuItem {
        id: 1010,
        name: "Peppered Chicken Wings",
        category: "Food",
        price: "5000",
        weight: 10,
    },
    MenuItem {
        id: 1011,
        name: "Smoked Salmon Salad",
        category: "Food",
        price: "8000",
        weight: 5,
    },
    MenuItem {
        id: 1012,
        name: "Caramel Pudding",
        category: "Food",
        price: "3000",
        weight: 7,
    },
    MenuItem {
        id: 1013,
        name: "Chapman",
        category: "Drinks",
        price: "2500",
        weight: 16,
    },
    MenuItem {
        id: 1014,
        name: "Zobo",
        category: "Drinks",
        price: "1500",
        weight: 12,
    },
    MenuItem {
        id: 1015,
        name: "Bottled Water",
        category: "Drinks",
        price: "700",
        weight: 20,
    },
    MenuItem {
        id: 1016,
        name: "Malt Drink",
        category: "Drinks",
        price: "1200",
        weight: 10,
    },
    MenuItem {
        id: 1017,
        name: "Fresh Orange Juice",
        category: "Drinks",
        price: "2000",
        weight: 8,
    },
    MenuItem {
        id: 1018,
        name: "Palm Wine",
        category: "Drinks",
        price: "2500",
        weight: 6,
    },
    MenuItem {
        id: 1019,
        name: "Lager Beer",
        category: "Drinks",
        price: "1800",
        weight: 12,
    },
    MenuItem {
        id: 1020,
        name: "Cappuccino",
        category: "Drinks",
        price: "2200",
        weight: 7,
    },
];

struct Vendor {
    key: &'static str,
    name: &'static str,
    contact: &'static str,
    email: &'static str,
    phone: &'static str,
}
const VENDORS: &[Vendor] = &[
    Vendor {
        key: "produce",
        name: "Lagoon Fresh Produce",
        contact: "Ada (fictional)",
        email: "orders@lagoon-produce.invalid",
        phone: "+234 800 000 0001",
    },
    Vendor {
        key: "proteins",
        name: "Harbour Proteins",
        contact: "Bayo (fictional)",
        email: "sales@harbour-proteins.invalid",
        phone: "+234 800 000 0002",
    },
    Vendor {
        key: "provisions",
        name: "Golden Grain Provisions",
        contact: "Chidi (fictional)",
        email: "orders@golden-grain.invalid",
        phone: "+234 800 000 0003",
    },
    Vendor {
        key: "beverages",
        name: "Sunrise Beverages",
        contact: "Dara (fictional)",
        email: "orders@sunrise-beverages.invalid",
        phone: "+234 800 000 0004",
    },
    Vendor {
        key: "spirits",
        name: "Ridge Spirits & Wine",
        contact: "Efe (fictional)",
        email: "trade@ridge-spirits.invalid",
        phone: "+234 800 000 0005",
    },
    Vendor {
        key: "cleaning",
        name: "Clearwater Cleaning & Packaging",
        contact: "Funke (fictional)",
        email: "orders@clearwater-supplies.invalid",
        phone: "+234 800 000 0006",
    },
    Vendor {
        key: "coffee",
        name: "Ember Coffee Roasters",
        contact: "Gbenga (fictional)",
        email: "hello@ember-roasters.invalid",
        phone: "+234 800 000 0007",
    },
];

/// Inventory item: id suffix, name, category, unit, balance, par, vendor key
/// (empty = intentionally unassigned), order unit, units per pack, pack price, minimum packs.
struct Item {
    id: &'static str,
    name: &'static str,
    category: &'static str,
    unit: &'static str,
    balance: &'static str,
    par: &'static str,
    vendor: &'static str,
    order_unit: &'static str,
    units_per_pack: &'static str,
    pack_price: &'static str,
    minimum: &'static str,
}
macro_rules! item {
    ($id:literal, $name:literal, $cat:literal, $unit:literal, $bal:literal, $par:literal, $vendor:literal, $ou:literal, $upp:literal, $price:literal, $min:literal) => {
        Item {
            id: $id,
            name: $name,
            category: $cat,
            unit: $unit,
            balance: $bal,
            par: $par,
            vendor: $vendor,
            order_unit: $ou,
            units_per_pack: $upp,
            pack_price: $price,
            minimum: $min,
        }
    };
}
const ITEMS: &[Item] = &[
    // Produce (12)
    item!(
        "prod-tomatoes",
        "Tomatoes",
        "Produce",
        "kg",
        "25",
        "15",
        "produce",
        "crate",
        "10",
        "9000",
        "1"
    ),
    item!(
        "prod-onions",
        "Onions",
        "Produce",
        "kg",
        "30",
        "15",
        "produce",
        "bag",
        "10",
        "7000",
        "1"
    ),
    item!(
        "prod-scotch-bonnet",
        "Scotch Bonnet Peppers",
        "Produce",
        "kg",
        "8",
        "5",
        "produce",
        "kg",
        "1",
        "2500",
        "2"
    ),
    item!(
        "prod-bell-peppers",
        "Bell Peppers",
        "Produce",
        "kg",
        "10",
        "6",
        "produce",
        "kg",
        "1",
        "3000",
        "2"
    ),
    // Intended shortage: 12 short, 2 packs of 10 at 1,500 = 3,000, within the automatic limit.
    item!(
        "prod-lemons",
        "Lemons",
        "Produce",
        "pcs",
        "18",
        "30",
        "produce",
        "bag of 10",
        "10",
        "1500",
        "1"
    ),
    item!(
        "prod-cucumbers",
        "Cucumbers",
        "Produce",
        "pcs",
        "20",
        "12",
        "produce",
        "pcs",
        "1",
        "300",
        "6"
    ),
    item!(
        "prod-lettuce",
        "Lettuce",
        "Produce",
        "head",
        "10",
        "6",
        "produce",
        "head",
        "1",
        "900",
        "4"
    ),
    item!(
        "prod-plantain",
        "Plantain",
        "Produce",
        "pcs",
        "40",
        "24",
        "produce",
        "bunch of 12",
        "12",
        "4800",
        "1"
    ),
    item!(
        "prod-yam", "Yam", "Produce", "tuber", "15", "10", "produce", "tuber", "1", "2800", "5"
    ),
    item!(
        "prod-spring-onions",
        "Spring Onions",
        "Produce",
        "bunch",
        "8",
        "5",
        "produce",
        "bunch",
        "1",
        "500",
        "4"
    ),
    item!(
        "prod-ginger",
        "Ginger",
        "Produce",
        "kg",
        "4",
        "2",
        "produce",
        "kg",
        "1",
        "3200",
        "1"
    ),
    item!(
        "prod-garlic",
        "Garlic",
        "Produce",
        "kg",
        "3",
        "2",
        "produce",
        "kg",
        "1",
        "4500",
        "1"
    ),
    // Proteins (9)
    // Intended shortage: 12 kg short, 3 packs of 5 kg at 9,000 = 27,000, needs manual approval.
    item!(
        "prot-chicken-breast",
        "Chicken Breast",
        "Proteins",
        "kg",
        "8",
        "20",
        "proteins",
        "5 kg pack",
        "5",
        "9000",
        "1"
    ),
    item!(
        "prot-chicken-wings",
        "Chicken Wings",
        "Proteins",
        "kg",
        "12",
        "10",
        "proteins",
        "5 kg pack",
        "5",
        "8000",
        "1"
    ),
    item!(
        "prot-turkey",
        "Turkey Wings",
        "Proteins",
        "kg",
        "15",
        "10",
        "proteins",
        "5 kg pack",
        "5",
        "12500",
        "1"
    ),
    item!(
        "prot-beef-fillet",
        "Beef Fillet",
        "Proteins",
        "kg",
        "12",
        "8",
        "proteins",
        "kg",
        "1",
        "6500",
        "2"
    ),
    item!(
        "prot-catfish",
        "Catfish",
        "Proteins",
        "kg",
        "14",
        "10",
        "proteins",
        "kg",
        "1",
        "4200",
        "5"
    ),
    item!(
        "prot-salmon",
        "Salmon",
        "Proteins",
        "kg",
        "6",
        "4",
        "proteins",
        "kg",
        "1",
        "14000",
        "1"
    ),
    item!(
        "prot-prawns",
        "Prawns",
        "Proteins",
        "kg",
        "5",
        "3",
        "proteins",
        "kg",
        "1",
        "11000",
        "1"
    ),
    item!(
        "prot-goat",
        "Goat Meat",
        "Proteins",
        "kg",
        "9",
        "6",
        "proteins",
        "kg",
        "1",
        "5800",
        "2"
    ),
    item!(
        "prot-eggs",
        "Eggs",
        "Proteins",
        "crate",
        "6",
        "4",
        "proteins",
        "crate",
        "1",
        "4300",
        "2"
    ),
    // Provisions (13)
    // Demo item: stocked at 40 kg (par 25). Issuing 20 kg leaves 20, short 5, one 25 kg bag at 42,000: manual approval.
    item!(
        "prov-basmati",
        "Basmati Rice",
        "Provisions",
        "kg",
        "40",
        "25",
        "provisions",
        "25 kg bag",
        "25",
        "42000",
        "1"
    ),
    item!(
        "prov-long-grain",
        "Long Grain Rice",
        "Provisions",
        "kg",
        "50",
        "30",
        "provisions",
        "25 kg bag",
        "25",
        "36000",
        "1"
    ),
    item!(
        "prov-flour",
        "Wheat Flour",
        "Provisions",
        "kg",
        "30",
        "20",
        "provisions",
        "10 kg bag",
        "10",
        "9500",
        "1"
    ),
    item!(
        "prov-veg-oil",
        "Vegetable Oil",
        "Provisions",
        "L",
        "40",
        "25",
        "provisions",
        "25 L drum",
        "25",
        "52000",
        "1"
    ),
    // Intended exception: below par with no supplier assigned. The operator assigns one on the Vendors page.
    item!(
        "prov-palm-oil",
        "Palm Oil",
        "Provisions",
        "L",
        "6",
        "15",
        "",
        "",
        "1",
        "",
        "1"
    ),
    item!(
        "prov-sugar",
        "Sugar",
        "Provisions",
        "kg",
        "20",
        "10",
        "provisions",
        "kg",
        "1",
        "1400",
        "5"
    ),
    item!(
        "prov-salt",
        "Salt",
        "Provisions",
        "kg",
        "10",
        "5",
        "provisions",
        "kg",
        "1",
        "600",
        "5"
    ),
    item!(
        "prov-curry",
        "Curry Powder",
        "Provisions",
        "kg",
        "3",
        "2",
        "provisions",
        "kg",
        "1",
        "5200",
        "1"
    ),
    item!(
        "prov-thyme",
        "Dried Thyme",
        "Provisions",
        "kg",
        "2",
        "1",
        "provisions",
        "kg",
        "1",
        "6800",
        "1"
    ),
    item!(
        "prov-tomato-paste",
        "Tomato Paste",
        "Provisions",
        "tin",
        "30",
        "20",
        "provisions",
        "carton of 12",
        "12",
        "9600",
        "1"
    ),
    item!(
        "prov-egusi",
        "Egusi",
        "Provisions",
        "kg",
        "6",
        "4",
        "provisions",
        "kg",
        "1",
        "4800",
        "1"
    ),
    item!(
        "prov-coffee",
        "Coffee Beans",
        "Provisions",
        "kg",
        "5",
        "3",
        "coffee",
        "kg",
        "1",
        "18000",
        "1"
    ),
    item!(
        "prov-milk",
        "Fresh Milk",
        "Provisions",
        "L",
        "20",
        "12",
        "beverages",
        "L",
        "1",
        "1500",
        "6"
    ),
    // Beverages (7)
    item!(
        "bev-water",
        "Bottled Water 750 ml",
        "Beverages",
        "bottle",
        "120",
        "80",
        "beverages",
        "pack of 12",
        "12",
        "3000",
        "2"
    ),
    item!(
        "bev-malt",
        "Malt Drink",
        "Beverages",
        "bottle",
        "60",
        "40",
        "beverages",
        "crate of 24",
        "24",
        "14400",
        "1"
    ),
    item!(
        "bev-cola",
        "Cola",
        "Beverages",
        "bottle",
        "70",
        "40",
        "beverages",
        "crate of 24",
        "24",
        "12000",
        "1"
    ),
    item!(
        "bev-lager",
        "Lager Beer",
        "Beverages",
        "bottle",
        "90",
        "50",
        "beverages",
        "crate of 24",
        "24",
        "26400",
        "1"
    ),
    item!(
        "bev-orange",
        "Orange Juice",
        "Beverages",
        "L",
        "15",
        "10",
        "beverages",
        "L",
        "1",
        "1800",
        "5"
    ),
    item!(
        "bev-zobo",
        "Hibiscus (Zobo) Leaves",
        "Beverages",
        "kg",
        "3",
        "2",
        "beverages",
        "kg",
        "1",
        "3500",
        "1"
    ),
    item!(
        "bev-palm-wine",
        "Palm Wine",
        "Beverages",
        "L",
        "10",
        "6",
        "beverages",
        "L",
        "1",
        "1200",
        "5"
    ),
    // Spirits (4)
    // Intended shortage: 10 bottles short at 45,000 = 450,000, above the 250,000 operator approval limit: blocked.
    item!(
        "spir-whisky",
        "Premium Whisky",
        "Spirits",
        "bottle",
        "2",
        "12",
        "spirits",
        "bottle",
        "1",
        "45000",
        "1"
    ),
    item!(
        "spir-vodka",
        "Vodka",
        "Spirits",
        "bottle",
        "8",
        "6",
        "spirits",
        "bottle",
        "1",
        "22000",
        "1"
    ),
    item!(
        "spir-red-wine",
        "Red Wine",
        "Spirits",
        "bottle",
        "14",
        "10",
        "spirits",
        "bottle",
        "1",
        "15000",
        "6"
    ),
    item!(
        "spir-gin", "Gin", "Spirits", "bottle", "7", "5", "spirits", "bottle", "1", "19000", "1"
    ),
    // Cleaning & packaging (5)
    item!(
        "clean-dish-soap",
        "Dish Soap",
        "Cleaning",
        "L",
        "12",
        "8",
        "cleaning",
        "5 L can",
        "5",
        "6500",
        "1"
    ),
    item!(
        "clean-bleach",
        "Bleach",
        "Cleaning",
        "L",
        "8",
        "5",
        "cleaning",
        "L",
        "1",
        "900",
        "4"
    ),
    item!(
        "clean-bin-bags",
        "Bin Bags",
        "Cleaning",
        "roll",
        "10",
        "6",
        "cleaning",
        "roll",
        "1",
        "2200",
        "2"
    ),
    item!(
        "clean-napkins",
        "Paper Napkins",
        "Cleaning",
        "pack",
        "30",
        "20",
        "cleaning",
        "pack",
        "1",
        "1100",
        "10"
    ),
    item!(
        "clean-takeaway",
        "Takeaway Boxes",
        "Cleaning",
        "pack of 50",
        "25",
        "15",
        "cleaning",
        "pack of 50",
        "1",
        "7500",
        "5"
    ),
];

pub struct Options {
    pub as_of: NaiveDate,
    pub seed: u64,
}

fn d(value: &str) -> Decimal {
    value.parse().expect("fixture decimal")
}

/// Lagos business date for "today": the sales day rolls over at 06:00.
pub fn default_as_of() -> NaiveDate {
    let lagos = chrono::Utc::now() + Duration::hours(1);
    let date = lagos.date_naive();
    if lagos.time().hour() < 6 {
        date - Duration::days(1)
    } else {
        date
    }
}
use chrono::Timelike;

/// Database names that may hold the demo. Anything else is refused.
fn allowed_database(name: &str) -> bool {
    ["_dev", "_demo", "_test", "_verify", "_fresh", "_local"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
}

async fn current_state(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
) -> Result<(Option<Option<String>>, Option<Value>)> {
    let fixture: Option<Option<String>> =
        sqlx::query_scalar("SELECT fixture FROM workspaces WHERE id=$1")
            .bind(workspace)
            .fetch_optional(&mut **tx)
            .await?;
    let metadata: Option<Value> = sqlx::query_scalar("SELECT metadata FROM dataset_imports WHERE workspace_id=$1 ORDER BY imported_at DESC LIMIT 1")
        .bind(workspace)
        .fetch_optional(&mut **tx)
        .await?;
    Ok((fixture, metadata))
}

/// Non-destructive: creates the demo workspace when it does not exist. A
/// workspace already holding this exact dataset is a no-op; anything else is
/// refused with instructions.
pub async fn init(pool: &PgPool, workspace: &str, options: &Options) -> Result<Value> {
    let mut tx = pool.begin().await?;
    lock(&mut tx, workspace).await?;
    let (fixture, metadata) = current_state(&mut tx, workspace).await?;
    match (fixture, metadata) {
        (None, None) => {}
        (Some(Some(f)), Some(meta)) if f == "synthetic_demo" => {
            if meta["seed"].as_u64() == Some(options.seed)
                && meta["to"].as_str() == Some(&options.as_of.to_string())
            {
                return Ok(
                    json!({"status":"already_initialized","workspace":workspace,"as_of":options.as_of,"seed":options.seed}),
                );
            }
            bail!(
                "Workspace {workspace} holds a synthetic dataset with a different seed or anchor ({} / {}); run `demo-reset --yes` to replace it",
                meta["seed"],
                meta["to"]
            );
        }
        _ => bail!(
            "Workspace {workspace} already contains data that the demo generator does not own; run `demo-reset --yes --replace-imported` only if you intend to replace it (back it up first)"
        ),
    }
    let summary = populate(&mut tx, workspace, options).await?;
    tx.commit().await?;
    Ok(
        json!({"status":"initialized","workspace":workspace,"as_of":options.as_of,"seed":options.seed,"summary":summary}),
    )
}

/// Destructive, explicit and guarded: the database name must be a local/demo
/// name and the workspace must be demo-owned unless `replace_imported` is set.
/// Everything derived from the old data (runs, events, artifacts, conversations,
/// orders, audit rows, checkpoints) is removed for this workspace only.
pub async fn reset(
    pool: &PgPool,
    workspace: &str,
    options: &Options,
    replace_imported: bool,
) -> Result<Value> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;
    let local: bool = sqlx::query_scalar("SELECT inet_server_addr() IS NULL OR inet_server_addr() <<= inet '127.0.0.0/8' OR inet_server_addr() = inet '::1'")
        .fetch_one(pool).await?;
    ensure!(
        local,
        "Demo resets are restricted to a local PostgreSQL server"
    );
    ensure!(
        allowed_database(&database),
        "Refusing to reset database {database}: demo resets only run against a local database whose name ends with _dev, _demo, _test, _verify, _fresh or _local"
    );
    let mut tx = pool.begin().await?;
    lock(&mut tx, workspace).await?;
    let (fixture, _) = current_state(&mut tx, workspace).await?;
    let owned = matches!(fixture, Some(Some(ref f)) if f == "synthetic_demo") || fixture.is_none();
    ensure!(
        owned || replace_imported,
        "Workspace {workspace} holds imported (non-synthetic) data; pass --replace-imported to replace it after taking a backup"
    );
    let removed = clear(&mut tx, workspace).await?;
    let summary = populate(&mut tx, workspace, options).await?;
    tx.commit().await?;
    Ok(
        json!({"status":"reset","database":database,"workspace":workspace,"as_of":options.as_of,"seed":options.seed,"removed":removed,"summary":summary}),
    )
}

async fn lock(tx: &mut Transaction<'_, Postgres>, workspace: &str) -> Result<()> {
    let idle: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("demo-runtime:{workspace}"))
            .fetch_one(&mut **tx)
            .await?;
    ensure!(
        idle,
        "Stop the backend before initializing or resetting demo data"
    );
    // Same advisory lock as the snapshot importer, plus the agent role rows so a
    // running monitor check finishes before the data underneath it changes.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(format!("snapshot:{workspace}"))
        .execute(&mut **tx)
        .await?;
    sqlx::query("SELECT 1 FROM scoped_agents WHERE workspace_id=$1 FOR UPDATE")
        .bind(workspace)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn clear(tx: &mut Transaction<'_, Postgres>, workspace: &str) -> Result<Value> {
    // Fence in-flight agent work first: cancelled runs cannot write results or events.
    let cancelled = sqlx::query("UPDATE agent_runs SET status='cancelled',lease_token=NULL,lease_until=NULL,updated_at=now() WHERE workspace_id=$1 AND status IN ('queued','running','paused')")
        .bind(workspace).execute(&mut **tx).await?.rows_affected();
    let mut removed = serde_json::Map::new();
    removed.insert("cancelled_runs".into(), json!(cancelled));
    for (label, sql) in [
        (
            "purchase_order_events",
            "DELETE FROM purchase_order_events WHERE workspace_id=$1",
        ),
        (
            "purchase_order_lines",
            "DELETE FROM purchase_order_lines WHERE workspace_id=$1",
        ),
        (
            "purchase_orders",
            "DELETE FROM purchase_orders WHERE workspace_id=$1",
        ),
        (
            "purchase_order_counters",
            "DELETE FROM purchase_order_counters WHERE workspace_id=$1",
        ),
        (
            "purchasing_config_events",
            "DELETE FROM purchasing_config_events WHERE workspace_id=$1",
        ),
        (
            "vendor_items",
            "DELETE FROM vendor_items WHERE workspace_id=$1",
        ),
        ("vendors", "DELETE FROM vendors WHERE workspace_id=$1"),
        (
            "purchasing_policies",
            "DELETE FROM purchasing_policies WHERE workspace_id=$1",
        ),
        ("artifacts", "DELETE FROM artifacts WHERE workspace_id=$1"),
        (
            "agent_events",
            "DELETE FROM agent_events WHERE run_id IN (SELECT id FROM agent_runs WHERE workspace_id=$1)",
        ),
        ("agent_runs", "DELETE FROM agent_runs WHERE workspace_id=$1"),
        (
            "conversations",
            "DELETE FROM conversations WHERE workspace_id=$1",
        ),
        (
            "scoped_agent_checks",
            "DELETE FROM scoped_agent_checks WHERE workspace_id=$1",
        ),
        (
            "inventory_movement_requests",
            "DELETE FROM inventory_movement_requests WHERE workspace_id=$1",
        ),
        (
            "inventory_movements",
            "DELETE FROM inventory_movements WHERE workspace_id=$1",
        ),
        (
            "inventory_items",
            "DELETE FROM inventory_items WHERE workspace_id=$1",
        ),
        (
            "sales_lines",
            "DELETE FROM sales_lines WHERE workspace_id=$1",
        ),
        (
            "sales_tickets",
            "DELETE FROM sales_tickets WHERE workspace_id=$1",
        ),
        ("menu_items", "DELETE FROM menu_items WHERE workspace_id=$1"),
        (
            "dataset_imports",
            "DELETE FROM dataset_imports WHERE workspace_id=$1",
        ),
    ] {
        let count = sqlx::query(sql)
            .bind(workspace)
            .execute(&mut **tx)
            .await?
            .rows_affected();
        removed.insert(label.into(), json!(count));
    }
    // Predictable starting state: both agents enabled, checkpoints cleared, one
    // fresh revision so the first check runs once against the new data.
    sqlx::query("UPDATE scoped_agents SET enabled=true,checked_revision=-1,last_checked_at=NULL,observation=NULL,updated_at=now() WHERE workspace_id=$1")
        .bind(workspace).execute(&mut **tx).await?;
    sqlx::query("UPDATE agent_data_revisions SET revision=0 WHERE workspace_id=$1")
        .bind(workspace)
        .execute(&mut **tx)
        .await?;
    Ok(Value::Object(removed))
}

async fn populate(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    options: &Options,
) -> Result<Value> {
    ensure!(MENU.len() == 20, "fixture must hold 20 menu items");
    ensure!(ITEMS.len() == 50, "fixture must hold 50 inventory items");
    let from = options.as_of - Duration::days(SALES_DAYS - 1);
    sqlx::query("INSERT INTO workspaces(id,name,fixture) VALUES($1,$2,'synthetic_demo') ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,fixture='synthetic_demo'")
        .bind(workspace).bind(RESTAURANT_NAME).execute(&mut **tx).await?;
    // Menu
    for m in MENU {
        let portions = json!([{"name":"Regular","multiplier":1,"prices":[{"priceTag":"Standard","price":m.price.parse::<f64>()?}]}]);
        sqlx::query("INSERT INTO menu_items(workspace_id,id,name,category,portions,price_min,price_max,source) VALUES($1,$2,$3,$4,$5,$6,$6,$7)")
            .bind(workspace).bind(m.id).bind(m.name).bind(m.category).bind(&portions).bind(d(m.price)).bind(json!({"synthetic":true}))
            .execute(&mut **tx).await?;
    }
    // Vendors and policy
    let mut vendor_ids = std::collections::HashMap::new();
    for (index, v) in VENDORS.iter().enumerate() {
        let id = Uuid::from_u128(
            0x5E_ED00_0000_0000_0000_0000_0000_0000
                + (options.seed as u128) * 1000
                + index as u128
                + 1,
        );
        sqlx::query("INSERT INTO vendors(workspace_id,id,name,contact_name,email,phone,notes,source) VALUES($1,$2,$3,$4,$5,$6,$7,'local_fixture')")
            .bind(workspace).bind(id).bind(v.name).bind(v.contact).bind(v.email).bind(v.phone)
            .bind("Fictional supplier from the synthetic demo dataset. Prices and contacts are invented; the .invalid domain cannot receive mail.")
            .execute(&mut **tx).await?;
        vendor_ids.insert(v.key, id);
    }
    sqlx::query("INSERT INTO purchasing_policies(workspace_id,auto_approve_limit,approval_limit) VALUES($1,$2::numeric,$3::numeric)")
        .bind(workspace).bind(POLICY_AUTO_APPROVE).bind(POLICY_APPROVAL_LIMIT).execute(&mut **tx).await?;
    // Inventory, opening movements and vendor rules
    let opening_date = from - Duration::days(1);
    let mut assigned = 0;
    for it in ITEMS {
        let par = d(it.par);
        ensure!(
            par > Decimal::ZERO,
            "every fixture item has a positive par level"
        );
        let unit_cost = if it.pack_price.is_empty() {
            None
        } else {
            Some((d(it.pack_price) / d(it.units_per_pack)).round_dp(2))
        };
        let vendor_name = if it.vendor.is_empty() {
            None
        } else {
            VENDORS.iter().find(|v| v.key == it.vendor).map(|v| v.name)
        };
        sqlx::query("INSERT INTO inventory_items(workspace_id,id,name,category,unit,par_level,current_balance,unit_cost,supplier,needs_review,source) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,false,$10)")
            .bind(workspace).bind(it.id).bind(it.name).bind(it.category).bind(it.unit).bind(par).bind(d(it.balance)).bind(unit_cost).bind(vendor_name)
            .bind(json!({"synthetic":true})).execute(&mut **tx).await?;
        sqlx::query("INSERT INTO inventory_movements(workspace_id,id,item_id,business_date,movement_type,quantity_delta,source,reason,balance_after,recorded_at) VALUES($1,$2,$3,$4,'opening',$5,$6,'Opening balance (synthetic demo)',$5,$7)")
            .bind(workspace).bind(format!("{}-opening", it.id)).bind(it.id).bind(opening_date).bind(d(it.balance)).bind(json!({"synthetic":true}))
            .bind(opening_date.and_hms_opt(6, 0, 0).unwrap().and_utc()).execute(&mut **tx).await?;
        if let Some(vendor) = vendor_ids.get(it.vendor) {
            sqlx::query("INSERT INTO vendor_items(workspace_id,item_id,vendor_id,supplier_reference,order_unit,units_per_pack,pack_price,minimum_order_quantity,reorder_target,preferred,source) VALUES($1,$2,$3,$4,$5,$6::numeric,$7::numeric,$8::numeric,NULL,true,'local_fixture')")
                .bind(workspace).bind(it.id).bind(vendor).bind(format!("SKU-{}", it.id.to_uppercase())).bind(it.order_unit).bind(it.units_per_pack).bind(it.pack_price).bind(it.minimum)
                .execute(&mut **tx).await?;
            assigned += 1;
        }
    }
    // Sales: 61 business days ending at the anchor. Weekend evenings are busier;
    // the most recent complete Monday–Sunday week runs about 15% above the one before it.
    let mut rng = Rng(options.seed);
    let weights: Vec<u32> = MENU.iter().map(|m| m.weight).collect();
    let (recent_week, previous_week) = complete_weeks(options.as_of);
    let mut ticket_id: i64 = 100_000;
    let mut line_id: i64 = 1_000_000;
    let mut tickets = 0usize;
    let mut lines = 0usize;
    let mut voided = 0usize;
    let base_seed = options.seed;
    for offset in 0..SALES_DAYS {
        let day = from + Duration::days(offset);
        let weekday_factor = match day.weekday() {
            Weekday::Fri => 130,
            Weekday::Sat => 145,
            Weekday::Sun => 115,
            Weekday::Mon => 80,
            _ => 100,
        };
        // The most recent complete week runs ~15% above every other week, including the previous one.
        let week_factor = if recent_week.contains(&day) { 115 } else { 100 };
        debug_assert!(!(recent_week.contains(&day) && previous_week.contains(&day)));
        let mut count = 16 * weekday_factor * week_factor / 10_000;
        count += rng.below(4) as i64 as usize;
        if day == options.as_of {
            count = count / 2 + 1; // today is still in progress
        }
        for _ in 0..count {
            ticket_id += 1;
            let line_count = 1 + rng.weighted(&[35, 40, 18, 7]);
            let mut total = Decimal::ZERO;
            let mut ticket_lines = Vec::new();
            for _ in 0..line_count {
                let m = &MENU[rng.weighted(&weights)];
                let qty = Decimal::from(1 + rng.weighted(&[75, 20, 5]) as u32);
                let price = d(m.price);
                total += price * qty;
                ticket_lines.push((m, qty, price));
            }
            let void = rng.below(50) == 0; // ~2% voided: total 0, excluded from gross sales
            let total_amount = if void { Decimal::ZERO } else { total };
            if void {
                voided += 1;
            }
            let hour = 11 + rng.below(12); // 11:00–22:59 local time
            let source = json!({"synthetic":true,"seed":base_seed,"localTime":format!("{}T{:02}:00:00",day,hour),"voided":void});
            sqlx::query("INSERT INTO sales_tickets(workspace_id,id,ticket_number,business_date,total_amount,source) VALUES($1,$2,$3,$4,$5,$6)")
                .bind(workspace).bind(ticket_id).bind(format!("T{ticket_id}")).bind(day).bind(total_amount).bind(source)
                .execute(&mut **tx).await?;
            tickets += 1;
            for (m, qty, price) in ticket_lines {
                line_id += 1;
                sqlx::query("INSERT INTO sales_lines(workspace_id,id,ticket_id,item_name,category,quantity,unit_price,billable,source) VALUES($1,$2,$3,$4,$5,$6,$7,true,$8)")
                    .bind(workspace).bind(line_id).bind(ticket_id).bind(m.name).bind(m.category).bind(qty).bind(price).bind(json!({"synthetic":true,"menuItemId":m.id}))
                    .execute(&mut **tx).await?;
                lines += 1;
            }
        }
    }
    let metadata = json!({
        "synthetic": true, "generator": "backhaus-ai-backend demo-init", "seed": options.seed, "restaurant": RESTAURANT_NAME,
        "orgId": workspace, "currency": "NGN", "timezone": "Africa/Lagos", "businessDayCutoffHour": 6,
        "from": from, "to": options.as_of, "exportedAt": options.as_of.and_hms_opt(23, 0, 0).unwrap().and_utc(),
        "sources": ["Synthetic demo dataset generated by the application; no production data"],
        "sales_ticket_count": tickets, "sales_line_count": lines, "voided_tickets": voided, "days_with_records": SALES_DAYS,
        "inventory_item_count": ITEMS.len(), "menu_item_count": MENU.len(), "vendor_count": VENDORS.len(), "vendor_assignments": assigned,
        "complete_weeks": {"recent": {"from": recent_week.start, "to": recent_week.end}, "previous": {"from": previous_week.start, "to": previous_week.end}},
        "missing_days_are_unknown": false,
        "intended_scenario": {
            "automatic_approval": "Lemons: 18 pcs on hand, par 30, bag of 10 at NGN 1,500 → 2 bags = NGN 3,000",
            "manual_approval": "Chicken Breast: 8 kg on hand, par 20, 5 kg pack at NGN 9,000 → 3 packs = NGN 27,000",
            "blocked_by_approval_limit": "Premium Whisky: 2 bottles on hand, par 12, NGN 45,000 each → 10 bottles = NGN 450,000",
            "missing_supplier": "Palm Oil: 6 L on hand, par 15, no supplier assigned",
            "demo_adjustment": "Basmati Rice: 40 kg on hand, par 25; issue 20 kg → 20 kg, short 5 → one 25 kg bag = NGN 42,000 (manual approval)"
        }
    });
    sqlx::query("INSERT INTO dataset_imports(id,workspace_id,sha256,metadata) VALUES($1,$2,$3,$4)")
        .bind(Uuid::new_v4())
        .bind(workspace)
        .bind(format!("synthetic:{}:{}", options.seed, options.as_of))
        .bind(&metadata)
        .execute(&mut **tx)
        .await?;
    Ok(
        json!({"menu_items":MENU.len(),"inventory_items":ITEMS.len(),"vendors":VENDORS.len(),"vendor_assignments":assigned,"tickets":tickets,"lines":lines,"voided_tickets":voided,"from":from,"to":options.as_of}),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Week {
    pub start: NaiveDate,
    pub end: NaiveDate,
}
impl Week {
    fn contains(&self, day: &NaiveDate) -> bool {
        *day >= self.start && *day <= self.end
    }
}
/// The most recent complete Monday–Sunday week ending strictly before `as_of`
/// (a partial current week never counts), and the week before it.
pub fn complete_weeks(as_of: NaiveDate) -> (Week, Week) {
    let days_since_monday = as_of.weekday().num_days_from_monday() as i64;
    let this_monday = as_of - Duration::days(days_since_monday);
    let recent_end = this_monday - Duration::days(1);
    let recent = Week {
        start: recent_end - Duration::days(6),
        end: recent_end,
    };
    let previous = Week {
        start: recent.start - Duration::days(7),
        end: recent.start - Duration::days(1),
    };
    (recent, previous)
}

pub fn parse_options(args: &[String]) -> Result<(Options, bool, bool)> {
    let mut as_of = default_as_of();
    let mut seed = DEFAULT_SEED;
    let mut yes = false;
    let mut replace_imported = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--as-of" => {
                let value = iter.next().context("--as-of needs YYYY-MM-DD")?;
                as_of = NaiveDate::parse_from_str(value, "%Y-%m-%d")
                    .context("--as-of must be YYYY-MM-DD")?;
            }
            "--seed" => {
                seed = iter
                    .next()
                    .context("--seed needs a number")?
                    .parse()
                    .context("--seed must be an integer")?
            }
            "--yes" => yes = true,
            "--replace-imported" => replace_imported = true,
            other => bail!("Unknown option {other}"),
        }
    }
    Ok((Options { as_of, seed }, yes, replace_imported))
}
