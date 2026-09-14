use anyhow::{Context, Result, bail};
use backhaus_ai_backend::{config::Config, import, runtime};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "backhaus_ai_backend=info,tower_http=info".into()),
        )
        .init();
    let config = Arc::new(Config::from_env()?);
    let pool = PgPoolOptions::new()
        .max_connections(config.db_max_connections)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='15s'")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET idle_in_transaction_session_timeout='30s'")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&config.database_url)
        .await
        .context("Cannot connect to configured PostgreSQL database")?;
    match std::env::args().nth(1).as_deref() {
        Some("migrate") => {
            sqlx::migrate!().run(&pool).await?;
            println!("Migrations applied.");
        }
        Some("import" | "import-menu") => {
            let path = std::env::args()
                .nth(2)
                .context("Usage: backhaus-ai-backend import /path/to/snapshot.json")?;
            let metadata = tokio::fs::metadata(&path).await?;
            anyhow::ensure!(
                metadata.len() <= 100 * 1024 * 1024,
                "Snapshot exceeds 100 MiB"
            );
            let bytes = tokio::fs::read(path).await?;
            let result = if std::env::args().nth(1).as_deref() == Some("import-menu") {
                import::menu_snapshot(&pool, &config.workspace_id, &bytes).await?
            } else {
                import::snapshot(&pool, &config.workspace_id, &bytes).await?
            };
            println!("{result}");
        }
        Some("demo-init") => {
            let args: Vec<String> = std::env::args().skip(2).collect();
            let (options, _, _) = backhaus_ai_backend::demo::parse_options(&args)?;
            let result =
                backhaus_ai_backend::demo::init(&pool, &config.workspace_id, &options).await?;
            println!("{result}");
        }
        Some("demo-reset") => {
            let args: Vec<String> = std::env::args().skip(2).collect();
            let (options, yes, replace_imported) = backhaus_ai_backend::demo::parse_options(&args)?;
            anyhow::ensure!(
                yes,
                "demo-reset deletes this workspace's demo data; pass --yes to confirm (and --replace-imported for non-synthetic data)"
            );
            let result = backhaus_ai_backend::demo::reset(
                &pool,
                &config.workspace_id,
                &options,
                replace_imported,
            )
            .await?;
            println!("{result}");
        }
        None | Some("start" | "serve" | "worker") => {
            let mode = std::env::args().nth(1).unwrap_or_else(|| "start".into());
            if mode == "start" {
                sqlx::migrate!().run(&pool).await?;
            }
            runtime::run(pool.clone(), config, &mode).await?;
        }
        _ => bail!(
            "Usage: backhaus-ai-backend [start | serve | worker | migrate | demo-init [--as-of YYYY-MM-DD] [--seed N] | demo-reset --yes [--replace-imported] [--as-of ...] [--seed N] | import PATH | import-menu PATH]"
        ),
    }
    pool.close().await;
    Ok(())
}
