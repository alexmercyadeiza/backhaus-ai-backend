use crate::{
    config::Config,
    error::{Error, Result},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::PgPool;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettingsInput {
    pub business_name: String,
    pub city: String,
    pub country: String,
    pub reply_to: String,
    pub auto_reply: bool,
    pub version: i32,
}
pub async fn get(pool: &PgPool, workspace: &str, config: &Config) -> Result<Value> {
    let value: Option<Value> = sqlx::query_scalar(
        "SELECT to_jsonb(s)-'workspace_id' FROM business_settings s WHERE workspace_id=$1",
    )
    .bind(workspace)
    .fetch_optional(pool)
    .await?;
    let mut value=value.unwrap_or(json!({"business_name":"Backhaus","city":"","country":"","reply_to":"","auto_reply":true,"version":0}));
    value["email_configured"] = json!(config.resend_key.is_some() && config.resend_from.is_some());
    value["email_from"] = json!(config.resend_from);
    value["search_configured"] = json!(
        config
            .model_base_url
            .as_deref()
            .is_some_and(|u| u.starts_with("https://openrouter.ai/"))
            && !config.model_api_key.is_empty()
    );
    value["location_ready"] = json!(
        !value["city"].as_str().unwrap_or("").is_empty()
            && !value["country"].as_str().unwrap_or("").is_empty()
    );
    Ok(value)
}
pub async fn save(
    pool: &PgPool,
    workspace: &str,
    config: &Config,
    input: SettingsInput,
) -> Result<Value> {
    if input.business_name.trim().is_empty()
        || input.business_name.len() > 120
        || input.city.len() > 100
        || input.country.len() > 100
        || input.reply_to.len() > 200
    {
        return Err(Error::Invalid(
            "Enter a business name and a valid city/country.".into(),
        ));
    }
    if !input.reply_to.is_empty()
        && (!input.reply_to.contains('@')
            || input.reply_to.contains(char::is_whitespace)
            || input.reply_to.contains(['<', '>', '\r', '\n']))
    {
        return Err(Error::Invalid("Enter a receiving email address.".into()));
    }
    let result=sqlx::query("INSERT INTO business_settings(workspace_id,business_name,city,country,reply_to,auto_reply) SELECT $1,$2,$3,$4,$5,$6 WHERE $7=0 ON CONFLICT(workspace_id) DO NOTHING")
        .bind(workspace).bind(input.business_name.trim()).bind(input.city.trim()).bind(input.country.trim()).bind(input.reply_to.trim()).bind(input.auto_reply).bind(input.version).execute(pool).await?;
    if result.rows_affected() == 0 {
        let result=sqlx::query("UPDATE business_settings SET business_name=$2,city=$3,country=$4,reply_to=$5,auto_reply=$6,version=version+1,updated_at=now() WHERE workspace_id=$1 AND version=$7")
            .bind(workspace).bind(input.business_name.trim()).bind(input.city.trim()).bind(input.country.trim()).bind(input.reply_to.trim()).bind(input.auto_reply).bind(input.version).execute(pool).await?;
        if result.rows_affected() == 0 {
            return Err(Error::Conflict(
                "Settings changed. Reload and try again.".into(),
            ));
        }
    }
    get(pool, workspace, config).await
}
