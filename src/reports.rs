use crate::{
    config::Config,
    data::{self, DateRange, InventoryQuery},
    error::{Error, Result},
    jobs::Run,
};
use docx_rs::{Docx, Paragraph, Run as TextRun, Table, TableCell, TableRow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{io::Cursor, time::Duration};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportFormat {
    Csv,
    Docx,
    Pdf,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportKind {
    Sales,
    Inventory,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReportRequest {
    pub kind: ReportKind,
    pub format: ReportFormat,
    pub range: Option<DateRange>,
}
#[derive(Clone, Debug, Serialize)]
pub struct ReportData {
    pub title: String,
    pub note: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

pub async fn collect(pool: &PgPool, workspace: &str, args: &ReportRequest) -> Result<ReportData> {
    let (title, note, headers, rows) = match args.kind {
        ReportKind::Sales => {
            let range = args
                .range
                .as_ref()
                .ok_or_else(|| Error::Invalid("Sales reports require a date range".into()))?;
            let value = data::sales(pool, workspace, range).await?;
            let rows = value["days"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    vec![
                        r["date"].as_str().unwrap().into(),
                        r["tickets"].to_string(),
                        r["gross_line_sales"].as_str().unwrap().into(),
                        r["ticket_total"].as_str().unwrap().into(),
                    ]
                })
                .collect();
            (
                format!("Sales report: {} to {}", range.from, range.to),
                format!(
                    "Currency: NGN. Gross line sales: {}. Ticket totals: {}. These are separate measures. Dates without records are not confirmed zero-sales days.",
                    value["gross_line_sales"].as_str().unwrap(),
                    value["ticket_total"].as_str().unwrap()
                ),
                vec![
                    "Business date",
                    "Tickets",
                    "Gross line sales (NGN)",
                    "Ticket total (NGN)",
                ],
                rows,
            )
        }
        ReportKind::Inventory => {
            let value = data::inventory(
                pool,
                workspace,
                &InventoryQuery {
                    limit: Some(100),
                    ..Default::default()
                },
            )
            .await?;
            let rows = value["items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    vec![
                        r["name"].as_str().unwrap().into(),
                        r["unit"].as_str().unwrap_or("Unknown").into(),
                        r["current_balance"].as_str().unwrap().into(),
                        r["par_level"].as_str().unwrap_or("Not configured").into(),
                    ]
                })
                .collect();
            ("Inventory snapshot".into(),"Current recorded balances for every inventory item. Missing par levels require configuration.".into(),vec!["Item","Unit","Balance","Par level"],rows)
        }
    };
    Ok(ReportData {
        title,
        note,
        headers: headers.into_iter().map(String::from).collect(),
        rows,
    })
}
fn csv_cell(value: &str) -> String {
    if value
        .trim_start()
        .starts_with(['=', '+', '-', '@', '\t', '\r'])
    {
        format!("'{value}")
    } else {
        value.into()
    }
}
pub fn render_csv(data: &ReportData) -> Result<Vec<u8>> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer
        .write_record(&data.headers)
        .map_err(|_| Error::Report)?;
    for row in &data.rows {
        writer
            .write_record(row.iter().map(|s| csv_cell(s)))
            .map_err(|_| Error::Report)?;
    }
    writer.into_inner().map_err(|_| Error::Report)
}
pub fn render_docx(data: &ReportData) -> Result<Vec<u8>> {
    let paragraph = |s: &str| Paragraph::new().add_run(TextRun::new().add_text(s));
    let mut rows = vec![TableRow::new(
        data.headers
            .iter()
            .map(|s| {
                TableCell::new()
                    .add_paragraph(Paragraph::new().add_run(TextRun::new().add_text(s).bold()))
            })
            .collect(),
    )];
    rows.extend(data.rows.iter().map(|r| {
        TableRow::new(
            r.iter()
                .map(|s| TableCell::new().add_paragraph(paragraph(s)))
                .collect(),
        )
    }));
    let doc = Docx::new()
        .add_paragraph(paragraph(&data.title))
        .add_paragraph(paragraph(&data.note))
        .add_table(Table::new(rows));
    let mut bytes = Cursor::new(Vec::new());
    doc.build().pack(&mut bytes).map_err(|_| Error::Report)?;
    Ok(bytes.into_inner())
}
pub async fn render_pdf(data: &ReportData, typst_bin: &str) -> Result<Vec<u8>> {
    let dir = tempfile::tempdir().map_err(|_| Error::Report)?;
    tokio::fs::write(
        dir.path().join("data.json"),
        serde_json::to_vec(data).map_err(|_| Error::Report)?,
    )
    .await
    .map_err(|_| Error::Report)?;
    // Fixed template: model and source text is passed as JSON, never Typst code.
    let template = r##"#let report = json("data.json")
#set page(paper: "a4", margin: 18mm)
#set text(size: 9pt)
#set heading(numbering: none)
#heading(level: 1, report.title)
#text(report.note)
#v(12pt)
#table(
  columns: report.headers.len(),
  inset: 6pt,
  stroke: 0.4pt + rgb("dddddd"),
  table.header(..report.headers.map(h => strong(h))),
  ..report.rows.flatten().map(cell => text(cell)),
)
"##;
    let input = dir.path().join("report.typ");
    let output = dir.path().join("report.pdf");
    tokio::fs::write(&input, template)
        .await
        .map_err(|_| Error::Report)?;
    let mut command = tokio::process::Command::new(typst_bin);
    command
        .args(["compile", "--root"])
        .arg(dir.path())
        .arg(&input)
        .arg(&output)
        .kill_on_drop(true)
        .env_remove("TYPST_FONT_PATHS");
    let result = tokio::time::timeout(Duration::from_secs(30), command.output())
        .await
        .map_err(|_| Error::Unavailable("PDF renderer timed out".into()))?
        .map_err(|_| {
            Error::Unavailable("Install Typst or set TYPST_BIN to enable PDF reports".into())
        })?;
    if !result.status.success() {
        return Err(Error::Report);
    }
    tokio::fs::read(output).await.map_err(|_| Error::Report)
}
pub static REPORT_SLOTS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

/// Render a purchase order with Typst. The document data is passed as JSON,
/// never interpolated into the template as code.
pub async fn render_purchase_order_pdf(order: &Value, typst_bin: &str) -> Result<Vec<u8>> {
    let dir = tempfile::tempdir().map_err(|_| Error::Report)?;
    tokio::fs::write(
        dir.path().join("order.json"),
        serde_json::to_vec(order).map_err(|_| Error::Report)?,
    )
    .await
    .map_err(|_| Error::Report)?;
    let template = r##"#let o = json("order.json")
#set page(paper: "a4", margin: 18mm)
#set text(size: 10pt)
#let muted = rgb("666666")
#grid(columns: (1fr, 1fr), gutter: 18pt, align: (left, right),
  [#text(size: 16pt, weight: "bold")[Purchase order #o.number]
   #v(4pt)
   #text(size: 9pt, fill: muted)[#o.restaurant]],
  [#text(size: 12pt, weight: "bold")[#o.status_label]
   #v(4pt)
   #text(size: 9pt, fill: muted)[#o.prepared_label]])
#v(10pt)
#block(inset: (y: 6pt))[
  #text(size: 10pt, weight: "bold", fill: rgb("b84820"))[#o.watermark]
]
#v(8pt)
#grid(columns: (1fr, 1fr), gutter: 18pt,
  [#text(weight: "bold")[Vendor]
   #v(4pt)
   #o.vendor.name
   #v(3pt)
   #text(size: 9pt, fill: muted)[#o.vendor.contact]],
  [#text(weight: "bold")[Order details]
   #v(4pt)
   #for line in o.details [#line #linebreak()]])
#v(12pt)
#table(
  columns: (1.3fr, 1fr, 1.2fr, 0.9fr, 0.9fr),
  inset: 6pt,
  stroke: 0.4pt + rgb("dddddd"),
  align: (left, right, right, right, right),
  table.header([*Item*], [*Packs*], [*Units*], [*Price per pack*], [*Line total*]),
  ..o.lines.map(l => (l.item, l.packs, l.units, l.price, l.total)).flatten().map(c => text(c)),
)
#v(8pt)
#align(right)[#text(size: 12pt, weight: "bold")[Total #o.currency #o.total]]
#v(12pt)
#for note in o.notes [
  #text(size: 9pt, fill: muted)[#note]
  #v(3pt)
]
"##;
    let input = dir.path().join("order.typ");
    let output = dir.path().join("order.pdf");
    tokio::fs::write(&input, template)
        .await
        .map_err(|_| Error::Report)?;
    let mut command = tokio::process::Command::new(typst_bin);
    command
        .args(["compile", "--root"])
        .arg(dir.path())
        .arg(&input)
        .arg(&output)
        .kill_on_drop(true)
        .env_remove("TYPST_FONT_PATHS");
    let result = tokio::time::timeout(Duration::from_secs(30), command.output())
        .await
        .map_err(|_| Error::Unavailable("PDF renderer timed out".into()))?
        .map_err(|_| {
            Error::Unavailable("Install Typst or set TYPST_BIN to enable PDF output".into())
        })?;
    if !result.status.success() {
        tracing::warn!(stderr=%String::from_utf8_lossy(&result.stderr).chars().take(500).collect::<String>(), "Typst failed");
        return Err(Error::Report);
    }
    tokio::fs::read(output).await.map_err(|_| Error::Report)
}

pub async fn generate(
    pool: &PgPool,
    config: &Config,
    workspace: &str,
    run: Option<&Run>,
    args: &ReportRequest,
) -> Result<Value> {
    let hash = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(args).map_err(|_| Error::Report)?)
    );
    let data = collect(pool, workspace, args).await?;
    if data.rows.is_empty() {
        let available_range = match args.kind {
            ReportKind::Sales => sqlx::query_scalar::<_, Value>(
                "SELECT CASE WHEN COUNT(*)=0 THEN 'null'::jsonb ELSE jsonb_build_object('from',MIN(business_date),'to',MAX(business_date)) END FROM sales_tickets WHERE workspace_id=$1"
            ).bind(workspace).fetch_one(pool).await?,
            ReportKind::Inventory => Value::Null,
        };
        let message = match &args.range {
            Some(range) if matches!(args.kind, ReportKind::Sales) => format!(
                "No sales records found for {} to {}. No report was created.",
                range.from, range.to
            ),
            _ => "No inventory records found. No report was created.".into(),
        };
        return Ok(
            json!({"status":"no_data","message":message,"kind":args.kind,"requested_range":args.range,"available_range":available_range}),
        );
    }
    let _permit = REPORT_SLOTS
        .try_acquire()
        .map_err(|_| Error::Unavailable("Report renderer is busy; retry shortly".into()))?;
    let (bytes, ext, media) = match args.format {
        ReportFormat::Csv => (render_csv(&data)?, "csv", "text/csv; charset=utf-8"),
        ReportFormat::Docx => {
            let copy = data.clone();
            let bytes = tokio::task::spawn_blocking(move || render_docx(&copy))
                .await
                .map_err(|_| Error::Report)??;
            (
                bytes,
                "docx",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )
        }
        ReportFormat::Pdf => (
            render_pdf(&data, &config.typst_bin).await?,
            "pdf",
            "application/pdf",
        ),
    };
    if bytes.len() > 10 * 1024 * 1024 {
        return Err(Error::Invalid("Report exceeds 10 MiB limit".into()));
    }
    let mut tx = pool.begin().await?;
    if let Some(run) = run {
        let owned=sqlx::query_scalar::<_,Uuid>("SELECT id FROM agent_runs WHERE id=$1 AND workspace_id=$2 AND lease_token=$3 AND status='running' AND lease_until>now() FOR UPDATE").bind(run.id).bind(workspace).bind(run.lease).fetch_optional(&mut *tx).await?;
        if owned.is_none() {
            return Err(Error::Conflict(
                "Run paused, cancelled, or lease lost".into(),
            ));
        }
    }
    let id = Uuid::new_v4();
    let metadata = json!({"title":data.title,"note":data.note,"row_count":data.rows.len(),"format":ext,"request":args});
    let id:Uuid=sqlx::query_scalar("INSERT INTO artifacts(id,workspace_id,run_id,request_hash,filename,media_type,bytes,metadata) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (workspace_id,run_id,request_hash) DO UPDATE SET request_hash=EXCLUDED.request_hash RETURNING id")
        .bind(id).bind(workspace).bind(run.map(|r|r.id)).bind(hash).bind(format!("backhaus-report.{ext}")).bind(media).bind(bytes).bind(&metadata).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(json!({"artifact_id":id,"download_url":format!("/v1/artifacts/{id}"),"metadata":metadata}))
}
