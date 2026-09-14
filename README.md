# Backhaus AI backend

Rust/Axum/Tokio/SQLx/PostgreSQL owns business data, stock monitoring, deterministic purchasing, approvals, reports and durable agent runs. A supervised Node.js worker uses official `@strands-agents/sdk` 1.17.0 with an OpenAI-compatible hosted model. The worker receives no database credentials and invokes validated Rust tools over line-delimited JSON stdio. No Python application service or Rig is used.

## Supported setup

Check out this directory beside `backhaus-ai`. Prerequisites: Rust 1.93+, Node 24+, local PostgreSQL 14+, Typst on PATH for PDFs. From the dashboard directory:

```sh
npm run setup
npm start
```

See `../backhaus-ai/README.md` for complete prerequisite, configuration, reset and combined-start instructions. Setup uses locked dependencies and a public synthetic generator; it does not require private snapshots. PostgreSQL is a local prerequisite, not a subprocess of the launcher.

For a standalone backend after setup:

```sh
cd backhaus-ai-backend
cargo run --locked
```

This starts the API, Finance/Inventory monitors, Procurement inbox processing and two supervised workers (chat and background). Ctrl+C stops all backend components. Standalone commands load this directory's `.env`; the dashboard's private runtime overrides only apply through its scripts. Do not launch standalone over a managed session's occupied API port.

## Dataset commands

```sh
cargo run --locked -- migrate
cargo run --locked -- demo-init --as-of 2026-09-14
cargo run --locked -- demo-reset --yes --as-of 2026-09-14
```

`demo-init` is non-destructive and idempotent for the same seed/date. `demo-reset` explicitly clears this workspace's derived state, regenerates the synthetic data and restores agent checkpoints. Reset requires a local PostgreSQL address, an allowed `_dev`, `_demo`, `_test`, `_verify`, `_fresh` or `_local` database suffix, and synthetic ownership. A session lock rejects initialization/reset while the backend is running. `--replace-imported` is a one-time migration override for an explicitly backed-up local workspace; never use it on production. Normal startup never resets or shifts dates.

`src/demo.rs` defines Harmattan House: 20 menu items in Food/Drinks, 50 inventory records with positive par levels, seven fictional suppliers and 61 days of synthetic sales. All supplier prices/contacts are invented. Seed/anchor metadata is persisted. Obsolete extraction and SQL seed scripts have been removed. The tested `import` and `import-menu` commands remain explicit data-loading utilities; setup does not use them.

## Model and runtime

Private `.env` settings: `DATABASE_URL`, `BACKEND_API_KEY`, `WORKSPACE_ID`; optional `MODEL_BASE_URL`, `MODEL_NAME`, `MODEL_API_KEY`, `MODEL_REQUEST_OPTIONS`. See `.env.example`. Never expose credentials through `VITE_*`. No automatic paid model fallback.

The protocol includes ready/run/text_delta/tool_call/tool_result/completed/failed/cancelled/cancel/shutdown. Rust validates the dispatched run ID and tool scope, keeps leases and retries, and kills/restarts failed workers with backoff. Chat receives priority over queued inventory notes. Superseded queued notes are cancelled; each background review has at most 30 seconds and 300 output tokens. Provider latency still varies. An active background review yields to queued chat at the next one-second monitor tick, without consuming a failure attempt; superseded active reviews are cancelled. Deterministic checks and approvals do not wait for the model.

Model-unconfigured mode supports stock checks, vendor management, orders and approvals; chat returns an explicit unavailable response. There are no fabricated AI notes. Conversation context is bounded to four complete exchanges/12 KB; archived threads are read-only.

## HTTP surface

Business `/v1` routes require either the server-only Bearer credential or a valid tester session cookie. `/v1/auth/login` validates `DEMO_EMAIL` and `DEMO_PASSWORD` from the private environment; `/v1/auth/session` checks the cookie; `/v1/auth/logout` revokes it. Sessions expire after eight hours or a backend restart. Cookie-based writes require the configured dashboard origin. Workspace scope comes from configuration, never model/user payload. `/healthz` is unprotected liveness. Frontend `/api` forwards cookies to `/v1` without injecting service credentials.

| Route | Purpose |
| --- | --- |
| GET `/status`, `/coverage`, `/sales/summary`, `/inventory` | Capabilities and targeted data |
| GET `/tables/{sales,inventory,menu,purchase-orders,vendors}` | Stable 20-row pagination |
| GET `/tables/{kind}/{id}` | Details and bounded related rows |
| POST `/inventory/{id}/movements` | Decimal issue/receipt/count with reason and Idempotency-Key; optional expected balance |
| POST `/vendors`; PATCH `/vendors/{id}` | Supplier creation/version-aware editing |
| PUT/DELETE `/vendors/{id}/items/{item}` | Preferred ordering rules; edits use versions |
| GET `/vendors/unassigned-items` | Items missing a preferred supplier |
| GET/PUT `/purchasing/policy` | Version-aware maximum operator approval limit (automatic approval disabled) |
| POST `/purchase-orders/{id}/{approve,reject}` | Locked, idempotent decisions; dashboard sends reviewed version |
| POST `/purchase-orders/{id}/pdf` | Scoped, cached PDF for the order snapshot |
| GET `/agents`; POST `/agents/{role}/control/{pause,resume}` | Saved monitoring checkpoints |
| POST `/conversations`, `/chat`, `/conversations/{id}/archive` | Durable chat and archive |
| GET `/agents/runs/{id}`, `/agents/runs/{id}/events` | Run state and resumable SSE |
| POST `/reports`; GET `/artifacts/{id}` | CSV/DOCX/PDF and protected downloads |

Tools include inventory reads/findings, order listing/details, supplier rules, sales totals/ranking, exact sales-period comparison, and report generation. Inventory review has a narrower read-only scope. Models cannot execute arbitrary SQL or unrestricted external writes. Chat can dispatch Procurement with find_vendors; outreach requires the explicit approval flow. Counts come from the actual workspace. Reports return `no_data` with no artifact when a requested sales period has no records.

## Purchasing behavior

Rust groups uncovered shortages by preferred supplier, rounds up to whole packs, enforces minimum quantities, and stores a draft per vendor. Missing supplier/price/par is surfaced. Changes revise drafts; no-longer-needed drafts are withdrawn. Rejected identical proposals stay held until relevant line conditions change. Approved quantities count as on order; receipt reconciliation is intentionally not implemented yet.

Automatic approval applies to complete drafts at or below its threshold. Zero disables it. The maximum operator limit blocks higher totals, even when requested directly over HTTP. Both use exact decimal NGN arithmetic. Approved vendor details and lines are snapshots. Editing a supplier must not change an approved order or PDF. Decisions and configuration edits have audit records. Identity is currently `dashboard-operator`, not a multi-user permission system.

Safety and scale boundaries: purchasing checks examine at most 500 items (larger workspaces fail explicitly), related table rows are paginated, tool results are bounded, reports are size/time/concurrency bounded. No claim of million-row production testing is made. Runtime reset coordination holds one extra PostgreSQL connection while active.

## Tests

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
TEST_DATABASE_URL=postgres://your_local_role@127.0.0.1/backhaus_ai_test BACKHAUS_LIVE_MODEL_TEST=0 cargo test --locked -- --include-ignored
cd worker && npm run check
```

Create the dedicated `backhaus_ai_test` database first. Tests use isolated workspaces. The optional live-model guard is not a paid request unless explicitly enabled. `../backhaus-ai/scripts/verify-demo.mjs` is a destructive-to-fixture HTTP acceptance test intended only for a fresh disposable instance; see the dashboard README.

See the dashboard's DEMO.md, architecture.md and IMPLEMENTATION-HANDOFF.md for the walkthrough and verified outcomes. Vendor sending, negotiation, payment execution, delivery reconciliation, user accounts and hosting are deferred. MIT license. Private `.env`, snapshots, backups and generated artifacts remain excluded from Git.

## Supplier research and enquiries

See the dashboard’s `SUPPLIER-WORKFLOW.md` for the current UI and acceptance flow. Migration 0008 adds business location settings, researched-vendor evidence, supplier enquiries and messages. All new POs require a human decision; Finance starts paused.

New protected routes: GET/PUT `/v1/settings`, GET `/v1/procurement/tasks`, POST `/v1/vendors/research`, POST `/v1/vendors/{id}/selection`, POST `/v1/vendors/{id}/outreach`, GET `/v1/outreach/{id}`, POST `/v1/outreach/{id}/{email,whatsapp,replies,draft,discard}`, POST `/v1/outreach/sync`.

`RESEND_API_KEY` and `RESEND_FROM` remain private in the Rust backend. The first email and operator counter-offers require explicit sending. A configured receiving address is required to collect email replies; receiving polling runs in the existing runtime. WhatsApp is a deep link, not a send API. `vendor_research` and `supplier_reply` runs use Strands with separate Rust tool allowlists. Neither has PO approval or payment tools. Search results are untrusted; contacts need source evidence, and review assessments remain provisional.

Local API bind and dashboard origin are fixed in `src/config.rs`: 127.0.0.1:8090 and http://127.0.0.1:5173. Environment port/origin overrides are ignored. Stop and rerun `cargo run --locked` after changing compiled configuration.

Research retries reload a bounded checkpoint of saved searches, remaining allowance, and suppliers from PostgreSQL. `get_research_source` reads the original evidence without making a new web request. The five-successful-search allowance belongs to the whole run; identical queries reuse saved results, and per-run locking prevents concurrent searches from overspending it. Exhaustion returns the checkpoint so the agent can finish from existing evidence. Rust builds the final supplier/assessment counts from committed rows, retaining model prose separately for audit. Tool failures and worker failure codes are recorded as events.

Regression coverage: `TEST_DATABASE_URL=postgres://apple@127.0.0.1/backhaus_ai_test cargo test --test research_recovery -- --include-ignored`. The optional `BACKHAUS_LIVE_RECOVERY=1` test exercises real Strands/OpenRouter inference against an exhausted synthetic checkpoint; it performs no new web searches and sends no messages.

## Specialist agents and worker queues

Migration 0009 adds Procurement as the third sidebar agent. Finance stays paused; Inventory owns stock and PO drafts; Procurement owns supplier research and reply handling. Each has its own control. Procurement pause also pauses its active/queued model jobs, preserves their checkpoints, and blocks new automatic follow-ups. Already-started provider sends may finish.

The default startup runs two supervised Node/Strands children, one claiming only chat jobs and one claiming only background jobs. They share the existing PostgreSQL pool and retain independent shutdown/restart handling. `find_vendors` starts a separate research job from a chat brief, including supplies outside inventory. One chat run can dispatch at most one research job, reused on retry; `get_supplier_activity` retrieves current progress. Research itself never sends an email.

Verification: the agent_roles suite covers queue isolation with two real child processes, chat dispatch through the stdio protocol, free-text enquiry preservation, idempotency, scope and independent Procurement pause/resume. Opt-in `BACKHAUS_LIVE_CHAT_DISPATCH=1` tests the actual configured model's dispatch without running supplier research or sending mail.
