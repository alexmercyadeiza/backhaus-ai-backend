# Backhaus AI backend

- Target a small Linux application server (reference: 2 vCPU, ~4 GB RAM, shared with existing services). The Mac is a development machine, not the production performance target.
- Rust / Axum / Tokio / SQLx / PostgreSQL, plus a Node.js worker in `worker/` built on the official TypeScript Strands Agents SDK. Rust owns HTTP, workspace scope, database access, tools, jobs/leases, checkpoints, reports and purchasing rules; Strands only orchestrates the model and calls back into Rust over a line-delimited JSON stdio protocol. Host model inference independently via a configurable OpenAI-compatible endpoint (OpenRouter locally); no implicit paid model fallback. Do not introduce Python into the application (`scripts/*.py` and `tests/*.py` are development utilities).
- The source production app is ../Backhaus. Do not import its initialization code, run migrations on its database, or trigger sync/message/payment services. Production data has already been copied to ../backhaus-ai/data/local/.
- Keep local snapshots and secrets out of Git. Currency is NGN. Use decimal amounts; preserve source business days (Lagos 06:00 cutoff).
- User has authorized starting and leaving the local API, worker, and dashboard running for testing. User still tests the UI themselves: do not use browser/computer automation unless asked. Builds, HTTP checks, and isolated database tests are allowed.
- Persist agent runs and events. Apply workspace scope and validation inside tools. External writes require explicit implemented policy checks and duplicate protection; no placeholder sends or fabricated outcomes.
- Purchase-order drafts are prepared in Rust from vendor rules and approved from the dashboard; vendor sending and payment execution are not implemented and must not be faked.
- No unsolicited deployment, live outreach, payment execution, or model download.
