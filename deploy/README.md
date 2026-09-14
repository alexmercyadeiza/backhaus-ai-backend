# Linux deployment reference

Templates only; no deployment has been performed. Build a release for the destination's x86_64 Linux architecture on a Linux build host. A binary compiled on the Mac is not a Linux binary.

1. Provision a dedicated application database and application role, a `backhaus-ai` OS user, and an authenticated inference endpoint. Never reuse the source app's production database URL for migration/import.
2. Build with `cargo build --release --locked`. Install the binary at `/opt/backhaus-ai-backend/backhaus-ai-backend` and a matching Linux Typst binary on PATH. Install Node.js 24+ and build the Strands worker once (`cd worker && npm ci && npm run build`), then copy `worker/dist` and `worker/node_modules` to `/opt/backhaus-ai-backend/worker/` (or set `WORKER_SCRIPT`). Nothing is installed at service start.
3. Put configuration at `/etc/backhaus-ai/backend.env` with restrictive permissions. Use PostgreSQL TLS when crossing hosts. Bind the API to loopback behind an authenticated reverse proxy. Do not publish the service bearer key in browser assets.
4. The combined `start` command applies migrations before starting the API, monitors and supervised Strands child. Use a dedicated role limited to this application's database with permission to apply its migrations. Review migrations as part of each release.
5. Adapt `backhaus-ai.service` and install it only when the user authorizes deployment. It owns the whole backend process tree; do not add a second worker service. Its 256/512 MiB memory settings apply to the combined service and are initial caps to evaluate, not measured capacity promises. Rust shares its small database pool between API and jobs; the Node child accesses data through Rust tools.
6. Configure TLS, request limits, connection limits, and `proxy_buffering off` for SSE. The event stream sends keep-alives every 15 seconds; use a suitable proxy read timeout.
7. Configure backups, artifact retention, monitoring, and user/session authorization before browser-facing production use.

Keep inference separate from the reference 4 GB server. Do not install or benchmark a model on the live Backhaus server without explicit instructions. A browser automation worker, if later needed for vendor research, should also have a separate resource budget.
