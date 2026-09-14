// Entry point. Rust spawns this process, writes JSON lines to stdin and reads
// JSON lines from stdout. Anything else (SDK logs, diagnostics) goes to stderr.
import { createInterface } from 'node:readline';
import { readFileSync } from 'node:fs';
import { configureLogging } from '@strands-agents/sdk';
import { MAX_LINE_BYTES, PROTOCOL_VERSION, encodeOutbound, parseInbound, type Outbound, type RunRequest } from './protocol.ts';
import { createModel, modelSettingsFromEnv } from './model.ts';
import { executeRun, type ToolOutcome } from './run.ts';

// stdout is the protocol channel. Nothing may print there except encoded messages.
const stdoutWrite = process.stdout.write.bind(process.stdout);
console.log = (...args: unknown[]) => console.error(...args);
console.info = (...args: unknown[]) => console.error(...args);
configureLogging({ debug() {}, info() {}, warn: (...args) => console.error('[strands]', ...args), error: (...args) => console.error('[strands]', ...args) });

const emit = (message: Outbound) => { stdoutWrite(encodeOutbound(message)); };

type Pending = { resolve: (outcome: ToolOutcome) => void; cleanup: () => void };
type Current = { runId: string; abort: AbortController; pending: Map<string, Pending>; finished: Promise<void> };
let current: Current | null = null;
let shuttingDown = false;

function sdkVersion(): string {
  try {
    const manifest = new URL('../node_modules/@strands-agents/sdk/package.json', import.meta.url);
    return String((JSON.parse(readFileSync(manifest, 'utf8')) as { version: string }).version);
  } catch { return 'unknown'; }
}

function startRun(request: RunRequest) {
  if (current) {
    emit({ type: 'failed', run_id: request.run_id, code: 'worker_busy', error: 'Worker already has an active run' });
    return;
  }
  let settings;
  try { settings = modelSettingsFromEnv(); } catch (error) {
    emit({ type: 'failed', run_id: request.run_id, code: 'model_not_configured', error: error instanceof Error ? error.message : 'Model is not configured' });
    return;
  }
  const abort = new AbortController();
  const pending = new Map<string, Pending>();
  const run: Current = { runId: request.run_id, abort, pending, finished: Promise.resolve() };
  current = run;
  const io = {
    emit,
    requestTool(callId: string, name: string, input: unknown, signal: AbortSignal) {
      return new Promise<ToolOutcome>((resolve) => {
        const onAbort = () => finish({ error: 'Tool request cancelled' });
        const finish = (outcome: ToolOutcome) => { pending.delete(callId); signal.removeEventListener('abort', onAbort); resolve(outcome); };
        if (signal.aborted) { finish({ error: 'Tool request cancelled' }); return; }
        pending.set(callId, { resolve: finish, cleanup: () => signal.removeEventListener('abort', onAbort) });
        signal.addEventListener('abort', onAbort, { once: true });
        emit({ type: 'tool_call', run_id: request.run_id, call_id: callId, name, input });
      });
    },
  };
  run.finished = executeRun(request, createModel(settings, request.limits.output_tokens), io, abort.signal)
    .catch((error: unknown) => { emit({ type: 'failed', run_id: request.run_id, code: 'worker_error', error: error instanceof Error ? error.message.slice(0, 500) : 'Worker error' }); })
    .finally(() => { for (const entry of pending.values()) entry.cleanup(); pending.clear(); if (current === run) current = null; if (shuttingDown) exit(0); });
}

function exit(code: number) {
  process.exitCode = code;
  setTimeout(() => process.exit(code), 50).unref();
}

async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  if (current) { current.abort.abort(); await Promise.race([current.finished, new Promise((resolve) => setTimeout(resolve, 3000))]); }
  exit(0);
}

const input = createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on('line', (line) => {
  if (Buffer.byteLength(line) > MAX_LINE_BYTES) { console.error('[worker] dropped oversized line'); return; }
  if (!line.trim()) return;
  let message;
  try { message = parseInbound(line); } catch (error) { console.error(`[worker] invalid message: ${error instanceof Error ? error.message : error}`); return; }
  switch (message.type) {
    case 'run': startRun(message); break;
    case 'tool_result': {
      const entry = current && current.runId === message.run_id ? current.pending.get(message.call_id) : undefined;
      if (!entry) { console.error('[worker] ignored tool_result for unknown call'); break; }
      entry.resolve(message.error !== undefined ? { error: message.error } : { result: message.result });
      break;
    }
    case 'cancel': if (current && current.runId === message.run_id) current.abort.abort(); break;
    case 'shutdown': void shutdown(); break;
  }
});
input.on('close', () => { void shutdown(); });
process.on('SIGTERM', () => { void shutdown(); });
process.on('SIGINT', () => { void shutdown(); });

emit({ type: 'ready', protocol: PROTOCOL_VERSION, sdk: sdkVersion() });
