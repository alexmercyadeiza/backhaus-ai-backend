// Line-delimited JSON protocol between the Rust runtime and this worker.
// Rust owns the database, workspace authority, leases and every tool
// implementation. The worker only orchestrates the model and forwards tool
// requests back to Rust. Identifiers supplied by the model are never trusted
// by Rust; it validates each request against the run it dispatched.

export const PROTOCOL_VERSION = 1;
export const MAX_LINE_BYTES = 1_048_576;

export type ToolDefinition = { name: string; description: string; input_schema: Record<string, unknown> };
export type HistoryTurn = { role: 'user' | 'assistant'; text: string };
export type RunLimits = {
  turns: number;
  output_tokens: number;
  max_output_chars: number;
  timeout_ms: number;
  tool_timeout_ms: number;
};
export type RunRequest = {
  type: 'run';
  run_id: string;
  kind: string;
  system_prompt: string;
  message: string;
  history: HistoryTurn[];
  tools: ToolDefinition[];
  limits: RunLimits;
};
export type ToolResultMessage = { type: 'tool_result'; run_id: string; call_id: string; result?: unknown; error?: string };
export type CancelMessage = { type: 'cancel'; run_id: string };
export type ShutdownMessage = { type: 'shutdown' };
export type Inbound = RunRequest | ToolResultMessage | CancelMessage | ShutdownMessage;

export type Outbound =
  | { type: 'ready'; protocol: number; sdk: string }
  | { type: 'text_delta'; run_id: string; text: string }
  | { type: 'tool_call'; run_id: string; call_id: string; name: string; input: unknown }
  | { type: 'completed'; run_id: string; answer: string; stop_reason: string; model_calls: number }
  | { type: 'failed'; run_id: string; code: string; error: string }
  | { type: 'cancelled'; run_id: string };

const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null && !Array.isArray(value);
const isText = (value: unknown, max = 200): value is string => typeof value === 'string' && value.length > 0 && value.length <= max;
const isPositiveInt = (value: unknown, max: number): value is number => Number.isInteger(value) && (value as number) > 0 && (value as number) <= max;
const validName = (value: unknown): value is string => typeof value === 'string' && /^[a-z][a-z0-9_]{0,63}$/.test(value);

export function parseInbound(line: string): Inbound {
  let value: unknown;
  try { value = JSON.parse(line); } catch { throw new Error('Malformed JSON line'); }
  if (!isRecord(value) || typeof value.type !== 'string') throw new Error('Message must be an object with a type');
  switch (value.type) {
    case 'shutdown':
      return { type: 'shutdown' };
    case 'cancel':
      if (!isText(value.run_id)) throw new Error('cancel requires run_id');
      return { type: 'cancel', run_id: value.run_id };
    case 'tool_result':
      if (!isText(value.run_id) || !isText(value.call_id)) throw new Error('tool_result requires run_id and call_id');
      if (value.error !== undefined && typeof value.error !== 'string') throw new Error('tool_result error must be a string');
      return { type: 'tool_result', run_id: value.run_id, call_id: value.call_id, result: value.result, error: value.error as string | undefined };
    case 'run': {
      if (!isText(value.run_id) || !isText(value.kind, 64)) throw new Error('run requires run_id and kind');
      if (typeof value.system_prompt !== 'string' || value.system_prompt.length > 64_000) throw new Error('run requires a bounded system_prompt');
      if (typeof value.message !== 'string' || value.message.length === 0 || value.message.length > 64_000) throw new Error('run requires a bounded message');
      if (!Array.isArray(value.history) || value.history.length > 64) throw new Error('run history must be a bounded array');
      const history: HistoryTurn[] = value.history.map((turn) => {
        if (!isRecord(turn) || (turn.role !== 'user' && turn.role !== 'assistant') || typeof turn.text !== 'string' || turn.text.length > 64_000) throw new Error('Invalid history turn');
        return { role: turn.role, text: turn.text };
      });
      if (!Array.isArray(value.tools) || value.tools.length > 32) throw new Error('run tools must be a bounded array');
      const seen = new Set<string>();
      const tools: ToolDefinition[] = value.tools.map((definition) => {
        if (!isRecord(definition) || !validName(definition.name) || !isText(definition.description, 2000) || !isRecord(definition.input_schema)) throw new Error('Invalid tool definition');
        if (seen.has(definition.name)) throw new Error(`Duplicate tool ${definition.name}`);
        seen.add(definition.name);
        return { name: definition.name, description: definition.description, input_schema: definition.input_schema };
      });
      const limits = value.limits;
      if (!isRecord(limits) || !isPositiveInt(limits.turns, 64) || !isPositiveInt(limits.output_tokens, 32_000) || !isPositiveInt(limits.max_output_chars, 1_000_000) || !isPositiveInt(limits.timeout_ms, 3_600_000) || !isPositiveInt(limits.tool_timeout_ms, 600_000)) throw new Error('Invalid run limits');
      return {
        type: 'run', run_id: value.run_id, kind: value.kind, system_prompt: value.system_prompt, message: value.message, history, tools,
        limits: { turns: limits.turns, output_tokens: limits.output_tokens, max_output_chars: limits.max_output_chars, timeout_ms: limits.timeout_ms, tool_timeout_ms: limits.tool_timeout_ms },
      };
    }
    default:
      throw new Error(`Unknown message type ${value.type}`);
  }
}

export function encodeOutbound(message: Outbound): string {
  return `${JSON.stringify(message)}\n`;
}
