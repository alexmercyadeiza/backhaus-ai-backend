import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Model, type BaseModelConfig, type Message, type ModelStreamEvent } from '@strands-agents/sdk';
import { executeRun, type RunIO } from '../src/run.ts';
import { parseInbound, type Outbound, type RunRequest } from '../src/protocol.ts';

// The SDK loop runs for real; only the model is scripted. No network, no credentials.
type Script = (call: number, messages: Message[]) => ModelStreamEvent[] | 'hang';
class ScriptedModel extends Model {
  calls = 0;
  readonly script: Script;
  constructor(script: Script) { super(); this.script = script; }
  updateConfig(_config: BaseModelConfig) {}
  getConfig(): BaseModelConfig { return { modelId: 'scripted' }; }
  async *stream(messages: Message[], options?: { cancelSignal?: AbortSignal }): AsyncIterable<ModelStreamEvent> {
    this.calls += 1;
    const events = this.script(this.calls, messages);
    if (events === 'hang') {
      await new Promise<void>((resolve) => options?.cancelSignal?.addEventListener('abort', () => resolve(), { once: true }));
      throw new Error('aborted');
    }
    for (const event of events) yield event;
  }
}
const text = (value: string): ModelStreamEvent[] => [
  { type: 'modelMessageStartEvent', role: 'assistant' },
  { type: 'modelContentBlockStartEvent' },
  { type: 'modelContentBlockDeltaEvent', delta: { type: 'textDelta', text: value } },
  { type: 'modelContentBlockStopEvent' },
  { type: 'modelMessageStopEvent', stopReason: 'endTurn' },
];
const toolUse = (name: string, input: unknown): ModelStreamEvent[] => [
  { type: 'modelMessageStartEvent', role: 'assistant' },
  { type: 'modelContentBlockStartEvent', start: { type: 'toolUseStart', name, toolUseId: 'use-1' } },
  { type: 'modelContentBlockDeltaEvent', delta: { type: 'toolUseInputDelta', input: JSON.stringify(input) } },
  { type: 'modelContentBlockStopEvent' },
  { type: 'modelMessageStopEvent', stopReason: 'toolUse' },
];
const request = (overrides: Partial<RunRequest> = {}): RunRequest => ({
  type: 'run', run_id: 'run-1', kind: 'chat', system_prompt: 'You are a test assistant.', message: 'What are my sales?',
  history: [{ role: 'user', text: 'Earlier question' }, { role: 'assistant', text: 'Earlier answer' }],
  tools: [{ name: 'get_sales_summary', description: 'Exact sales totals', input_schema: { type: 'object', properties: { from: { type: 'string' }, to: { type: 'string' } }, required: ['from', 'to'] } }],
  limits: { turns: 6, output_tokens: 1200, max_output_chars: 64_000, timeout_ms: 5000, tool_timeout_ms: 2000 },
  ...overrides,
});
function collector(handleTool: (name: string, input: unknown) => Promise<{ result?: unknown; error?: string }>) {
  const events: Outbound[] = [];
  const io: RunIO = { emit: (message) => { events.push(message); }, requestTool: (_id, name, input) => handleTool(name, input) };
  return { events, io };
}

test('streams text, forwards a tool call to the host and completes with the final answer', async () => {
  const model = new ScriptedModel((call, messages) => {
    if (call === 1) {
      assert.equal(messages.length, 3, 'history plus the new message');
      assert.equal(messages[0]?.role, 'user');
      return [...text('Checking sales. '), ...toolUse('get_sales_summary', { from: '2026-07-01', to: '2026-08-31' })].filter((event) => !(event.type === 'modelMessageStopEvent' && event.stopReason === 'endTurn'));
    }
    const result = messages.at(-1)?.content.find((block) => block.type === 'toolResultBlock');
    assert.equal(result?.status, 'success');
    assert.match(JSON.stringify(result), /35\.5/);
    return text('Gross sales are NGN 35.5.');
  });
  const seen: { name: string; input: unknown }[] = [];
  const { events, io } = collector(async (name, input) => { seen.push({ name, input }); return { result: { gross_line_sales: '35.5' } }; });
  await executeRun(request(), model, io, new AbortController().signal);
  assert.deepEqual(seen, [{ name: 'get_sales_summary', input: { from: '2026-07-01', to: '2026-08-31' } }]);
  assert.deepEqual(events.map((event) => event.type), ['text_delta', 'text_delta', 'completed']);
  const completed = events.at(-1);
  assert.equal(completed?.type, 'completed');
  if (completed?.type === 'completed') { assert.equal(completed.answer, 'Gross sales are NGN 35.5.'); assert.equal(completed.model_calls, 2); }
});

test('a host tool error is returned to the model instead of ending the run', async () => {
  const model = new ScriptedModel((call, messages) => {
    if (call === 1) return toolUse('get_sales_summary', { from: 'bad', to: 'range' });
    const result = messages.at(-1)?.content.find((block) => block.type === 'toolResultBlock');
    assert.equal(result?.status, 'error');
    return text('That date range is invalid.');
  });
  const { events, io } = collector(async () => ({ error: 'Date range must be ordered' }));
  await executeRun(request(), model, io, new AbortController().signal);
  assert.equal(events.at(-1)?.type, 'completed');
});

test('cancellation reports cancelled, not a failure', async () => {
  const model = new ScriptedModel(() => 'hang');
  const cancel = new AbortController();
  const { events, io } = collector(async () => ({ result: null }));
  const running = executeRun(request(), model, io, cancel.signal);
  setTimeout(() => cancel.abort(), 20);
  await running;
  assert.deepEqual(events.map((event) => event.type), ['cancelled']);
});

test('timeouts and output limits fail with explicit codes', async () => {
  const hanging = new ScriptedModel(() => 'hang');
  const short = collector(async () => ({ result: null }));
  await executeRun(request({ limits: { turns: 6, output_tokens: 1200, max_output_chars: 64_000, timeout_ms: 30, tool_timeout_ms: 20 } }), hanging, short.io, new AbortController().signal);
  assert.equal(short.events[0]?.type, 'failed');
  if (short.events[0]?.type === 'failed') assert.equal(short.events[0].code, 'model_timeout');
  const verbose = new ScriptedModel(() => text('x'.repeat(50)));
  const capped = collector(async () => ({ result: null }));
  await executeRun(request({ limits: { turns: 6, output_tokens: 1200, max_output_chars: 10, timeout_ms: 5000, tool_timeout_ms: 2000 } }), verbose, capped.io, new AbortController().signal);
  assert.equal(capped.events.at(-1)?.type, 'failed');
  if (capped.events.at(-1)?.type === 'failed') assert.equal((capped.events.at(-1) as { code: string }).code, 'output_limit');
});

test('turn limits end the loop without a fabricated answer', async () => {
  const looping = new ScriptedModel(() => toolUse('get_sales_summary', { from: '2026-07-01', to: '2026-07-02' }));
  const { events, io } = collector(async () => ({ result: { ok: true } }));
  await executeRun(request({ limits: { turns: 2, output_tokens: 1200, max_output_chars: 64_000, timeout_ms: 5000, tool_timeout_ms: 2000 } }), looping, io, new AbortController().signal);
  assert.equal(events.at(-1)?.type, 'failed');
  assert.ok(looping.calls <= 3);
});

test('protocol parsing rejects malformed and unbounded input', () => {
  assert.throws(() => parseInbound('not json'));
  assert.throws(() => parseInbound(JSON.stringify({ type: 'run', run_id: 'x' })));
  assert.throws(() => parseInbound(JSON.stringify({ ...request(), tools: [{ name: 'Bad Name', description: 'x', input_schema: {} }] })));
  assert.throws(() => parseInbound(JSON.stringify({ ...request(), limits: { turns: 999 } })));
  const parsed = parseInbound(JSON.stringify(request()));
  assert.equal(parsed.type, 'run');
  assert.equal(parseInbound('{"type":"cancel","run_id":"run-1"}').type, 'cancel');
  assert.equal(parseInbound('{"type":"tool_result","run_id":"r","call_id":"c","result":{"a":1}}').type, 'tool_result');
});
