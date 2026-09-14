import { Agent, tool, type Message, type Model, type MessageData } from '@strands-agents/sdk';
import type { JSONSchema, JSONValue } from '@strands-agents/sdk';
import type { Outbound, RunRequest } from './protocol.ts';

export type ToolOutcome = { result?: unknown; error?: string };
export interface RunIO {
  emit(message: Outbound): void;
  /** Ask Rust to execute a validated tool. Resolves with the tool result or a model-visible error. */
  requestTool(callId: string, name: string, input: unknown, signal: AbortSignal): Promise<ToolOutcome>;
}

class OutputLimitError extends Error {}

function textOf(message: Message): string {
  return message.content.map((block) => (block.type === 'textBlock' ? block.text : '')).join('');
}

/** Run one isolated Strands agent turn. Every run builds a fresh Agent; nothing is shared between runs. */
export async function executeRun(request: RunRequest, model: Model, io: RunIO, cancel: AbortSignal): Promise<void> {
  const timeout = AbortSignal.timeout(request.limits.timeout_ms);
  const signal = AbortSignal.any([cancel, timeout]);
  let emitted = 0;
  let modelCalls = 0;
  let callSequence = 0;
  const tools = request.tools.map((definition) => tool({
    name: definition.name,
    description: definition.description,
    inputSchema: definition.input_schema as JSONSchema,
    callback: async (input: unknown, context) => {
      // The worker never executes business logic; Rust validates and runs the tool.
      const callId = `${context.toolUse.toolUseId || 'call'}-${++callSequence}`;
      const toolSignal = AbortSignal.any([signal, AbortSignal.timeout(request.limits.tool_timeout_ms)]);
      const outcome = await io.requestTool(callId, definition.name, input, toolSignal);
      if (outcome.error !== undefined) throw new Error(outcome.error);
      return (outcome.result ?? null) as JSONValue;
    },
  }));
  const messages: MessageData[] = request.history.map((turn) => ({ role: turn.role, content: [{ type: 'textBlock', text: turn.text }] }));
  const agent = new Agent({
    name: request.kind,
    model,
    systemPrompt: request.system_prompt,
    messages,
    tools,
    printer: false,
    toolExecutor: 'sequential',
    retryStrategy: null,
    sandbox: false,
  });
  try {
    const stream = agent.stream(request.message, { cancelSignal: signal, limits: { turns: request.limits.turns, outputTokens: request.limits.output_tokens } });
    let step = await stream.next();
    while (!step.done) {
      const event = step.value;
      if (event.type === 'beforeModelCallEvent') modelCalls += 1;
      if (event.type === 'modelStreamUpdateEvent' && event.event.type === 'modelContentBlockDeltaEvent' && event.event.delta.type === 'textDelta') {
        const text = event.event.delta.text;
        if (text.length > 0) {
          emitted += text.length;
          if (emitted > request.limits.max_output_chars) throw new OutputLimitError('Model response exceeded output limit');
          io.emit({ type: 'text_delta', run_id: request.run_id, text });
        }
      }
      // Reasoning deltas and raw tool arguments are intentionally not forwarded.
      step = await stream.next();
    }
    const result = step.value;
    if (signal.aborted || result.stopReason === 'cancelled') {
      io.emit(cancel.aborted ? { type: 'cancelled', run_id: request.run_id } : { type: 'failed', run_id: request.run_id, code: 'model_timeout', error: 'Model run timed out' });
      return;
    }
    const answer = textOf(result.lastMessage).trim();
    if (!answer) {
      io.emit({ type: 'failed', run_id: request.run_id, code: 'no_final_answer', error: `Model returned no final answer (${result.stopReason})` });
      return;
    }
    io.emit({ type: 'completed', run_id: request.run_id, answer, stop_reason: result.stopReason, model_calls: modelCalls });
  } catch (error) {
    if (cancel.aborted) { io.emit({ type: 'cancelled', run_id: request.run_id }); return; }
    if (timeout.aborted) { io.emit({ type: 'failed', run_id: request.run_id, code: 'model_timeout', error: 'Model run timed out' }); return; }
    const code = error instanceof OutputLimitError ? 'output_limit' : 'model_failed';
    const message = error instanceof Error ? error.message : String(error);
    // Bounded, credential-free description; the key never appears in SDK messages but the URL might.
    io.emit({ type: 'failed', run_id: request.run_id, code, error: message.replace(/https?:\/\/\S+/g, '[url]').slice(0, 500) });
  } finally {
    agent.cancel();
  }
}
