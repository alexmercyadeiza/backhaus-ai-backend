// Protocol test double for the Rust supervisor. Behaviour is selected through the
// model name the runtime passes in (MODEL_NAME=fake:<mode>), so production code
// needs no test hooks. Never used outside the Rust integration tests.
import { createInterface } from 'node:readline';
const mode = (process.env.MODEL_NAME || 'fake:tool_then_answer').replace(/^fake:/, '');
const emit = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
if (mode === 'no_ready') process.exit(3);
emit({ type: 'ready', protocol: mode === 'bad_ready' ? 99 : 1, sdk: 'fake' });
let current = null;
const input = createInterface({ input: process.stdin });
input.on('line', (line) => {
  if (!line.trim()) return;
  const message = JSON.parse(line);
  if (message.type === 'shutdown') process.exit(0);
  if (message.type === 'run') {
    current = message.run_id;
    const id = message.run_id;
    switch (mode) {
      case 'research_resume': {
        const checkpoint = JSON.parse(message.message).checkpoint;
        if (checkpoint.attempt < 2 || checkpoint.searches_used !== 5 || checkpoint.searches_remaining !== 0 || checkpoint.saved_suppliers.length !== 3) {
          emit({ type:'failed', run_id:id, code:'bad_checkpoint', error:'Durable research checkpoint missing' });
          break;
        }
        emit({type:'tool_call',run_id:id,call_id:'recovered-source',name:'get_research_source',input:{search_id:checkpoint.searches[0].search_id,source_index:0}});
        break;
      }
      case 'chat_dispatch':
        if (!message.tools.some(t => t.name === 'find_vendors')) {
          emit({type:'failed',run_id:id,code:'missing_tool',error:'Chat dispatcher tool missing'});
          break;
        }
        emit({type:'tool_call',run_id:id,call_id:'dispatch',name:'find_vendors',input:{request:'Coffee beans, medium roast; pricing and minimum order'}});
        break;
      case 'tool_then_answer':
        emit({ type: 'text_delta', run_id: id, text: 'Reading inventory. ' });
        emit({ type: 'tool_call', run_id: id, call_id: 'c1', name: 'get_inventory', input: { limit: 1 } });
        break;
      case 'crash_after_tool_call':
        emit({ type: 'tool_call', run_id: id, call_id: 'c1', name: 'get_inventory', input: { limit: 1 } });
        break;
      case 'hang':
        emit({ type: 'text_delta', run_id: id, text: 'Thinking…' });
        break;
      case 'hang_ignore_cancel':
        break;
      case 'stale':
        emit({ type: 'completed', run_id: '00000000-0000-4000-8000-000000000000', answer: 'stale answer', stop_reason: 'endTurn', model_calls: 1 });
        emit({ type: 'tool_call', run_id: '00000000-0000-4000-8000-000000000000', call_id: 'x', name: 'get_sales_summary', input: { from: '2026-07-01', to: '2026-08-31' } });
        emit({ type: 'tool_call', run_id: id, call_id: 'bad', name: 'drop_database', input: {} });
        break;
      case 'busy':
        emit({ type: 'failed', run_id: id, code: 'worker_busy', error: 'busy' });
        break;
      default:
        emit({ type: 'failed', run_id: id, code: 'model_failed', error: `unknown mode ${mode}` });
    }
  } else if (message.type === 'tool_result') {
    if (message.run_id !== current) return;
    if (mode === 'crash_after_tool_call') process.exit(1);
    if (mode === 'research_resume') {
      if(message.error || !message.result?.source?.content) {
        emit({type:'failed',run_id:current,code:'missing_evidence',error:'Cached evidence was not recovered'});
        return;
      }
      // Deliberately false prose reproduces the original incident. Rust must override it.
      emit({type:'completed',run_id:current,answer:'I saved zero suppliers and ran no searches.',stop_reason:'endTurn',model_calls:2});
      return;
    }
    if (mode === 'chat_dispatch') {
      if (message.error || !message.result?.run_id) emit({type:'failed',run_id:current,code:'dispatch_failed',error:message.error || 'Missing research run'});
      else emit({type:'completed',run_id:current,answer:`Procurement dispatched: ${message.result.run_id}`,stop_reason:'endTurn',model_calls:2});
      return;
    }
    if (mode === 'stale') {
      // The rejected tool call comes back as an error; finish with the real run id.
      emit({ type: 'completed', run_id: current, answer: `Finished after rejected tool: ${message.error || 'no error'}`, stop_reason: 'endTurn', model_calls: 1 });
      return;
    }
    const total = message.result && message.result.total_items;
    emit({ type: 'text_delta', run_id: current, text: `Inventory read: ${total} items.` });
    emit({ type: 'completed', run_id: current, answer: `Inventory read: ${total} items.`, stop_reason: 'endTurn', model_calls: 2 });
  } else if (message.type === 'cancel') {
    if (mode === 'hang_ignore_cancel') return;
    if (message.run_id === current) emit({ type: 'cancelled', run_id: current });
  }
});
input.on('close', () => process.exit(0));
