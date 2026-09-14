import type { Model } from '@strands-agents/sdk';
import { OpenAIModel } from '@strands-agents/sdk/models/openai';

export type ModelSettings = {
  baseUrl: string;
  modelId: string;
  apiKey: string;
  requestOptions: Record<string, unknown>;
  timeoutMs: number;
};

// Read the OpenRouter/OpenAI-compatible settings from the environment that the
// Rust runtime passes to this process. The key is never logged or echoed.
export function modelSettingsFromEnv(env: NodeJS.ProcessEnv = process.env): ModelSettings {
  const baseUrl = env.MODEL_BASE_URL?.trim();
  const modelId = env.MODEL_NAME?.trim();
  if (!baseUrl || !modelId) throw new Error('MODEL_BASE_URL and MODEL_NAME are required');
  if (!/^https?:\/\//.test(baseUrl)) throw new Error('MODEL_BASE_URL must be an HTTP(S) URL');
  let requestOptions: Record<string, unknown> = {};
  const raw = env.MODEL_REQUEST_OPTIONS?.trim();
  if (raw) {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) throw new Error('MODEL_REQUEST_OPTIONS must be a JSON object');
    for (const key of Object.keys(parsed)) {
      if (key !== 'reasoning' && key !== 'provider') throw new Error('MODEL_REQUEST_OPTIONS only accepts reasoning and provider options');
    }
    requestOptions = parsed as Record<string, unknown>;
  }
  const timeoutSeconds = Number(env.MODEL_TIMEOUT_SECONDS ?? '120');
  if (!Number.isFinite(timeoutSeconds) || timeoutSeconds < 10 || timeoutSeconds > 600) throw new Error('MODEL_TIMEOUT_SECONDS must be 10..600');
  return { baseUrl, modelId, apiKey: env.MODEL_API_KEY?.trim() || 'local-no-key', requestOptions, timeoutMs: timeoutSeconds * 1000 };
}

export function createModel(settings: ModelSettings, maxTokens: number): Model {
  // The output cap travels as `max_tokens` inside params: the SDK's `maxTokens`
  // option becomes `max_completion_tokens`, which OpenRouter's strict provider
  // routing (require_parameters) rejects for the configured model.
  return new OpenAIModel({
    api: 'chat',
    modelId: settings.modelId,
    apiKey: settings.apiKey,
    clientConfig: { baseURL: settings.baseUrl, timeout: settings.timeoutMs, maxRetries: 0 },
    temperature: 0.1,
    params: { ...settings.requestOptions, max_tokens: maxTokens },
  });
}
