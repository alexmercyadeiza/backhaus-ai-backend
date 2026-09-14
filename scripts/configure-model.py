#!/usr/bin/env python3
"""Verify a hosted chat/tool endpoint and optionally save its configuration.
Only sends a synthetic test prompt. Never sends business data or launches a model.
"""
import argparse
import getpass
import json
import os
from pathlib import Path
import shlex
import sys
import tempfile
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen, build_opener, HTTPRedirectHandler

ENV = Path(__file__).resolve().parents[1] / '.env'


def settings():
    values = {}
    if ENV.exists():
        for line in ENV.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                parts = shlex.split(value, comments=True)
                values[key.strip()] = parts[0] if parts else ''
    return {**values, **os.environ}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None  # Never forward the inference credential to another host.


def completion(base, key, payload):
    headers = {'Content-Type': 'application/json', 'Accept': 'text/event-stream'}
    if key:
        headers['Authorization'] = f'Bearer {key}'
    request = Request(base + '/chat/completions', data=json.dumps({**payload, 'stream': True}).encode(), headers=headers)
    text, calls, size = '', {}, 0
    with build_opener(NoRedirect).open(request, timeout=120) as response:
        if 'text/event-stream' not in response.headers.get('Content-Type', ''):
            raise ValueError('Endpoint did not return a streaming response.')
        for line in response:
            size += len(line)
            if size > 1024 * 1024:
                raise ValueError('Probe response exceeded 1 MiB.')
            if not line.startswith(b'data:'):
                continue
            raw = line[5:].strip()
            if raw == b'[DONE]':
                break
            event = json.loads(raw)
            if 'error' in event:
                raise ValueError('Inference server returned a streaming error.')
            for choice in event.get('choices', []):
                if choice.get('index', 0) != 0:
                    continue
                delta = choice.get('delta', {})
                text += delta.get('content') or ''
                for part in delta.get('tool_calls') or []:
                    call = calls.setdefault(part['index'], {'id': '', 'type': 'function', 'function': {'name': '', 'arguments': ''}})
                    call['id'] += part.get('id') or ''
                    for field in ('name', 'arguments'):
                        call['function'][field] += part.get('function', {}).get(field) or ''
    return text, list(calls.values())


def probe(base, model, key, options=None):
    options = options or {}
    tools = [{'type': 'function', 'function': {'name': 'connection_check', 'description': 'Check the connection. Call this before replying.',
        'parameters': {'type': 'object', 'properties': {'value': {'type': 'string'}}, 'required': ['value'], 'additionalProperties': False}}}]
    messages = [{'role': 'user', 'content': 'Call connection_check with value backhaus-ready. Then report its result in one short sentence.'}]
    text, calls = completion(base, key, {**options, 'model': model, 'messages': messages, 'tools': tools, 'tool_choice': 'auto', 'max_tokens': 256, 'temperature': 0})
    if len(calls) != 1 or not calls[0]['id'] or calls[0]['function']['name'] != 'connection_check':
        raise ValueError('Model did not return the requested structured tool call. Check its tool/chat template.')
    if json.loads(calls[0]['function']['arguments']) != {'value': 'backhaus-ready'}:
        raise ValueError('Model returned incorrect tool arguments.')
    messages += [{'role': 'assistant', 'content': text or None, 'tool_calls': calls},
                 {'role': 'tool', 'tool_call_id': calls[0]['id'], 'content': '{"status":"backhaus-ready"}'}]
    answer, extra = completion(base, key, {**options, 'model': model, 'messages': messages, 'tools': tools, 'max_tokens': 256, 'temperature': 0})
    if not answer.strip() or extra:
        raise ValueError('Model did not finish after receiving the tool result.')


def save(base, model, key):
    replacements = {'MODEL_BASE_URL': base, 'MODEL_NAME': model, 'MODEL_API_KEY': key}
    lines = ENV.read_text().splitlines() if ENV.exists() else []
    result = []
    for line in lines:
        name = line.split('=', 1)[0].strip()
        result.append(f'{name}={json.dumps(replacements.pop(name))}' if name in replacements else line)
    result.extend(f'{name}={json.dumps(value)}' for name, value in replacements.items())
    fd, filename = tempfile.mkstemp(dir=ENV.parent, prefix='.env-model-')
    try:
        with os.fdopen(fd, 'w') as output:
            output.write('\n'.join(result) + '\n')
        os.replace(filename, ENV)
    finally:
        if os.path.exists(filename):
            os.unlink(filename)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', help='Inference API base URL, including /v1')
    parser.add_argument('--model', help='Exact model ID/alias served by your inference host')
    parser.add_argument('--prompt-key', action='store_true', help='Read an API key without displaying it')
    parser.add_argument('--save', action='store_true', help='Save only after both streaming probes pass')
    args = parser.parse_args()
    env = settings()
    base = (args.url or env.get('MODEL_BASE_URL', '')).rstrip('/')
    model = args.model or env.get('MODEL_NAME', '')
    key = getpass.getpass('Model API key: ') if args.prompt_key else env.get('MODEL_API_KEY', '')
    if not base or not model:
        parser.error('Provide --url and --model, or set MODEL_BASE_URL and MODEL_NAME in .env.')
    parsed = urlsplit(base)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        parser.error('Use an HTTP(S) API URL without embedded credentials, query, or fragment.')
    try:
        options = json.loads(env.get('MODEL_REQUEST_OPTIONS', '{}'))
        if not isinstance(options, dict) or set(options) - {'reasoning', 'provider'}:
            raise ValueError('Only reasoning and provider model options are supported.')
        probe(base, model, key, options)
        if args.save:
            save(base, model, key)
    except HTTPError as error:
        print(f'Endpoint check failed (HTTP {error.code}). Check URL, model name and credentials.', file=sys.stderr)
        return 1
    except (URLError, TimeoutError, ValueError, KeyError, OSError):
        print('Endpoint check failed: connection, streaming, or tool calling did not pass. Configuration was not saved.', file=sys.stderr)
        return 1
    print('Streaming, tool arguments and tool-result response passed.' + (' Configuration saved; restart API and worker to apply.' if args.save else ' Configuration unchanged.'))
    return 0


if __name__ == '__main__':
    sys.exit(main())
