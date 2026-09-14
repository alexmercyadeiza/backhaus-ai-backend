import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('configure_model', Path(__file__).resolve().parents[1] / 'scripts/configure-model.py')
model = importlib.util.module_from_spec(spec)
spec.loader.exec_module(model)


class ModelConfigurationTests(unittest.TestCase):
    def test_streamed_tool_fragments_and_unicode(self):
        class Response:
            headers = {'Content-Type': 'text/event-stream'}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def __iter__(self):
                for delta in [
                    {'tool_calls': [{'index': 0, 'id': 'call-1', 'function': {'name': 'connection_check', 'arguments': '{"value":'}}]},
                    {'tool_calls': [{'index': 0, 'function': {'arguments': '"backhaus-ready"}'}}]},
                    {'content': '₦'},
                ]:
                    yield ('data: ' + json.dumps({'choices': [{'delta': delta}]}) + '\n').encode()
                yield b'data: [DONE]\n'
        with patch.object(model, 'build_opener') as opener:
            opener.return_value.open.return_value = Response()
            text, calls = model.completion('https://example.invalid/v1', 'private-test-key', {})
        self.assertEqual(text, '₦')
        self.assertEqual(json.loads(calls[0]['function']['arguments']), {'value': 'backhaus-ready'})

    def test_probe_requires_tool_call_and_final_answer(self):
        call = {'id': '1', 'type': 'function', 'function': {'name': 'connection_check', 'arguments': '{"value":"backhaus-ready"}'}}
        with patch.object(model, 'completion', side_effect=[('', [call]), ('Ready', [])]) as complete:
            model.probe('https://example.invalid/v1', 'model-alias', '')
            second = complete.call_args_list[1].args[2]
            self.assertEqual(second['messages'][-1]['role'], 'tool')
        with patch.object(model, 'completion', return_value=('Made up an answer', [])):
            with self.assertRaises(ValueError): model.probe('https://example.invalid/v1', 'model-alias', '')

    def test_atomic_save_preserves_unrelated_settings_and_private_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / '.env'
            env.write_text('DATABASE_URL=existing\nMODEL_NAME=old\n# Keep this\n')
            with patch.object(model, 'ENV', env):
                model.save('https://example.invalid/v1', 'model-alias', 'private-test-key')
                with patch.dict(model.os.environ, {}, clear=True):
                    result = model.settings()
            self.assertEqual(result['DATABASE_URL'], 'existing')
            self.assertEqual(result['MODEL_NAME'], 'model-alias')
            self.assertEqual(result['MODEL_API_KEY'], 'private-test-key')
            self.assertEqual(env.stat().st_mode & 0o777, 0o600)


if __name__ == '__main__': unittest.main()
