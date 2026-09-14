"""Opt-in local process test; never contacts a model or the production database."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from urllib.request import Request, urlopen
import uuid

ROOT = Path(__file__).resolve().parents[1]

@unittest.skipUnless(os.environ.get('BACKHAUS_RUNTIME_TEST') == '1', 'Opt-in process lifecycle check')
class RuntimeLifecycle(unittest.TestCase):
    def test_single_command_pause_persistence_duplicate_start_and_shutdown(self):
        database = os.environ['TEST_DATABASE_URL']
        self.assertTrue(database.split('?')[0].endswith('/backhaus_ai_test'))
        workspace = f'runtime-test-{uuid.uuid4()}'
        subprocess.run(['psql', database, '-v', 'ON_ERROR_STOP=1', '-c',
                        f"INSERT INTO workspaces(id,name) VALUES('{workspace}','Runtime test')"], check=True, capture_output=True)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        key = str(uuid.uuid4())
        env = {**os.environ, 'DATABASE_URL': database, 'WORKSPACE_ID': workspace,
               'BACKEND_API_KEY': key, 'BIND_ADDRESS': f'127.0.0.1:{port}',
               'MODEL_BASE_URL': '', 'MODEL_NAME': '', 'MODEL_API_KEY': '',
               'RUST_LOG': 'backhaus_ai_backend=info'}
        base = f'http://127.0.0.1:{port}'
        process = None
        def call(path, method='GET'):
            req = Request(base + path, method=method, headers={'Authorization': f'Bearer {key}'})
            with urlopen(req, timeout=2) as response:
                return json.load(response)
        def wait_until(predicate):
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                try:
                    result = predicate()
                    if result:
                        return result
                except OSError:
                    pass
                time.sleep(.1)
            self.fail('Timed out waiting for backend state')
        def stop():
            nonlocal process
            os.killpg(process.pid, signal.SIGINT)
            self.assertEqual(process.wait(timeout=8), 0)
            process = None
            with self.assertRaises(OSError):
                urlopen(base + '/healthz', timeout=1)
        with tempfile.TemporaryFile() as log:
            def start():
                nonlocal process
                process = subprocess.Popen(['cargo', 'run', '--locked'], cwd=ROOT, env=env,
                                           stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)
                wait_until(lambda: call('/healthz'))
                wait_until(lambda: call('/v1/agents')['monitor_online'])
            try:
                start()
                wait_until(lambda: all(a['status'] == 'watching' for a in call('/v1/agents')['agents']))
                paused = call('/v1/agents/inventory/control/pause', 'POST')['agents'][1]
                self.assertEqual(paused['status'], 'paused')
                # Duplicate default start must fail, with no additional watcher left behind.
                duplicate = subprocess.run([str(ROOT/'target/debug/backhaus-ai-backend')], cwd=ROOT, env=env,
                                           capture_output=True, timeout=10)
                self.assertNotEqual(duplicate.returncode, 0)
                stop()
                start()
                restored = call('/v1/agents')['agents'][1]
                self.assertEqual(restored['status'], 'paused')
                self.assertEqual(restored['last_checked_at'], paused['last_checked_at'])
                self.assertEqual(restored['checked_revision'], paused['checked_revision'])
                self.assertTrue(call('/v1/agents/inventory/control/resume', 'POST')['agents'][1]['enabled'])
                stop()
                log.seek(0)
                output = log.read().decode()
                self.assertIn('Sales and Inventory monitors ready', output)
                self.assertIn('All backend services stopped', output)
                self.assertNotIn(key, output)
            finally:
                if process is not None and process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=3)

if __name__ == '__main__':
    unittest.main()
