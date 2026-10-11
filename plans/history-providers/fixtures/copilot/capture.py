"""Exercise the pinned official importer and historical reader without a model turn.
Usage: python3 capture.py --cli /path/to/pinned/copilot --out /tmp/copilot-capture
The binary must match provenance.json. Never reads an existing Copilot home.
"""
import argparse
import hashlib
import json
import pathlib
import selectors
import subprocess
import tempfile
import time

ROOT = pathlib.Path(__file__).parent
parser = argparse.ArgumentParser()
parser.add_argument('--cli', required=True, type=pathlib.Path)
parser.add_argument('--out', required=True, type=pathlib.Path)
args = parser.parse_args()
metadata = json.loads((ROOT / 'provenance.json').read_text())
cli = args.cli.resolve()
assert hashlib.sha256(cli.read_bytes()).hexdigest() == metadata['cliSha256'], 'Wrong pinned CLI binary'
assert not args.out.exists(), 'Choose a fresh output directory'

with tempfile.TemporaryDirectory(prefix='copilot-research-432-') as temporary:
    home = pathlib.Path(temporary)
    project = home / 'project'
    project.mkdir()
    env = {'PATH': '/usr/bin:/bin', 'COPILOT_HOME': str(home / 'config'), 'COPILOT_CACHE_HOME': str(home / 'cache'), 'GH_CONFIG_DIR': str(home / 'gh'), 'XDG_CONFIG_HOME': str(home / 'xdg')}
    def run(*arguments):
        result = subprocess.run([str(cli), *arguments], cwd=project, env=env, capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr + result.stdout
        return result.stdout
    assert '1.0.95' in run('--version')
    source = str((ROOT / 'semantic-input.jsonl').resolve())
    validation = json.loads(run('sessions', 'import', '--dry-run', '--output', 'json', source))
    assert validation['ok'] and validation['dryRun']
    imported = json.loads(run('sessions', 'import', '--output', 'json', source, '--working-directory', str(project)))
    sid = imported['sessionId']
    native = home / 'config' / 'session-state' / sid / 'events.jsonl'
    initial = native.read_bytes()
    rows = [json.loads(line) for line in initial.splitlines()]
    stderr = home / 'rpc-stderr.txt'
    with stderr.open('wb') as error_file:
        process = subprocess.Popen([str(cli), '--headless', '--stdio', '--no-auto-update', '--no-auto-login'], cwd=project, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=error_file)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        buffer = b''
        def call(number, method, parameters):
            global buffer
            assert method in ['ping', 'session.resume', 'session.getMessages', 'session.detach']
            body = json.dumps({'jsonrpc': '2.0', 'id': number, 'method': method, 'params': parameters}).encode()
            process.stdin.write(b'Content-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
            process.stdin.flush()
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                while b'\r\n\r\n' in buffer:
                    header, pending = buffer.split(b'\r\n\r\n', 1)
                    size = int(header.split(b':', 1)[1].strip())
                    if len(pending) < size:
                        break
                    message = json.loads(pending[:size])
                    buffer = pending[size:]
                    if message.get('id') == number:
                        assert 'error' not in message, message
                        return message['result']
                if selector.select(0.2):
                    chunk = process.stdout.read1(65536)
                    assert chunk, stderr.read_text()
                    buffer += chunk
            raise TimeoutError(method)
        try:
            ping = call(1, 'ping', {'message': 'synthetic-history-probe'})
            resumed = call(2, 'session.resume', {'sessionId': sid})
            reader = call(3, 'session.getMessages', {'sessionId': sid})['events']
            call(4, 'session.detach', {'sessionId': sid})
        finally:
            selector.close()
            process.terminate()
            process.wait(timeout=5)
    assert rows == reader[:len(rows)], 'Official reader differs from persisted journal'
    assert len(reader) == len(rows) + 1 and reader[-1]['type'] == 'session.resume'
    assert native.read_bytes() == initial, 'Resume unexpectedly changed persisted bytes'
    assert not (home / 'config' / 'session-store.db').exists()
    def sanitize(value):
        if isinstance(value, str):
            return value.replace(str(project), '/fixture/copilot')
        if isinstance(value, list):
            return [sanitize(item) for item in value]
        if isinstance(value, dict):
            return {key: sanitize(item) for key, item in value.items()}
        return value
    args.out.mkdir(parents=True)
    (args.out / 'events.jsonl').write_text(''.join(json.dumps(sanitize(row), separators=(',', ':')) + '\n' for row in rows))
    (args.out / 'official-reader.json').write_text(json.dumps(sanitize(reader), indent=2) + '\n')
    (args.out / 'workspace.yaml').write_text((native.parent / 'workspace.yaml').read_text().replace(str(project), '/fixture/copilot'))
    (args.out / 'capture-result.json').write_text(json.dumps({'importer': imported, 'dryRun': validation, 'ping': ping, 'nativeBytes': len(initial), 'persistedEvents': len(rows), 'readerEvents': len(reader), 'journalUnchangedAfterResume': True, 'derivedIndexPresent': False}, indent=2) + '\n')
    print(f'Official writer + reader verified: {len(rows)} persisted, {len(reader)} returned; no model turn')
