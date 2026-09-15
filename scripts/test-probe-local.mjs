import { createHash, randomUUID } from 'node:crypto';
import { mkdtemp, mkdir, writeFile, readFile, stat, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, isAbsolute } from 'node:path';
import { parseArgs } from 'node:util';
import { spawn } from 'node:child_process';

// This test deliberately never inherits the real HOME, provider paths or credentials.
const { values } = parseArgs({ options: { binary: { type: 'string' }, 'site-url': { type: 'string', default: 'http://127.0.0.1:3100' } } });
if (!values.binary || !isAbsolute(values.binary)) throw new Error('Pass --binary /absolute/path/to/agent-relay-probe');
const selected = new URL(values['site-url']);
if (selected.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(selected.hostname) || selected.pathname !== '/' || selected.username || selected.password || selected.search || selected.hash) throw new Error('This test requires an HTTP loopback origin');
const site = selected.origin;
const base = site + '/cloud';
const root = await mkdtemp(join(tmpdir(), 'probe-native-fixture-'));
const home = join(root, 'home');
const binary = values.binary;
const env = { HOME: home, USERPROFILE: home, XDG_DATA_HOME: join(home, '.local/share'), PATH: '/usr/bin:/bin', TMPDIR: root };
let step = 'local fixture sign-in';
let target;
let active;
function assert(condition, message) { if (!condition) throw new Error(message); }
function run(command, args, approve) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
    active = child;
    let output = '';
    let approved = false;
    let approval = Promise.resolve();
    const timer = setTimeout(() => { child.kill(); reject(new Error('Command timed out')); }, 120_000);
    const capture = chunk => {
      output += chunk;
      if (!approved && approve) {
        const match = output.match(/http:\/\/[^\s]+/);
        if (match) {
          const url = new URL(match[0]);
          if (url.origin !== site) return;
          const code = url.searchParams.get('user_code') || url.searchParams.get('code');
          if (code) { approved = true; approval = approve(code); approval.catch(() => child.kill()); }
        }
      }
    };
    child.stdout.on('data', capture);
    child.stderr.on('data', capture);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('close', async code => {
      clearTimeout(timer);
      active = undefined;
      try { await approval; assert(code === 0, 'Native command exit ' + code + '; milestones=' + ['Downloading Agent Relay Probe', 'Installed agent-relay-probe', 'Connecting to Agent Relay Cloud', 'Open this URL', 'Preparing local session capture', 'Probe connected', 'Probe is running'].filter(marker => output.includes(marker)).join(', ') + '; diagnostic=' + ['not available on this site', 'checksum verification failed', 'Could not reach Cloud', 'different Cloud account', 'could not connect this workspace', 'could not finish', 'already running', 'needs attention', 'stopped during startup'].filter(marker => output.toLowerCase().includes(marker.toLowerCase())).join(', ')); resolve(output); }
      catch (error) { reject(error); }
    });
  });
}
try {
  const login = await fetch(base + '/api/auth/dev-login?source=teams', { redirect: 'manual' });
  assert(login.status === 307, 'Local test login unavailable');
  const cookie = login.headers.get('set-cookie')?.split(';')[0];
  assert(cookie, 'Missing fixture cookie');
  const who = await (await fetch(base + '/api/v1/auth/whoami', { headers: { cookie } })).json();
  assert(who.user?.id && who.currentWorkspace?.id, 'Missing fixture identity');
  target = ['--site-url', site, '--account', who.user.id, '--workspace', who.currentWorkspace.id];
  const source = join(home, '.claude/projects/-work-fixture');
  await mkdir(source, { recursive: true });
  const sessionId = 'native-probe-' + randomUUID();
  const common = { sessionId, cwd: '/work/fixture', timestamp: new Date().toISOString() };
  await writeFile(join(source, sessionId + '.jsonl'), [
    { ...common, type: 'user', uuid: sessionId + '-u', message: { role: 'user', content: 'Synthetic native probe test: hello world.' } },
    { ...common, type: 'assistant', uuid: sessionId + '-a', parentUuid: sessionId + '-u', message: { role: 'assistant', content: [{ type: 'text', text: 'Synthetic native response: hello world.' }] } },
  ].map(value => JSON.stringify(value)).join('\n') + '\n');
  step = 'native device sign-in → background startup';
  await run(binary, ['cloud', 'install', ...target, '--include-existing', '--acknowledge-uninspected-schedules'], async code => {
    const approved = await fetch(base + '/api/v1/auth/device/approve', {
      method: 'POST', headers: { cookie, 'content-type': 'application/json' },
      body: JSON.stringify({ action: 'approve', user_code: code }),
    });
    assert(approved.ok, 'Device approval failed');
  });
  assert((await stat(binary)).mode & 0o111, 'Binary not executable');
  const bytes = await readFile(binary);
  assert(bytes[0] !== 35, 'Downloaded a script instead of the native binary');
  assert((await run(binary, ['status', ...target])).includes('running'), 'Background probe not running');
  step = 'authenticated dashboard status';
  const statusResponse = await fetch(base + '/api/v1/workspaces/' + who.currentWorkspace.id + '/teams/status', { headers: { cookie } });
  const status = await statusResponse.json();
  assert(statusResponse.ok && status.state === 'receiving', 'Dashboard did not confirm data');
  step = 'exact synthetic session readback';
  const minted = await fetch(base + '/api/v1/workspaces/' + who.currentWorkspace.id + '/relayhistory/session', {
    method: 'POST', headers: { cookie, 'content-type': 'application/json' }, body: JSON.stringify({ mode: 'read', label: 'Native probe synthetic verification' }),
  });
  assert(minted.ok, 'Read session unavailable');
  const reader = await minted.json();
  const history = new URL(reader.baseUrl);
  assert(history.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(history.hostname), 'Readback must stay local');
  const account = 'relayhistory:' + createHash('sha256').update(JSON.stringify([reader.orgId, reader.workspaceId])).digest('hex');
  const readback = await fetch(history.origin + '/v1/delivery/records?session_id=' + encodeURIComponent(sessionId), {
    headers: { Authorization: 'Bearer ' + reader.accessToken, 'X-RelayHistory-Expected-Account': account }, redirect: 'error',
  });
  assert(readback.ok, 'Readback failed');
  const listing = await readback.json();
  assert(listing.records?.some(record => record.session_id === sessionId && ['history', 'session_event'].includes(record.kind) && JSON.stringify(record.payload).includes('Synthetic native')), 'Synthetic content was not stored');
  step = 'private credential storage and clean shutdown';
  const probeRoot = join(home, '.agentworkforce/probe');
  const directories = await readdir(probeRoot);
  assert(directories.length === 1, 'Unexpected probe directory');
  assert(((await stat(join(probeRoot, directories[0]))).mode & 0o777) === 0o700, 'Probe directory is not private');
  await run(binary, ['stop', ...target]);
  assert((await run(binary, ['status', ...target])).includes('stopped'), 'Probe did not stop');
  console.log('PASS: native device auth, background collection, exact session storage, dashboard receiving state, private storage, and clean stop all verified.');
  console.log('No Node/npm on the probe PATH. Only synthetic local history was used.');
} catch (error) {
  console.error('FAIL at ' + step + ': ' + error.message);
  process.exitCode = 1;
} finally {
  if (active) active.kill();
  if (target) { try { await run(binary, ['stop', ...target]); } catch {} }
  await rm(root, { recursive: true, force: true });
}
