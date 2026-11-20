import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, stat, symlink, unlink, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { initDevelopment, openDevelopment, claimReceiver, setupFailure } from '../src/config.mjs';

const run = promisify(execFile);
const cli = fileURLToPath(new URL('../bin/watch.mjs', import.meta.url));
const demo = fileURLToPath(new URL('../examples/demo.mjs', import.meta.url));

async function development(t) {
  const root = await mkdtemp(join(tmpdir(), 'watch-setup-test-'));
  t.after(() => rm(root, {recursive: true, force: true}));
  const directory = join(root, 'run');
  initDevelopment(directory);
  return {root, directory, dev: openDevelopment(directory)};
}

test('init creates new isolated namespaces, keys and state files; an existing path is preserved', async t => {
  const {root, directory, dev} = await development(t);
  const other = join(root, 'second');
  initDevelopment(other);
  const second = openDevelopment(other);
  assert.notEqual(dev.namespace, second.namespace);
  assert.equal(dev.key.equals(second.key), false);
  assert.equal(dev.key.length, 32);
  assert.notEqual(dev.runtimeDb, dev.inboxDb);
  assert.equal((await stat(directory)).mode & 0o077, 0);
  assert.equal((await stat(join(directory, 'webhook.key'))).mode & 0o077, 0);
  assert.throws(() => initDevelopment(directory));
  assert.deepEqual(openDevelopment(directory).key, dev.key);
  assert.deepEqual([...dev.sources.keys()], ['sample', 'json', 'ticks']);
  const emitted = await dev.sources.get('ticks')({checkpoint: {reads: 0, cursor: null}, mode: 'emitted'});
  assert.equal(emitted.contract, 'source_emitted');
  assert.equal(emitted.source_event_id, 'tick-1');
  assert.equal(dev.routes.get('worker').url, undefined);
  await assert.rejects(run(process.execPath, [cli, 'init', directory], {env: {}}),
    error => error.code === 1 && error.stderr.trim() === 'already_exists');
});

test('setup rejects linked state files and an unrelated directory', async t => {
  const {root, directory, dev} = await development(t);
  assert.throws(() => openDevelopment(root));
  const other = join(root, 'unrelated.db');
  const marker = Buffer.from('unrelated test fixture');
  await writeFile(other, marker);
  await unlink(dev.runtimeDb);
  await symlink(other, dev.runtimeDb);
  assert.throws(() => openDevelopment(directory));
  assert.deepEqual(await readFile(other), marker);
});

test('setup classifies storage failures without including paths or source strings', () => {
  const message = randomBytes(24).toString('hex');
  assert.equal(setupFailure({message, code: 'ENOSPC'}), 'storage_full');
  assert.equal(setupFailure({message, code: 'ERR_SQLITE_ERROR', errcode: 13}), 'storage_full');
  assert.equal(setupFailure({message, code: 'ERR_SQLITE_ERROR', errcode: 266}), 'storage_io_error');
  assert.equal(setupFailure({message}), 'setup_failed');
});

test('invalid optional web configuration degrades its watch without exposing options', async t => {
  const {root, directory} = await development(t);
  const config = join(directory, 'web-source.json');
  const valid = {baseUrl: 'http://127.0.0.1:39971', watchId: randomUUID()};
  const context = {checkpoint: {reads: 0, cursor: null}, mode: 'until', now: Date.now()};
  const invalid = {version: 1, contract: 'durable_ready', coverage: 'failed', authority: 'advisory', code: 'source_invalid'};
  const values = [null, [valid], {...valid, extra: randomBytes(16).toString('hex')}, {watchId: valid.watchId},
    {...valid, baseUrl: 'http://localhost:39971'}, {...valid, watchId: 'invalid'}, {...valid, apiKey: 8},
    {...valid, timeoutMs: 0}, {...valid, maxAgeMs: 0}];
  for (const body of [...values.map(v => JSON.stringify(v)), '{', ' '.repeat(4097)]) {
    await writeFile(config, body, {mode: 0o600});
    const {sources} = openDevelopment(directory);
    assert.deepEqual(await sources.get('web')(context), invalid);
    assert.equal((await sources.get('json')(context)).coverage, 'complete');
  }
  const linked = join(root, 'generated-options.json');
  await writeFile(linked, JSON.stringify(valid));
  await unlink(config);
  await symlink(linked, config);
  assert.deepEqual(await openDevelopment(directory).sources.get('web')(context), invalid);
});

async function mcpTools(directory, calls) {
  const pending = run(process.execPath, [cli, 'serve', directory], {env: {}, timeout: 5000, maxBuffer: 32768});
  const messages = [
    {id: 1, method: 'initialize', params: {protocolVersion: '2025-11-25', capabilities: {},
      clientInfo: {name: 'web-fixture-client', version: '1'}}},
    {method: 'notifications/initialized'},
    ...calls.map(([name, args], i) => ({id: i + 2, method: 'tools/call', params: {name, arguments: args}})),
  ];
  pending.child.stdin.end(messages.map(message => JSON.stringify({jsonrpc: '2.0', ...message})).join('\n') + '\n');
  const {stdout} = await pending;
  const replies = stdout.trimEnd().split('\n').map(line => JSON.parse(line));
  assert.equal(replies.length, calls.length + 1);
  assert.equal(replies[0].result.protocolVersion, '2025-11-25');
  return replies.slice(1).map(({result}) => {
    assert.equal(result.isError, false);
    return result.structuredContent;
  });
}

test('stdio MCP watches a generated web snapshot for readiness and changes', async t => {
  const {directory} = await development(t);
  const watchId = randomUUID();
  const apiKey = randomBytes(24).toString('hex');
  const first = Math.floor(Date.now() / 1000) - 2;
  const snapshots = new Map([[first, 'fixture ready']]);
  const paths = [];
  const prefix = '/api/v1/watch/' + watchId;
  const fixture = createServer((req, res) => {
    if (req.method !== 'GET' || req.headers['x-api-key'] !== apiKey || !req.url.startsWith(prefix)) {
      res.writeHead(403).end();
      return;
    }
    const path = req.url.slice(prefix.length);
    paths.push(path);
    const latest = [...snapshots.keys()].at(-1);
    if (path === '') {
      const health = {uuid: watchId, processor: 'text_json_diff', paused: false, last_error: false,
        last_checked: latest, last_changed: snapshots.size === 1 ? 0 : latest, history_n: snapshots.size};
      res.writeHead(200, {'content-type': 'application/json'}).end(JSON.stringify(health));
    } else if (path === '/history') {
      const history = Object.fromEntries([...snapshots.keys()].map(stamp => [stamp, 'generated-snapshot']));
      res.writeHead(200, {'content-type': 'application/json'}).end(JSON.stringify(history));
    } else if (path.startsWith('/history/')) {
      const stamp = path === '/history/latest' ? latest : Number(path.slice('/history/'.length));
      res.writeHead(snapshots.has(stamp) ? 200 : 404, {'content-type': 'text/plain'}).end(snapshots.get(stamp));
    } else if (path === '/difference/' + first + '/' + (first + 1) + '?format=text') {
      res.writeHead(200, {'content-type': 'text/plain'}).end('generated difference');
    } else {
      res.writeHead(404).end();
    }
  });
  await new Promise((resolve, reject) => {
    fixture.once('error', reject);
    fixture.listen(0, '127.0.0.1', resolve);
  });
  t.after(() => new Promise(resolve => fixture.close(resolve)));
  await writeFile(join(directory, 'web-source.json'), JSON.stringify({
    baseUrl: 'http://127.0.0.1:' + fixture.address().port, watchId, apiKey, maxAgeMs: 60000, timeoutMs: 2000,
  }), {flag: 'wx', mode: 0o600});

  const base = {source_id: 'web', consumer_id: 'worker', interval_ms: 20};
  const changeArgs = {...base, request_id: 'changes'};
  const [ready, baseline] = await mcpTools(directory, [
    ['watch_until', {...base, request_id: 'until', predicate: {path: '/text', contains: 'ready'}}],
    ['watch_changes', changeArgs],
  ]);
  assert.equal(ready.state, 'ready');
  assert.equal(ready.truth, 'TRUE');
  assert.equal(ready.sequence, 1);
  assert.equal(baseline.state, 'active');
  assert.equal(baseline.sequence, 0);
  assert.ok(paths.includes('/history/latest'));
  assert.ok(paths.includes('/history/' + first));
  snapshots.set(first + 1, 'fixture advanced');
  // A restarted scheduler may already be sampling when a retried create
  // returns. Each server drains its in-flight observation at EOF.
  let updated = baseline;
  for (let i = 0; i < 5 && !updated.sequence; i++) {
    [updated] = await mcpTools(directory, [['watch_changes', changeArgs]]);
  }
  assert.equal(updated.watch_id, baseline.watch_id);
  assert.equal(updated.sequence, 1);
  assert.ok(paths.includes('/difference/' + first + '/' + (first + 1) + '?format=text'));
  assert.ok(!JSON.stringify([ready, updated]).includes(apiKey));
});

test('consumer discovers only a local registration signed for its directory', async t => {
  const {root, directory, dev} = await development(t);
  const route = dev.routes.get('worker');
  // Use a synthetic port as data. This test opens no network connection.
  const registration = claimReceiver(dev);
  assert.equal(route.url, undefined);
  registration.publish(39179);
  assert.equal(route.url, 'http://127.0.0.1:39179/inbox');
  assert.throws(() => claimReceiver(dev), /receiver_already_running/);
  const contact = join(directory, 'receiver.json');
  const other = join(root, 'second');
  initDevelopment(other);
  await writeFile(join(other, 'receiver.json'), await readFile(contact));
  assert.equal(openDevelopment(other).routes.get('worker').url, undefined);
  const tampered = JSON.parse(await readFile(contact, 'utf8'));
  tampered.port++;
  await writeFile(contact, JSON.stringify(tampered));
  assert.equal(route.url, undefined);
  assert.throws(() => registration.refresh(), /receiver_registration_lost/);
  registration.close();
  assert.equal(route.url, undefined);
  const replacement = claimReceiver(dev);
  replacement.publish(39180);
  assert.equal(route.url, 'http://127.0.0.1:39180/inbox');
  replacement.refresh();
  replacement.close();
  assert.equal(route.url, undefined);
});

function receiverProcess(directory, children) {
  const child = spawn(process.execPath, [cli, 'receive', directory], {env: {}, stdio: ['ignore', 'pipe', 'pipe']});
  const exited = new Promise(resolve => child.once('close', code => resolve(code)));
  let diagnostic = '';
  child.stderr.setEncoding('utf8').on('data', chunk => { diagnostic = (diagnostic + chunk).slice(-256); });
  const admission = new Promise(resolve => {
    let first = '';
    child.stdout.setEncoding('utf8').on('data', chunk => {
      first += chunk;
      if (first.includes('\n')) resolve(first.split('\n')[0] === '{"state":"listening"}');
    });
    child.once('error', () => resolve(false));
    child.once('close', () => resolve(false));
  });
  const handle = {child, exited, admission, diagnostic: () => diagnostic};
  children.push(handle);
  return handle;
}

test('simultaneous receiver starts elect one owner and a crashed owner can be replaced', {timeout: 10000}, async t => {
  const {dev, directory} = await development(t);
  const children = [];
  try {
    const first = receiverProcess(directory, children);
    const second = receiverProcess(directory, children);
    const [a, b] = await Promise.all([first.admission, second.admission]);
    assert.equal(Number(a) + Number(b), 1);
    const [winner, loser] = a ? [first, second] : [second, first];
    assert.equal(await loser.exited, 1);
    assert.match(loser.diagnostic(), /receiver_already_running/);
    const file = join(directory, 'receiver.json');
    const old = await readFile(file, 'utf8').then(JSON.parse).catch(error => {
      const code = winner.diagnostic().split('\n').find(label => setupFailure({message: label}) === label);
      const reason = code ?? setupFailure(error);
      throw Error('receiver_publication_unavailable_' + (winner.child.exitCode ?? 'running') + '_' + reason);
    });
    assert.equal(old.pid, winner.child.pid);
    assert.match(dev.routes.get('worker').url, /^http:\/\/127\.0\.0\.1:\d+\/inbox$/);
    winner.child.kill('SIGKILL');
    await winner.exited;
    assert.equal(dev.routes.get('worker').url, undefined);
    const replacement = receiverProcess(directory, children);
    assert.equal(await replacement.admission, true);
    const next = JSON.parse(await readFile(file, 'utf8'));
    assert.equal(next.pid, replacement.child.pid);
    assert.notEqual(old.instance, next.instance);
    assert.equal(dev.routes.get('worker').url, 'http://127.0.0.1:' + next.port + '/inbox');
    replacement.child.kill('SIGTERM');
    assert.equal(await replacement.exited, 0);
    assert.equal(dev.routes.get('worker').url, undefined);
  } finally {
    for (const {child} of children) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await Promise.all(children.map(item => item.exited));
  }
});

test('normal CLI creates a watch, disconnects, resumes headlessly and processes its push', async () => {
  const {stdout} = await run(process.execPath, [demo], {env: {}, timeout: 20000, maxBuffer: 4096});
  const result = JSON.parse(stdout);
  assert.equal(result.accepted, 1);
  assert.equal(result.processed, 1);
  assert.equal(result.sequence, 1);
  assert.match(result.watch_id, /^[a-f0-9-]{36}$/);
  assert.match(result.event_id, /^[a-f0-9-]{36}$/);
  assert.deepEqual(Object.keys(result).sort(), ['accepted', 'event_id', 'processed', 'sequence', 'watch_id']);
});
