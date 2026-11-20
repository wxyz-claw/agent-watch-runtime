import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const cli = fileURLToPath(new URL('../bin/watch.mjs', import.meta.url));
const day = 86_400_000;

function launch(command, directory, children) {
  const child = spawn(process.execPath, [cli, command, directory], {env: {}, stdio: 'pipe'});
  const lines = createInterface({input: child.stdout, crlfDelay: Infinity})[Symbol.asyncIterator]();
  child.stderr.resume();
  child.stdin.on('error', () => {});
  const exited = new Promise(resolve => {
    child.once('error', () => resolve(-1));
    child.once('close', code => resolve(code));
  });
  const handle = {child, exited, async read() {
    const {done, value} = await lines.next();
    assert.equal(done, false, 'child closed before returning a result');
    return JSON.parse(value);
  }};
  children.push(handle);
  return handle;
}

async function connect(directory, children) {
  const server = launch('serve', directory, children);
  let nextId = 0;
  const send = message => server.child.stdin.write(JSON.stringify({jsonrpc: '2.0', ...message}) + '\n');
  async function request(method, params) {
    const id = ++nextId;
    send({id, method, params});
    const reply = await server.read();
    assert.equal(reply.id, id);
    assert.equal(reply.jsonrpc, '2.0');
    assert.ok(reply.result, 'expected a successful JSON-RPC response');
    return reply.result;
  }
  const init = await request('initialize', {protocolVersion: '2025-11-25', capabilities: {},
    clientInfo: {name: 'controls-fixture', version: '1'}});
  assert.equal(init.protocolVersion, '2025-11-25');
  send({method: 'notifications/initialized'});
  return {
    request,
    async tool(name, args, expectedError) {
      const result = await request('tools/call', {name, arguments: args});
      assert.equal(result.isError, expectedError !== undefined, name);
      if (expectedError !== undefined) assert.deepEqual(result.structuredContent, {error: expectedError}, name);
      return result.structuredContent;
    },
    async close() { server.child.stdin.end(); assert.equal(await server.exited, 0); },
  };
}

async function fixture(directory, value) {
  const next = join(directory, 'controls.next.json');
  await writeFile(next, JSON.stringify(value), {flag: 'wx', mode: 0o600});
  await rename(next, join(directory, 'sample.json'));
}

function storedWatchData(db) {
  const hash = createHash('sha256');
  for (const table of ['watches', 'watch_configs', 'manual_checks', 'events', 'outbox']) {
    const rows = db.prepare('SELECT * FROM ' + table + ' ORDER BY rowid').all();
    hash.update(JSON.stringify(rows));
  }
  return hash.digest('hex');
}

async function untilStatus(client, watch_id, predicate) {
  const end = Date.now() + 4000;
  do {
    const status = await client.tool('get_watch', {watch_id});
    if (predicate(status)) return status;
    await delay(20);
  } while (Date.now() < end);
  assert.fail('the expected durable watch status was not observed');
}

test('stdio controls preview, edit, check, persist and deliver a held terminal event', {timeout: 15000}, async () => {
  const temporary = await mkdtemp(join(tmpdir(), 'watch-controls-'));
  const directory = join(temporary, 'state');
  const children = [];
  let producerDb, inboxDb;
  const guard = setTimeout(() => {
    for (const {child} of children) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  }, 12000);
  try {
    await promisify(execFile)(process.execPath, [cli, 'init', directory], {env: {}});
    const receiver = launch('receive', directory, children);
    assert.equal((await receiver.read()).state, 'listening');
    let client = await connect(directory, children);
    producerDb = new DatabaseSync(join(directory, 'runtime.sqlite'), {readOnly: true});
    inboxDb = new DatabaseSync(join(directory, 'inbox.sqlite'), {readOnly: true});
    const tools = new Set((await client.request('tools/list', {})).tools.map(item => item.name));
    for (const name of ['create_watch', 'preview_watch', 'list_watches', 'pause_watch', 'resume_watch',
      'update_watch', 'check_now']) assert.ok(tools.has(name), name);

    const target = 'generated-fixture-match';
    const config = {version: 1, mode: 'until', source_id: 'json', consumer_id: 'worker',
      interval_ms: day, deadline_ms: null, delivery: {enabled: false}, predicate: {path: '/phase', equals: target}};
    assert.deepEqual((await client.tool('list_watches', {})).watches, []);
    const empty = storedWatchData(producerDb);
    const draft = await client.tool('preview_watch', {config});
    assert.equal(draft.basis, 'sample');
    assert.equal(draft.truth, 'FALSE');
    assert.ok(!JSON.stringify(draft).includes(target));
    assert.equal(storedWatchData(producerDb), empty);
    const created = await client.tool('create_watch', {config, request_id: 'controls'});
    const {watch_id} = created;
    assert.equal(created.revision, 1);
    assert.equal(created.truth, 'FALSE');
    assert.equal(created.delivery_enabled, false);
    assert.equal((await client.tool('create_watch', {config, request_id: 'controls'})).watch_id, watch_id);

    await fixture(directory, {phase: target});
    const stored = storedWatchData(producerDb);
    const cached = await client.tool('preview_watch', {config, watch_id});
    assert.equal(cached.basis, 'checkpoint');
    assert.equal(cached.truth, 'FALSE');
    assert.equal((await client.tool('preview_watch', {config})).truth, 'TRUE');
    const missing = {...config, predicate: {path: '/approved', equals: true}};
    assert.equal((await client.tool('preview_watch', {config: missing})).truth, 'UNKNOWN');
    assert.equal(storedWatchData(producerDb), stored);

    const paused = await client.tool('pause_watch', {watch_id, expected_revision: created.revision});
    assert.equal(paused.state, 'paused');
    assert.ok(paused.revision > created.revision);
    await client.tool('check_now', {watch_id, expected_revision: paused.revision, operation_seq: 1}, 'watch_paused');
    const patch = {version: 1, predicate: missing.predicate, deadline_ms: null};
    const edited = await client.tool('update_watch', {watch_id, expected_revision: paused.revision, patch});
    assert.equal(edited.state, 'paused');
    assert.ok(edited.revision > paused.revision);
    await client.tool('update_watch', {watch_id, expected_revision: paused.revision, patch}, 'revision_conflict');
    assert.deepEqual((await client.tool('list_watches', {state: 'paused', limit: 1})).watches.map(w => w.watch_id), [watch_id]);

    await client.close();
    client = await connect(directory, children);
    assert.equal((await client.tool('get_watch', {watch_id})).revision, edited.revision);
    await fixture(directory, {approved: false});
    const resumed = await client.tool('resume_watch', {watch_id, expected_revision: edited.revision});
    assert.equal(resumed.state, 'active');
    const steady = await untilStatus(client, watch_id, s => s.reads > edited.reads && s.truth === 'FALSE' &&
      s.next_read_ms > Date.now() + day / 2);
    const first = {watch_id, expected_revision: steady.revision, operation_seq: 1};
    await client.tool('check_now', first);
    const checked = await untilStatus(client, watch_id, s => s.manual_check?.state === 'completed');
    assert.equal(checked.reads, steady.reads + 1);
    assert.equal((await client.tool('check_now', first)).state, 'completed');
    assert.equal((await client.tool('get_watch', {watch_id})).reads, checked.reads);

    await fixture(directory, {});
    const second = {...first, operation_seq: 2};
    await client.tool('check_now', second);
    const unknown = await untilStatus(client, watch_id, s => s.manual_check?.operation_seq === 2 &&
      s.manual_check.state === 'completed');
    assert.equal(unknown.truth, 'UNKNOWN');
    await client.tool('check_now', first, 'operation_conflict');

    await fixture(directory, {approved: true});
    const last = {...first, operation_seq: 3};
    await client.tool('check_now', last);
    const ready = await untilStatus(client, watch_id, s => s.state === 'ready' && s.manual_check?.state === 'completed');
    assert.equal(ready.sequence, 1);
    const history = await client.tool('read_events', {watch_id});
    assert.equal(history.events.length, 1);
    const [event] = history.events;
    assert.equal(event.kind, 'ready');
    assert.equal(inboxDb.prepare('SELECT count(*) AS n FROM inbox').get().n, 0);

    await client.close();
    client = await connect(directory, children);
    assert.equal((await client.tool('check_now', last)).state, 'completed');
    await client.tool('check_now', {...last, expected_revision: ready.revision, operation_seq: 4}, 'watch_terminal');
    const release = await client.tool('update_watch', {watch_id, expected_revision: ready.revision,
      patch: {version: 1, delivery: {enabled: true}}});
    assert.equal(release.state, 'ready');
    assert.equal(release.reads, ready.reads);
    assert.equal(release.sequence, ready.sequence);
    const processed = await receiver.read();
    assert.equal(processed.state, 'done');
    assert.equal(processed.event_id, event.event_id);
    assert.deepEqual((await client.tool('read_events', {watch_id})).events, [event]);
    assert.equal((await client.tool('check_now', last)).state, 'completed');

    const other = await client.tool('create_watch', {request_id: 'second', config: {version: 1, mode: 'changes',
      source_id: 'json', consumer_id: 'worker', interval_ms: day, delivery: {enabled: false}}});
    const page = await client.tool('list_watches', {limit: 1});
    assert.equal(page.has_more, true);
    assert.equal(typeof page.next_cursor, 'string');
    const tail = await client.tool('list_watches', {limit: 1, after: page.next_cursor});
    assert.equal(tail.has_more, false);
    assert.equal(tail.next_cursor, null);
    assert.deepEqual(new Set([...page.watches, ...tail.watches].map(w => w.watch_id)), new Set([watch_id, other.watch_id]));
    await client.close();
    receiver.child.kill('SIGTERM');
    assert.equal(await receiver.exited, 0);
  } finally {
    clearTimeout(guard);
    for (const {child} of children) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await Promise.all(children.map(handle => handle.exited));
    producerDb?.close();
    inboxDb?.close();
    await rm(temporary, {recursive: true, force: true});
  }
});
