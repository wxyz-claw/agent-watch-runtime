import assert from 'node:assert/strict';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, mkdir, rm, symlink, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { changedetectionSource, jsonSource, syntheticSource } from '../src/sources.mjs';
import { evaluatePredicate, isAuthoritativeObservation } from '../src/sdk.mjs';

const NOW = Date.UTC(2030, 5, 4, 12);
const SECOND = NOW / 1_000;
const context = (mode, checkpoint = {}) => ({now: NOW, mode, checkpoint});
const hash = (data) => createHash('sha256').update(data).digest('hex');

async function fixtures(t) {
  const dir = await mkdtemp(join(tmpdir(), 'watch-source-test-'));
  t.after(() => rm(dir, {recursive: true, force: true}));
  return dir;
}

test('synthetic progression resumes by committed attempts across a coverage failure', async () => {
  const frames = [{value: {ready: false}}, {coverage: 'failed', code: 'source_timeout'}, {value: {ready: true}}];
  const source = syntheticSource(frames);
  frames[0].value.ready = true;
  const start = await source(context('until', {reads: 0}));
  assert.deepEqual(start.value, {ready: false});
  start.value.ready = true;
  assert.deepEqual((await source(context('until', {reads: 0}))).value, {ready: false});
  const outage = await source(context('until', {reads: 1, cursor: '0'}));
  assert.equal(outage.coverage, 'failed');
  assert.equal(isAuthoritativeObservation(outage), false);
  const restarted = syntheticSource(frames);
  const recovered = await restarted(context('until', {reads: 2, cursor: '0'}));
  assert.equal(recovered.value.ready, true);
  assert.equal(recovered.cursor, '2');
  assert.deepEqual(await restarted(context('until', {reads: 3})), recovered);
});

test('only explicit emitted fixtures supply upstream identity', async () => {
  const frames = [{source_event_id: 'sample-emission', value: {step: 1}}];
  const source = syntheticSource(frames);
  const first = await source(context('emitted'));
  assert.equal(first.contract, 'source_emitted');
  assert.equal(first.source_event_id, 'sample-emission');
  assert.deepEqual(await syntheticSource(frames)(context('emitted', {reads: 1})), first);
  assert.equal((await source(context('until'))).code, 'unsupported_contract');
  assert.equal((await syntheticSource([{value: false}])(context('emitted'))).code, 'unsupported_contract');
  assert.throws(() => syntheticSource([{value: 1, code: 'unrecognized'}]));
});

test('JSON file observes current data and keeps metadata-shaped JSON fields as input values', async (t) => {
  const path = join(await fixtures(t), 'state.json');
  const body = '{"coverage":"failed","authority":"advisory","ready":true}\n';
  await writeFile(path, body);
  const source = jsonSource(path);
  const result = await source(context('until'));
  assert.equal(result.coverage, 'complete');
  assert.equal(result.authority, 'authoritative');
  assert.equal(result.contract, 'durable_ready');
  assert.deepEqual(result.value, JSON.parse(body));
  assert.equal(result.evidence.snapshot_bytes, Buffer.byteLength(body));
  assert.equal(result.evidence.snapshot_digest, hash(body));
  assert.equal(result.cursor, hash(body));
  await writeFile(path, 'null');
  assert.equal((await source(context('changes'))).value, null);
  assert.equal((await source(context('emitted'))).code, 'unsupported_contract');
});

test('JSON file reports bounded diagnostics for missing, malformed, deep and oversized data', async (t) => {
  const path = join(await fixtures(t), 'state.json');
  const source = jsonSource(path);
  const missing = await source(context('until'));
  assert.equal(missing.code, 'source_unavailable');
  assert.equal(JSON.stringify(missing).includes(path), false);
  for (const [contents, expectedCode] of [
    [Buffer.from([0xff]), 'source_invalid'],
    ['{"invalid":', 'source_invalid'],
    ['['.repeat(40) + '1' + ']'.repeat(40), 'source_invalid'],
    [Buffer.alloc(65_537, 32), 'capacity'],
  ]) {
    await writeFile(path, contents);
    const failed = await source(context('changes'));
    assert.equal(failed.code, expectedCode);
    assert.equal(isAuthoritativeObservation(failed), false);
    assert.equal(Object.hasOwn(failed, 'value'), false);
  }
  const aborted = AbortSignal.abort();
  assert.equal((await source({...context('changes'), signal: aborted})).code, 'canceled');
});

test('JSON file refuses file links, directory links and non-regular input', async (t) => {
  const dir = await fixtures(t);
  await mkdir(join(dir, 'physical'));
  await writeFile(join(dir, 'physical', 'value.json'), 'true');
  await symlink(join(dir, 'physical', 'value.json'), join(dir, 'value.json'));
  await symlink(join(dir, 'physical'), join(dir, 'linked-dir'));
  for (const path of [join(dir, 'physical'), join(dir, 'value.json'), join(dir, 'linked-dir', 'value.json')]) {
    const result = await jsonSource(path)(context('until'));
    assert.equal(result.code, 'source_invalid');
    assert.equal(Object.hasOwn(result, 'value'), false);
  }
});

async function webFixture(t) {
  const id = randomUUID();
  const key = randomBytes(24).toString('hex');
  const prefix = '/api/v1/watch/' + id;
  const state = {
    snapshots: new Map([[String(SECOND - 2), 'available']]),
    health: {uuid: id, processor: 'text_json_diff', last_checked: SECOND, last_error: false, paused: false},
    requests: [], diff: 'sample difference', handle: () => false,
  };
  const server = createServer((req, res) => {
    const route = req.url.slice(prefix.length);
    state.requests.push({route, authorized: req.headers['x-api-key'] === key, method: req.method});
    if (req.method !== 'GET' || !req.url.startsWith(prefix) || req.headers['x-api-key'] !== key) {
      res.writeHead(403).end();
      return;
    }
    if (state.handle(route, res)) return;
    const keys = [...state.snapshots.keys()];
    if (route === '') {
      const meta = {history_n: keys.length, last_changed: keys.length > 1 ? Number(keys.at(-1)) : 0, ...state.health};
      res.writeHead(200, {'content-type': 'application/json'}).end(JSON.stringify(meta));
    } else if (route === '/history') {
      const entries = Object.fromEntries(keys.map((stamp) => [stamp, 'fixture-snapshot']));
      res.writeHead(200, {'content-type': 'application/json'}).end(JSON.stringify(entries));
    } else if (route.startsWith('/history/')) {
      const stamp = route.slice('/history/'.length);
      const selected = stamp === 'latest' ? keys.at(-1) : stamp;
      const content = state.snapshots.get(selected);
      res.writeHead(content === undefined ? 404 : 200, {'content-type': 'text/plain; charset=UTF-8'}).end(content);
    } else if (route === '/difference/' + keys[0] + '/' + keys.at(-1) + '?format=text') {
      res.writeHead(200, {'content-type': 'text/plain'}).end(state.diff);
    } else {
      res.writeHead(404).end();
    }
  });
  await new Promise((resolve, reject) => server.once('error', reject).listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => {
    server.close(resolve);
    server.closeAllConnections();
  }));
  const config = {baseUrl: 'http://127.0.0.1:' + server.address().port, watchId: id, apiKey: key};
  return {state, config};
}

test('web until evaluates the fresh first retained snapshot even when last_changed is zero', async (t) => {
  const {state, config} = await webFixture(t);
  const observation = await changedetectionSource(config)(context('until'));
  assert.equal(isAuthoritativeObservation(observation), true);
  assert.equal(evaluatePredicate(observation.value, {path: '/text', contains: 'available'}), 'TRUE');
  assert.equal(observation.cursor, String(SECOND - 2));
  assert.deepEqual(observation.evidence, {
    snapshot_digest: hash('available'), snapshot_bytes: 9, history_count: 1, history_complete: false,
  });
  assert.deepEqual(state.requests.map((r) => r.route), ['', '/history', '/history/latest', '/history', '']);
  assert.ok(state.requests.every((r) => r.authorized && r.method === 'GET'));
});

test('web changes uses explicit retained timestamps and hashes the endpoint diff', async (t) => {
  const {state, config} = await webFixture(t);
  const old = String(SECOND - 2);
  const current = String(SECOND - 1);
  state.snapshots.set(current, 'changed text');
  const observation = await changedetectionSource(config)(context('changes', {cursor: old}));
  assert.equal(observation.contract, 'observe_change');
  assert.deepEqual(observation.value, {text: 'changed text'});
  assert.equal(observation.cursor, current);
  assert.equal(observation.evidence.diff_digest, hash(state.diff));
  assert.equal(observation.evidence.diff_bytes, Buffer.byteLength(state.diff));
  assert.equal(observation.evidence.history_complete, false);
  const routes = state.requests.map((r) => r.route);
  assert.ok(routes.includes('/history/' + current));
  assert.ok(routes.includes('/difference/' + old + '/' + current + '?format=text'));
  const diagnostics = JSON.stringify(observation.evidence);
  for (const value of [config.apiKey, config.watchId, config.baseUrl, state.diff, 'changed text']) {
    assert.equal(diagnostics.includes(value), false);
  }
});

test('retention gaps and unsupported diff preserve an independently verified fresh point', async (t) => {
  const {state, config} = await webFixture(t);
  const src = changedetectionSource(config);
  const pruned = await src(context('changes', {cursor: String(SECOND - 5)}));
  assert.equal(pruned.code, 'history_gap');
  assert.equal(isAuthoritativeObservation(pruned), true);
  assert.equal(pruned.evidence.history_complete, false);
  assert.equal(Object.hasOwn(pruned.evidence, 'diff_digest'), false);
  assert.ok(!state.requests.some((r) => r.route.startsWith('/difference/')));
  state.snapshots.set(String(SECOND - 1), 'updated');
  state.handle = (route, res) => {
    if (!route.startsWith('/difference/')) return false;
    res.writeHead(404).end();
    return true;
  };
  const missingDiff = await src(context('changes', {cursor: String(SECOND - 2)}));
  assert.equal(missingDiff.code, 'history_gap');
  assert.equal(isAuthoritativeObservation(missingDiff), true);
  assert.equal(missingDiff.evidence.history_complete, false);
  const rewind = await src(context('changes', {cursor: String(SECOND)}));
  assert.equal(rewind.coverage, 'partial');
  assert.equal(rewind.code, 'history_gap');
});

test('a stalled supplementary diff uses its own budget and preserves the snapshot', async (t) => {
  const {state, config} = await webFixture(t);
  state.snapshots.set(String(SECOND - 1), 'retained update');
  state.handle = (route) => route.startsWith('/difference/');
  const src = changedetectionSource({...config, timeoutMs: 2_000});
  const updated = await src(context('changes', {cursor: String(SECOND - 2)}));
  assert.equal(updated.code, 'history_gap');
  assert.equal(updated.coverage, 'complete');
  assert.deepEqual(updated.value, {text: 'retained update'});
  assert.equal(Object.hasOwn(updated.evidence, 'diff_digest'), false);
  assert.equal((await src(context('until'))).coverage, 'complete');
});

test('unhealthy, absent, stale and changing snapshots cannot establish readiness', async (t) => {
  const {state, config} = await webFixture(t);
  const source = changedetectionSource({...config, maxAgeMs: 5_000});
  for (const [field, value, expected] of [
    ['last_error', 'fixture diagnostic with upstream details', 'source_error'],
    ['paused', true, 'source_error'],
    ['last_checked', SECOND - 120, 'source_stale'],
    ['ignore_status_codes', true, 'source_error'],
    ['consecutive_filter_failures', 1, 'source_error'],
  ]) {
    const original = {...state.health};
    state.health[field] = value;
    const observed = await source(context('until'));
    assert.equal(observed.code, expected);
    assert.equal(Object.hasOwn(observed, 'value'), false);
    assert.equal(JSON.stringify(observed).includes('upstream details'), false);
    state.health = original;
  }
  state.snapshots.clear();
  assert.equal((await source(context('until'))).code, 'history_gap');
  state.snapshots.set(String(SECOND - 9), 'old but still retained');
  assert.equal((await source(context('until'))).code, 'source_stale');
  state.snapshots = new Map([[String(SECOND - 2), 'old']]);
  state.handle = (route) => {
    if (route === '/history/latest') state.snapshots = new Map([[String(SECOND - 1), 'new']]);
    return false;
  };
  const changedDuringRead = await source(context('until'));
  assert.equal(changedDuringRead.coverage, 'partial');
  assert.equal(changedDuringRead.code, 'coverage_partial');
  assert.equal(Object.hasOwn(changedDuringRead, 'value'), false);
  state.handle = () => false;
  assert.deepEqual((await source(context('until'))).value, {text: 'new'});
});

test('web adapter rejects unregistered origins and holds request, redirect and body limits', async (t) => {
  const {state, config} = await webFixture(t);
  for (const baseUrl of ['http://localhost:1', 'http://127.0.0.1:1/other', 'http://127.0.0.1:0', 'https://127.0.0.1:1']) {
    assert.throws(() => changedetectionSource({...config, baseUrl}), /invalid_web_source/);
  }
  assert.throws(() => changedetectionSource({...config, watchId: 'fixture'}), /invalid_web_source/);
  assert.equal((await changedetectionSource(config)(context('emitted'))).code, 'unsupported_contract');
  assert.equal(state.requests.length, 0);
  state.handle = (_route, res) => { res.writeHead(302, {location: '/alternate'}).end(); return true; };
  assert.equal((await changedetectionSource(config)(context('until'))).code, 'source_unavailable');
  assert.deepEqual(state.requests.map((r) => r.route), ['']);
  state.handle = (_route, res) => {
    res.writeHead(200, {'content-type': 'application/json'}).end(Buffer.alloc(131_073));
    return true;
  };
  assert.equal((await changedetectionSource(config)(context('until'))).code, 'capacity');
  state.handle = () => true;
  assert.equal((await changedetectionSource({...config, timeoutMs: 75})(context('until'))).code, 'source_timeout');
});
