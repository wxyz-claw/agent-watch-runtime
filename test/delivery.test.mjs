import assert from 'node:assert/strict';
import { createHmac, randomBytes, randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer, request } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import test from 'node:test';
import { createReceiver, deliverOnce, Inbox } from '../src/delivery.mjs';
import { MAX_EVENT_BYTES, sealEvent } from '../src/sdk.mjs';

const NOW = 1_700_000_000_000;
const CONSUMER = 'sample-worker';
const makeEvent = (fields = {}) => sealEvent({
  version: 1, event_id: randomUUID(), watch_id: randomUUID(), sequence: 1,
  kind: 'changed', observed_at: NOW, evidence: {}, ...fields,
});
const bytes = event => Buffer.from(JSON.stringify(event));

function workspace(t) {
  const dir = mkdtempSync(join(tmpdir(), 'watch-inbox-'));
  const path = join(dir, 'receiver.sqlite');
  const servers = new Set();
  const databases = new Set();
  t.after(async () => {
    try {
      for (const server of servers) {
        if (server.listening) {
          const closed = once(server, 'close');
          server.close();
          server.closeAllConnections();
          await closed;
        }
      }
      for (const db of databases) db.close();
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
  return {
    path,
    open(options) {
      const inbox = new Inbox(path, options);
      databases.add(inbox);
      return inbox;
    },
    close(inbox) {
      databases.delete(inbox);
      inbox.close();
    },
    async listen(server) {
      servers.add(server);
      const ready = once(server, 'listening');
      server.listen(0, '127.0.0.1');
      await ready;
      return server.address().port;
    },
    async stop(server) {
      const closed = once(server, 'close');
      server.close();
      await closed;
      servers.delete(server);
    },
  };
}

function outbox(event) {
  const outcomes = [];
  return {
    outcomes,
    claimDelivery() {
      if (outcomes.at(-1)?.accepted) return null;
      return {
        event_id: event.event_id, watch_id: event.watch_id, sequence: event.sequence,
        consumer_id: CONSUMER, body: bytes(event), attempt: outcomes.length + 1, token: randomUUID(),
      };
    },
    finishDelivery(_claim, outcome) { outcomes.push(outcome); },
  };
}

function signedPost(port, body, key, overrides = {}) {
  const { consumer = CONSUMER, keyId = 'test-key', timestamp = NOW, headers = {} } = overrides;
  const signature = createHmac('sha256', key)
    .update(`agent-watch-webhook-v1\n${consumer}\n${keyId}\n${timestamp}\n`)
    .update(body).digest('hex');
  return new Promise((resolve, reject) => {
    const req = request({
      hostname: '127.0.0.1', port, method: 'POST', path: '/inbox', agent: false,
      headers: {
        'content-type': 'application/json', 'x-watch-consumer': consumer,
        'x-watch-timestamp': timestamp, 'x-watch-key-id': keyId, 'x-watch-signature': signature,
        'content-length': body.length, ...headers,
      },
    }, res => {
      let response = '';
      res.setEncoding('utf8');
      res.on('data', part => { response += part; });
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, response }));
    });
    req.on('error', reject);
    req.end(body);
  });
}

function observeWire(server) {
  const rows = [];
  server.on('request', (req) => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => rows.push({
      body: Buffer.concat(chunks), keyId: req.headers['x-watch-key-id'],
      timestamp: req.headers['x-watch-timestamp'],
    }));
  });
  return rows;
}

test('a lost acknowledgement retries immutable bytes at the current route and key after restart', async t => {
  const f = workspace(t);
  const event = makeEvent();
  const store = outbox(event);
  const key = randomBytes(32);
  const inbox = f.open();
  const receiver = createReceiver({ inbox, consumerId: CONSUMER, keys: new Map([['first', key]]), now: NOW });
  const wire1 = observeWire(receiver);
  receiver.prependListener('request', (_req, res) => {
    const end = res.end;
    res.end = function (...args) {
      if (res.statusCode === 202) { res.destroy(); return res; }
      return end.apply(this, args);
    };
  });
  const port = await f.listen(receiver);
  const routes = new Map([[CONSUMER, { url: `http://127.0.0.1:${port}/inbox`, keyId: 'first', key }]]);
  assert.equal((await deliverOnce(store, routes, { now: NOW })).code, 'transport_error');
  assert.equal(inbox.status(CONSUMER, event.event_id).state, 'queued');
  assert.equal(inbox.status(CONSUMER, event.event_id).attempts, 0);
  await f.stop(receiver);
  f.close(inbox);

  const restarted = f.open();
  const newKey = randomBytes(32);
  const keys = new Map([['second', newKey]]);
  const next = createReceiver({ inbox: restarted, consumerId: CONSUMER, keys, now: NOW + 10 });
  const wire2 = observeWire(next);
  const newPort = await f.listen(next);
  routes.set(CONSUMER, { url: `http://127.0.0.1:${newPort}/inbox`, keyId: 'second', key: newKey });
  assert.equal((await deliverOnce(store, routes, { now: NOW + 10 })).accepted, true);
  assert.deepEqual(wire1[0].body, wire2[0].body);
  assert.deepEqual([wire1[0].keyId, wire2[0].keyId], ['first', 'second']);
  assert.notEqual(wire1[0].timestamp, wire2[0].timestamp);
  assert.equal(restarted.summary(CONSUMER).queued, 1);
  assert.equal((await signedPost(newPort, bytes(event), key, { keyId: 'first' })).status, 401);
  const handled = [];
  const done = await restarted.workOnce(value => { handled.push(value.event_id); }, { now: NOW + 10 });
  assert.equal(done.state, 'done');
  assert.deepEqual(handled, [event.event_id]);
  assert.equal(await restarted.workOnce(() => assert.fail(), { now: NOW + 10 }), null);
  assert.equal(await deliverOnce(store, routes, { now: NOW + 10 }), null);
});

test('receiver authenticates and bounds each body before acceptance and keeps diagnostic replies empty', async t => {
  const f = workspace(t);
  const inbox = f.open({ maxRows: 1 });
  const event = makeEvent();
  const key = randomBytes(32);
  const port = await f.listen(createReceiver({
    inbox, consumerId: CONSUMER, keys: new Map([['test-key', key]]), now: NOW,
  }));
  const body = bytes(event);
  for (const [value, signer, options, status] of [
    [body, randomBytes(32), {}, 401],
    [body, key, { consumer: 'other-worker' }, 401],
    [body, key, { timestamp: NOW - 300_001 }, 401],
    [body, key, { timestamp: NOW + 300_001 }, 401],
    [body, key, { keyId: 'missing-key' }, 401],
    [bytes({ ...event, digest: '0'.repeat(64) }), key, {}, 400],
    [Buffer.from([255]), key, {}, 400],
    [Buffer.alloc(MAX_EVENT_BYTES + 1), key, {}, 413],
  ]) {
    assert.deepEqual(await signedPost(port, value, signer, options), { status, response: '' });
  }
  assert.equal(inbox.status(CONSUMER, event.event_id), null);
  assert.deepEqual(await signedPost(port, body, key), { status: 202, response: '' });
  const conflict = makeEvent({ event_id: event.event_id, watch_id: event.watch_id, sequence: 2 });
  const occupied = makeEvent({ watch_id: event.watch_id, sequence: 1 });
  assert.equal((await signedPost(port, bytes(conflict), key)).status, 409);
  assert.equal((await signedPost(port, bytes(occupied), key)).status, 409);
  assert.equal((await signedPost(port, bytes(makeEvent()), key)).status, 503);
  assert.equal((await signedPost(port, body, key)).status, 200);
  assert.equal(inbox.status(CONSUMER, event.event_id).digest, event.digest);
});

test('failure while enqueuing rolls the receipt back before the receiver replies', async t => {
  const f = workspace(t);
  const inbox = f.open();
  const event = makeEvent();
  const key = randomBytes(32);
  const port = await f.listen(createReceiver({
    inbox, consumerId: CONSUMER, keys: new Map([['test-key', key]]), now: NOW,
  }));
  const probe = new DatabaseSync(f.path);
  try {
    probe.exec(`CREATE TRIGGER stop_queue BEFORE INSERT ON jobs
      BEGIN SELECT RAISE(ABORT,'fixture-job-unavailable'); END;`);
    assert.deepEqual(await signedPost(port, bytes(event), key), { status: 503, response: '' });
    assert.equal(probe.prepare('SELECT count(*) AS n FROM inbox').get().n, 0);
    assert.equal(probe.prepare('SELECT count(*) AS n FROM jobs').get().n, 0);
    probe.exec('DROP TRIGGER stop_queue');
    assert.equal((await signedPost(port, bytes(event), key)).status, 202);
    assert.equal(probe.prepare('SELECT count(*) AS n FROM inbox').get().n, 1);
    assert.equal(probe.prepare('SELECT count(*) AS n FROM jobs').get().n, 1);
  } finally {
    probe.close();
  }
});

test('work waits for predecessor, persists backoff and exposes an exhausted stream independently', async t => {
  const f = workspace(t);
  const first = makeEvent();
  const second = makeEvent({ watch_id: first.watch_id, sequence: 2 });
  const other = makeEvent();
  const inbox = f.open({ maxAttempts: 2, retryBaseMs: 10 });
  assert.equal(inbox.accept(CONSUMER, bytes(second), NOW), 'accepted');
  assert.equal(inbox.status(CONSUMER, second.event_id).blocked_by, 'predecessor');
  assert.equal(await inbox.workOnce(() => assert.fail(), { now: NOW }), null);
  inbox.accept(CONSUMER, bytes(first), NOW);
  const fail = () => { throw new Error('generated handler detail'); };
  const retry = await inbox.workOnce(fail, { now: NOW });
  assert.equal(retry.state, 'queued');
  assert.equal(retry.attempts, 1);
  assert.equal(retry.available_at, NOW + 10);
  assert.equal(retry.code, 'handler_failed');
  assert.equal(JSON.stringify(retry).includes('generated handler detail'), false);
  f.close(inbox);
  const restarted = f.open({ maxAttempts: 9, retryBaseMs: 200 });
  restarted.accept(CONSUMER, bytes(other), NOW + 1);
  const independent = await restarted.workOnce(() => {}, { now: NOW + 1 });
  assert.equal(independent.event_id, other.event_id);
  assert.equal(independent.state, 'done');
  assert.equal(await restarted.workOnce(() => assert.fail(), { now: NOW + 9 }), null);
  const dead = await restarted.workOnce(fail, { now: NOW + 10 });
  assert.equal(dead.event_id, first.event_id);
  assert.equal(dead.state, 'dead');
  assert.equal(dead.attempts, 2);
  assert.deepEqual(restarted.summary(CONSUMER), { queued: 1, running: 0, dead: 1, done: 1, blocked: 1 });
  assert.equal(await restarted.workOnce(() => assert.fail(), { now: NOW + 1000 }), null);
});

test('expired worker lease can be recovered and an old completion cannot overwrite the new job state', async t => {
  const f = workspace(t);
  const event = makeEvent();
  const second = makeEvent({ watch_id: event.watch_id, sequence: 2 });
  const inbox = f.open({ maxAttempts: 3, leaseMs: 5000, retryBaseMs: 10 });
  inbox.accept(CONSUMER, bytes(event), NOW);
  inbox.accept(CONSUMER, bytes(second), NOW);
  let release;
  let began;
  const started = new Promise(resolve => { began = resolve; });
  const paused = new Promise(resolve => { release = resolve; });
  const old = inbox.workOnce(async () => { began(); await paused; }, { now: NOW });
  await started;
  const reopened = f.open();
  try {
    assert.equal(await reopened.workOnce(() => assert.fail(), { now: NOW + 5000 }), null);
    const retried = await reopened.workOnce((value, context) => {
      assert.equal(value.event_id, event.event_id);
      assert.equal(context.attempt, 2);
      assert.equal(context.consumerId, CONSUMER);
    }, { now: NOW + 5010 });
    assert.equal(retried.state, 'done');
    const next = await reopened.workOnce(() => {}, { now: NOW + 5010 });
    assert.equal(next.event_id, second.event_id);
    assert.equal(next.state, 'done');
  } finally {
    release();
  }
  assert.equal((await old).state, 'superseded');
  assert.equal(inbox.status(CONSUMER, event.event_id).attempts, 2);
});

test('receiver refuses an existing database from another application', t => {
  const f = workspace(t);
  const unrelated = new DatabaseSync(f.path);
  try {
    unrelated.exec('CREATE TABLE sample(value INTEGER); INSERT INTO sample VALUES(42);');
    assert.throws(() => f.open(), /incompatible_inbox_database/);
    assert.equal(unrelated.prepare('PRAGMA application_id').get().application_id, 0);
    assert.equal(unrelated.prepare('SELECT value FROM sample').get().value, 42);
    assert.deepEqual(unrelated.prepare("SELECT name FROM sqlite_schema WHERE type='table'").all().map(x => x.name), ['sample']);
  } finally {
    unrelated.close();
  }
});

test('consumer identity scopes both duplicate detection and work selection', async t => {
  const f = workspace(t);
  const inbox = f.open();
  const value = makeEvent();
  for (const id of [CONSUMER, 'another-worker']) {
    assert.equal(inbox.accept(id, bytes(value), NOW), 'accepted');
    assert.equal(inbox.accept(id, bytes(value), NOW), 'duplicate');
  }
  const scoped = await inbox.workOnce((_event, ctx) => {
    assert.equal(ctx.consumerId, 'another-worker');
  }, { now: NOW, consumerId: 'another-worker' });
  assert.equal(scoped.state, 'done');
  assert.equal(inbox.status(CONSUMER, value.event_id).state, 'queued');
  assert.equal(await inbox.workOnce(() => assert.fail(), { now: NOW, consumerId: 'another-worker' }), null);
});

test('a slow async handler receives an abort signal and reaches its persisted attempt limit', async t => {
  const f = workspace(t);
  const inbox = f.open({ maxAttempts: 1, leaseMs: 25 });
  const event = makeEvent();
  inbox.accept(CONSUMER, bytes(event), NOW);
  let aborted = false;
  const result = await inbox.workOnce((_event, { signal }) => new Promise(resolve => {
    signal.addEventListener('abort', () => { aborted = true; resolve(); }, { once: true });
  }), { now: NOW });
  assert.equal(aborted, true);
  assert.equal(result.state, 'dead');
  assert.equal(result.code, 'handler_timeout');
  assert.equal(inbox.summary(CONSUMER).dead, 1);
});

test('sender enforces route and response limits with fixed diagnostic codes', async t => {
  const f = workspace(t);
  const key = randomBytes(32);
  const event = makeEvent();
  const store = outbox(event);
  const routes = new Map();
  assert.equal((await deliverOnce(store, routes, { now: NOW })).code, 'route_unavailable');
  for (const url of ['http://localhost:1234/inbox', 'http://127.0.0.1:1234/inbox?fixture=value']) {
    routes.set(CONSUMER, { url, keyId: 'key', key });
    assert.equal((await deliverOnce(store, routes, { now: NOW })).code, 'route_unavailable');
  }
  let hits = 0;
  const target = await f.listen(createServer((_req, res) => { hits++; res.writeHead(202).end(); }));
  const route = port => routes.set(CONSUMER, { url: `http://127.0.0.1:${port}/inbox`, keyId: 'key', key });
  route(await f.listen(createServer((_req, res) => {
    res.writeHead(307, { location: `http://127.0.0.1:${target}/inbox` }).end();
  })));
  assert.equal((await deliverOnce(store, routes, { now: NOW })).code, 'http_rejected');
  assert.equal(hits, 0);
  route(await f.listen(createServer((_req, res) => { res.writeHead(202).end(Buffer.alloc(4097)); })));
  assert.equal((await deliverOnce(store, routes, { now: NOW })).code, 'response_too_large');
  route(await f.listen(createServer(() => {})));
  assert.equal((await deliverOnce(store, routes, { now: NOW, timeoutMs: 50 })).code, 'delivery_timeout');
});
