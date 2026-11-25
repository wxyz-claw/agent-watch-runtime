import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { test } from 'node:test';
import { WatchRuntime } from '../src/runtime.mjs';
import { WatchStore } from '../src/store.mjs';

function databaseFile(t) {
  const dir = mkdtempSync(join(tmpdir(), 'watch-control-storage-'));
  const path = join(dir, 'store.sqlite');
  let store;
  t.after(() => {
    store?.close();
    rmSync(dir, { recursive: true, force: true });
  });
  return {
    path,
    open(options) {
      store?.close();
      store = undefined;
      store = new WatchStore(path, options);
      return store;
    },
  };
}

const v3Definition = '{"consumer_id":"worker","deadline_ms":null,"interval_ms":250,"mode":"changes","request_id":"initial","source_id":"sample"}';
const v3Checkpoint = '{"reads":3,"cursor":"frame-3","value":{"phase":"ready"}}';
const v3Body = JSON.stringify({
  version: 1, event_id: 'event-v3', watch_id: 'watch-v3', sequence: 1, kind: 'changed', observed_at: 1000,
  evidence: { source_digest: '90593b5d5814c41932e1b8bef45de63949ecb021dde26d6bacf931324a67de23' },
  digest: '853f7626ba38e5f82d7289aa7effb7d5042bd38200e7979ebbc15dfde67a2984',
});

// This fixture defines the v3 file directly. It needs neither an older checkout nor the current migration.
function writeV3(path, { attempt = 1, due = 2090 } = {}) {
  const db = new DatabaseSync(path);
  try {
    db.exec(`
      PRAGMA application_id=0x41575231;
      PRAGMA user_version=3;
      CREATE TABLE watches (
        watch_id TEXT PRIMARY KEY, consumer_id TEXT NOT NULL, request_id TEXT NOT NULL,
        source_id TEXT NOT NULL, mode TEXT NOT NULL, definition TEXT NOT NULL, deadline_ms INTEGER,
        interval_ms INTEGER NOT NULL, state TEXT NOT NULL DEFAULT 'active', truth TEXT NOT NULL DEFAULT 'UNKNOWN',
        coverage TEXT, authority TEXT, code TEXT, sequence INTEGER NOT NULL DEFAULT 0,
        checkpoint TEXT NOT NULL DEFAULT '{"reads":0,"cursor":null}', next_read_ms INTEGER,
        collect_token TEXT, collect_until_ms INTEGER, UNIQUE(consumer_id,request_id)
      );
      CREATE INDEX watch_due ON watches(state,next_read_ms);
      CREATE INDEX watch_deadline ON watches(state,deadline_ms);
      CREATE TABLE events (
        event_id TEXT PRIMARY KEY, watch_id TEXT NOT NULL REFERENCES watches(watch_id),
        sequence INTEGER NOT NULL, body TEXT NOT NULL, UNIQUE(watch_id,sequence)
      );
      CREATE TABLE emitted (
        watch_id TEXT NOT NULL REFERENCES watches(watch_id), source_event_digest TEXT NOT NULL,
        payload_digest TEXT NOT NULL, PRIMARY KEY(watch_id,source_event_digest)
      );
      CREATE TABLE outbox (
        event_id TEXT PRIMARY KEY REFERENCES events(event_id), state TEXT NOT NULL DEFAULT 'pending',
        attempt INTEGER NOT NULL DEFAULT 0, next_ms INTEGER NOT NULL, token TEXT, lease_ms INTEGER, code TEXT,
        max_attempts INTEGER NOT NULL, retry_base_ms INTEGER NOT NULL, retry_cap_ms INTEGER NOT NULL
      );
      CREATE INDEX delivery_due ON outbox(state,next_ms);
      CREATE TABLE delivery_routes (
        consumer_id TEXT PRIMARY KEY, fingerprint TEXT, paused INTEGER NOT NULL DEFAULT 0 CHECK(paused IN (0,1))
      );
      INSERT INTO delivery_routes(consumer_id) VALUES ('worker');
    `);
    db.prepare(`INSERT INTO watches(watch_id,consumer_id,request_id,source_id,mode,definition,interval_ms,
        truth,coverage,authority,sequence,checkpoint,next_read_ms)
      VALUES ('watch-v3','worker','initial','sample','changes',?,250,'TRUE','complete','authoritative',1,?,9000)`)
      .run(v3Definition, v3Checkpoint);
    db.prepare("INSERT INTO events VALUES ('event-v3','watch-v3',1,?)").run(v3Body);
    db.prepare(`INSERT INTO outbox(event_id,state,attempt,next_ms,code,max_attempts,retry_base_ms,retry_cap_ms)
      VALUES ('event-v3','pending',?,?,?,5,9,40)`).run(attempt, due, attempt ? 'transport_error' : null);
  } finally {
    db.close();
  }
}

test('opening a v3 store preserves a pending retry and the original watch across versioned configuration edits', async t => {
  const file = databaseFile(t);
  writeV3(file.path);
  const options = { maxAttempts: 2, baseDelayMs: 500, maxDelayMs: 900, retryJitterMs: 60_000 };
  let store = file.open(options);
  let now = 2000;
  let reads = 0;
  const runtime = new WatchRuntime({
    store, now: () => now, sources: new Map([['sample', () => { reads++; }]]), routes: new Map([['worker', {}]]),
  });
  const legacy = { source_id: 'sample', consumer_id: 'worker', request_id: 'initial', interval_ms: 250 };
  const config = { version: 1, mode: 'changes', source_id: 'sample', consumer_id: 'worker', interval_ms: 250 };
  const original = runtime.get('watch-v3');
  assert.equal(store.one('PRAGMA user_version').user_version, 4);
  assert.equal(original.revision, 1);
  assert.equal(original.delivery_enabled, true);
  assert.equal(original.reads, 3);
  assert.deepEqual([original.delivery_head.attempt, original.delivery_head.max_attempts, original.delivery_head.next_ms],
    [1, 5, 2090]);
  assert.deepEqual(await runtime.create('changes', legacy), original);
  assert.deepEqual(await runtime.createConfigured({ ...config, delivery: { enabled: true }, deadline_ms: null }, 'initial'), original);
  const held = runtime.update('watch-v3', 1, { version: 1, interval_ms: 500, delivery: { enabled: false } });
  assert.equal(held.revision, 2);
  assert.equal(held.configuration.interval_ms, 500);
  assert.equal(held.delivery_enabled, false);
  assert.deepEqual(await runtime.createConfigured(config, 'initial'), held);
  assert.deepEqual(await runtime.create('changes', legacy), held);
  await assert.rejects(runtime.createConfigured({ ...config, interval_ms: 500 }, 'initial'), /request_conflict/);
  now = 2090;
  assert.equal(store.claimDelivery(now), null);
  assert.equal(runtime.update('watch-v3', 2, { version: 1, delivery: { enabled: true } }).revision, 3);
  assert.equal(store.claimDelivery(now - 1), null);
  const claim = store.claimDelivery(now);
  assert.deepEqual([claim.event_id, claim.attempt, claim.body], ['event-v3', 2, v3Body]);
  assert.ok(store.finishDelivery(claim, { accepted: false, code: 'transport_error', now }));
  assert.equal(store.get('watch-v3').delivery_head.next_ms, 2090 + 18);
  assert.equal(reads, 0);

  store = file.open(options);
  assert.deepEqual({ ...store.one('SELECT definition,checkpoint FROM watches') },
    { definition: v3Definition, checkpoint: v3Checkpoint });
  assert.equal(store.one('SELECT body FROM events WHERE event_id=?', 'event-v3').body, v3Body);
  assert.deepEqual(store.history('watch-v3').events, [JSON.parse(v3Body)]);
  assert.equal(store.get('watch-v3').revision, 3);
  assert.equal(store.one('SELECT COUNT(*) AS n FROM watches').n, 1);
  assert.equal(store.claimDelivery(2107), null);
  assert.equal(store.claimDelivery(2108).attempt, 3);
});

function enqueue(store, requestId, now) {
  const result = store.create({
    mode: 'emitted', source_id: 'sample', consumer_id: 'worker', request_id: requestId, interval_ms: 1000, deadline_ms: null,
  }, now, false);
  return store.cancel(result.watch_id, now).delivery_head.event_id;
}

// The first eight hex digits encode the leading unsigned, big-endian 32-bit integer.
function expectedJitter(eventId, attempt, limit) {
  const hex = createHash('sha256').update('watch-retry-v1/' + eventId + '/' + attempt).digest('hex');
  return parseInt(hex.slice(0, 8), 16) % (limit + 1);
}

test('jitter is selected on each event’s first claim and survives process defaults changing', t => {
  const file = databaseFile(t);
  let now = 10_000;
  writeV3(file.path, { attempt: 0, due: now });
  const firstId = 'event-v3';
  assert.deepEqual([1, 3].map(attempt => expectedJitter(firstId, attempt, 300)), [48, 70]);
  let store = file.open({ retryJitterMs: 300, baseDelayMs: 100, maxDelayMs: 900, maxAttempts: 8 });
  const first = store.claimDelivery(now);
  assert.equal(first.event_id, firstId);
  assert.ok(store.finishDelivery(first, { accepted: false, code: 'transport_error', now }));
  const firstDue = now + 100 + expectedJitter(firstId, 1, 300);
  assert.equal(store.get(first.watch_id).delivery_head.next_ms, firstDue);

  // Enqueue under the first sender, then claim under a sender with the default of zero jitter.
  const secondId = enqueue(store, 'second', now);
  store = file.open({ baseDelayMs: 4, maxDelayMs: 8, maxAttempts: 2 });
  const second = store.claimDelivery(now);
  assert.equal(second.event_id, secondId);
  assert.ok(store.finishDelivery(second, { accepted: false, code: 'transport_error', now }));
  assert.equal(store.get(second.watch_id).delivery_head.next_ms, now + 4);
  const secondRetry = store.claimDelivery(now + 4);
  assert.ok(store.finishDelivery(secondRetry, { accepted: true, code: 'accepted', now: now + 4 }));
  assert.equal(store.claimDelivery(firstDue - 1), null);

  now = firstDue;
  for (const [attempt, hint, delay] of [
    [2, 850, 850],
    [3, 1, 400 + expectedJitter(firstId, 3, 300)],
    [4, 5000, 900],
    [5, 0, 900],
  ]) {
    const claim = store.claimDelivery(now);
    assert.deepEqual([claim.event_id, claim.attempt, claim.body], [firstId, attempt, first.body]);
    assert.ok(store.finishDelivery(claim, {
      accepted: false, code: hint ? 'rate_limited' : 'transport_error', retryAfterMs: hint, now,
    }));
    now += delay;
    const head = store.get(first.watch_id).delivery_head;
    assert.deepEqual([head.next_ms, head.max_attempts, head.state], [now, 8, 'pending']);
    assert.equal(store.claimDelivery(now - 1), null);
  }
});

test('retry jitter accepts only safe integer milliseconds from zero through sixty thousand', () => {
  for (const retryJitterMs of [-1, 60_001, 0.5, Infinity, NaN, null, '1']) {
    assert.throws(() => new WatchStore(':memory:', { retryJitterMs }), /invalid_request/);
  }
  for (const retryJitterMs of [0, 60_000]) new WatchStore(':memory:', { retryJitterMs }).close();
});
