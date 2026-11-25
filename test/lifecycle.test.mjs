import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { WatchRuntime } from '../src/runtime.mjs';
import { WatchStore } from '../src/store.mjs';

const until = {
  version: 1, mode: 'until', source_id: 'fixture', consumer_id: 'inbox', interval_ms: 3_600_000,
  predicate: { path: '/phase', equals: 'ready' },
};
const changes = {
  version: 1, mode: 'changes', source_id: 'fixture', consumer_id: 'inbox', interval_ms: 3_600_000,
};
const held = { delivery: { enabled: false } };

function fixtureSource() {
  const fixture = { value: { phase: 'waiting' }, coverage: 'complete', authority: 'authoritative', calls: 0 };
  let nextGate;
  fixture.blockNext = () => {
    const entered = Promise.withResolvers();
    const release = Promise.withResolvers();
    nextGate = { entered, release };
    return { entered: entered.promise, release: release.resolve };
  };
  fixture.read = async ({ mode }) => {
    const result = {
      version: 1, contract: mode === 'until' ? 'durable_ready' : 'observe_change',
      coverage: fixture.coverage, authority: fixture.authority, cursor: String(++fixture.calls),
    };
    if (fixture.coverage !== 'failed') result.value = structuredClone(fixture.value);
    const gate = nextGate;
    nextGate = null;
    if (gate) {
      gate.entered.resolve();
      await gate.release.promise;
    }
    return result;
  };
  return fixture;
}

async function environment(t, storeOptions, runtimeOptions) {
  const dir = await mkdtemp(join(tmpdir(), 'lifecycle-test-'));
  const path = join(dir, 'runtime.sqlite');
  const source = fixtureSource();
  const peers = new Set();
  let time = 10_000;
  function connect() {
    const store = new WatchStore(path, storeOptions);
    const rt = new WatchRuntime({
      store, now: () => time,
      sources: new Map([['fixture', source.read], ['other-fixture', source.read], ['push', null]]),
      routes: new Map([['inbox', {}], ['other-inbox', {}]]), parallelism: 1, ...runtimeOptions,
    });
    const peer = { store, rt };
    peers.add(peer);
    return peer;
  }
  async function close(peer) {
    await peer.rt.stop();
    peer.store.close();
    peers.delete(peer);
  }
  t.after(async () => {
    for (const peer of peers) await close(peer);
    await rm(dir, { recursive: true, force: true });
  });
  let main = connect();
  return {
    source, connect, now: () => time, advance: ms => { time += ms; },
    get rt() { return main.rt; }, get store() { return main.store; },
    async reopen() { await close(main); main = connect(); },
  };
}

function saved(store, watchId) {
  return store.one('SELECT state,checkpoint,sequence,next_read_ms FROM watches WHERE watch_id=?', watchId);
}

function eventBytes(store, watchId) {
  return store.db.prepare('SELECT event_id,body FROM events WHERE watch_id=? ORDER BY sequence').all(watchId);
}

const setDelivery = enabled => ({ version: 1, delivery: { enabled } });

test('create replays the original definition after edits, while stale concurrent revisions cannot edit it', async t => {
  const h = await environment(t);
  const config = { ...until, predicate: { path: '/fixture_path', equals: 'fixture_needle' } };
  const initial = await h.rt.createConfigured(config, 'one');
  const id = initial.watch_id;
  assert.deepEqual([initial.config_version, initial.revision, initial.delivery_enabled], [1, 1, true]);
  assert.doesNotMatch(JSON.stringify(initial.configuration), /fixture_path|fixture_needle/);
  const peer = h.connect();
  const revised = await peer.rt.update(id, initial.revision, { version: 1, interval_ms: 9000 });
  assert.equal(revised.revision, initial.revision + 1);
  assert.notEqual(revised.configuration.config_digest, initial.configuration.config_digest);
  assert.equal(revised.configuration.predicate_digest, initial.configuration.predicate_digest);
  await assert.rejects(async () => h.rt.update(id, initial.revision, setDelivery(false)), /revision_conflict/);
  const same = await h.rt.update(id, revised.revision, { version: 1, interval_ms: 9000 });
  assert.equal(same.revision, revised.revision);
  const paused = await h.rt.pause(id, same.revision);
  assert.equal(paused.revision, same.revision + 1);
  assert.equal((await peer.rt.pause(id, paused.revision)).revision, paused.revision);
  assert.equal(paused.state, 'paused');
  await h.reopen();
  const replay = await h.rt.createConfigured(config, 'one');
  assert.equal(replay.watch_id, id);
  assert.equal(replay.revision, paused.revision);
  assert.equal(replay.configuration.interval_ms, 9000);
  assert.equal(replay.state, 'paused');
  await assert.rejects(async () => h.rt.createConfigured({ ...config, interval_ms: 9000 }, 'one'), /request_conflict/);
  const resumed = await h.rt.resume(id, replay.revision);
  assert.equal(resumed.revision, paused.revision + 1);
  assert.equal((await peer.rt.resume(id, resumed.revision)).revision, resumed.revision);
  assert.equal((await h.rt.list({})).watches.length, 1);
});

test('preview leaves the schedule, deadline and checkpoint untouched, including at the exact deadline', async t => {
  const h = await environment(t);
  const config = { ...until, deadline_ms: h.now() + 10 };
  const unsampled = await h.rt.preview(config);
  assert.equal(unsampled.basis, 'unavailable');
  assert.equal(unsampled.truth, 'UNKNOWN');
  assert.equal(h.source.calls, 0);
  assert.equal(h.store.one('SELECT COUNT(*) AS n FROM watches').n, 0);
  const watch = await h.rt.createConfigured(config, 'preview');
  const before = saved(h.store, watch.watch_id);
  h.advance(10);
  const result = await h.rt.preview({ ...config, predicate: { path: '/phase', equals: 'waiting' } }, watch.watch_id);
  assert.deepEqual([result.basis, result.truth, result.coverage, result.authority],
    ['checkpoint', 'TRUE', 'complete', 'authoritative']);
  assert.doesNotMatch(JSON.stringify(result), /waiting|\/phase/);
  for (const mismatch of [changes, { ...config, source_id: 'other-fixture' }, { ...config, consumer_id: 'other-inbox' }]) {
    await assert.rejects(async () => h.rt.preview(mismatch, watch.watch_id), /invalid_request/);
  }
  assert.equal((await h.rt.list({ state: 'active' })).watches.length, 1);
  assert.deepEqual(saved(h.store, watch.watch_id), before);
  assert.equal(h.source.calls, 1);
  assert.equal(h.store.one('SELECT COUNT(*) AS n FROM outbox').n, 0);
  assert.equal(h.rt.get(watch.watch_id).state, 'deadline');
  assert.deepEqual(h.rt.history(watch.watch_id).events.map(e => e.kind), ['deadline']);
});

test('a noncooperative preview retains only its own slot after timeout while collection continues', async t => {
  const h = await environment(t, undefined, { parallelism: 2, collectTimeoutMs: 50 });
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const entered = Promise.withResolvers();
  const late = Promise.withResolvers();
  let previewCalls = 0;
  const observation = { version: 1, contract: 'durable_ready', coverage: 'complete',
    authority: 'authoritative', value: { phase: 'ready' } };
  h.source.read.preview = ({ signal }) => {
    if (++previewCalls === 1) {
      entered.resolve(signal);
      return late.promise;
    }
    return observation;
  };
  const first = h.rt.preview(until);
  const signal = await entered.promise;
  try {
    assert.equal((await h.rt.preview(until)).basis, 'unavailable');
    assert.equal(previewCalls, 1);
    const watch = await h.rt.createConfigured(until, 'independent-collection');
    assert.equal(watch.truth, 'FALSE');
    t.mock.timers.tick(50);
    const timedOut = await first;
    assert.equal(signal.aborted, true);
    assert.deepEqual([timedOut.truth, timedOut.coverage, timedOut.code], ['UNKNOWN', 'failed', 'source_timeout']);
    assert.equal((await h.rt.preview(until)).basis, 'unavailable');
    await h.rt.checkNow(watch.watch_id, watch.revision, 1);
    assert.equal(h.source.calls, 2);
    assert.equal(previewCalls, 1);
  } finally {
    late.resolve(observation);
  }
  await new Promise(resolve => setImmediate(resolve));
  const fresh = await h.rt.preview(until);
  assert.deepEqual([fresh.basis, fresh.truth, previewCalls], ['sample', 'TRUE', 2]);
  assert.equal(h.store.one('SELECT COUNT(*) AS n FROM events').n, 0);
});

test('checkpoint preview reports unknown for degraded coverage with or without an earlier reliable value', async t => {
  const h = await environment(t);
  h.source.value = { phase: 'ready' };
  h.source.coverage = 'partial';
  const watch = await h.rt.createConfigured(until, 'degraded');
  const id = watch.watch_id;
  for (const [index, coverage] of ['partial', 'failed', 'complete', 'partial', 'failed'].entries()) {
    h.source.coverage = coverage;
    if (index > 0) {
      h.source.value = { phase: 'waiting' };
      await h.rt.checkNow(id, watch.revision, index);
    }
    const before = saved(h.store, id);
    assert.equal(Object.hasOwn(JSON.parse(before.checkpoint), 'value'), index >= 2);
    const preview = await h.rt.preview({ ...until, predicate: { path: '/phase', equals: 'waiting' } }, id);
    assert.deepEqual([preview.basis, preview.truth, preview.coverage],
      ['checkpoint', coverage === 'complete' ? 'TRUE' : 'UNKNOWN', coverage]);
    assert.equal(h.source.calls, index + 1);
    assert.deepEqual(saved(h.store, id), before);
  }
});

test('list returns bounded keyset pages with a stable state filter', async t => {
  const h = await environment(t);
  const watches = [];
  for (const request of ['alpha', 'beta', 'gamma', 'delta']) watches.push(await h.rt.createConfigured(until, request));
  const paused = watches.pop();
  await h.rt.pause(paused.watch_id, paused.revision);
  const first = await h.rt.list({ state: 'active', limit: 2 });
  assert.equal(first.watches.length, 2);
  assert.equal(first.has_more, true);
  assert.equal(typeof first.next_cursor, 'string');
  const last = await h.rt.list({ state: 'active', limit: 2, after: first.next_cursor });
  assert.equal(last.has_more, false);
  assert.equal(last.watches.length, 1);
  assert.deepEqual([...first.watches, ...last.watches].map(w => w.watch_id), watches.map(w => w.watch_id).sort());
  assert.doesNotMatch(JSON.stringify(first), /"equals"|\/phase/);
  assert.deepEqual((await h.rt.list({ state: 'paused' })).watches.map(w => w.watch_id), [paused.watch_id]);
  for (const options of [{ limit: 0 }, { limit: 51 }, { state: 'unknown-state' }, { after: '../invalid' }]) {
    await assert.rejects(async () => h.rt.list(options), /invalid_request/);
  }
});

test('manual checks use the ordinary authority, coverage, cursor and terminal rules and replay once', async t => {
  const h = await environment(t);
  const watch = await h.rt.createConfigured({ ...until, ...held }, 'manual');
  const id = watch.watch_id;
  const revision = watch.revision;
  assert.equal(watch.truth, 'FALSE');
  const baseline = JSON.parse(saved(h.store, id).checkpoint);
  h.source.value = { phase: 'ready' };
  for (const [index, [coverage, authority]] of [
    ['partial', 'authoritative'], ['complete', 'advisory'], ['failed', 'authoritative'],
  ].entries()) {
    const seq = index + 1;
    Object.assign(h.source, { coverage, authority });
    const done = await h.rt.checkNow(id, revision, seq);
    assert.deepEqual([done.watch_id, done.operation_seq, done.revision, done.state], [id, seq, revision, 'completed']);
    const status = h.rt.get(id);
    assert.deepEqual([status.truth, status.coverage, status.authority, status.sequence], ['UNKNOWN', coverage, authority, 0]);
    const latest = JSON.parse(saved(h.store, id).checkpoint);
    assert.equal(latest.cursor, baseline.cursor);
    assert.deepEqual(latest.value, baseline.value);
    if (seq === 1) await h.reopen();
    const calls = h.source.calls;
    assert.deepEqual(await h.rt.checkNow(id, revision, seq), done);
    assert.equal(h.source.calls, calls);
    await assert.rejects(async () => h.rt.checkNow(id, revision, seq + 2), /operation_conflict/);
  }
  Object.assign(h.source, { coverage: 'complete', authority: 'authoritative' });
  const finished = await h.rt.checkNow(id, revision, 4);
  assert.equal(finished.state, 'completed');
  const ready = h.rt.get(id);
  assert.deepEqual([ready.state, ready.truth, ready.sequence], ['ready', 'TRUE', 1]);
  assert.deepEqual(await h.rt.checkNow(id, revision, 4), finished);
  await assert.rejects(async () => h.rt.checkNow(id, ready.revision, 5), /watch_terminal/);
  assert.equal(h.source.calls, 5);
});

test('collection pause fences a live read across resume and leaves committed delivery claimable', async t => {
  const h = await environment(t);
  const watch = await h.rt.createConfigured(changes, 'pause');
  const id = watch.watch_id;
  h.source.value = { phase: 'one' };
  await h.rt.checkNow(id, watch.revision, 1);
  const before = saved(h.store, id);
  const [original] = eventBytes(h.store, id);
  h.source.value = { phase: 'two' };
  const gate = h.source.blockNext();
  const pending = h.rt.checkNow(id, watch.revision, 2);
  await gate.entered;
  const peer = h.connect();
  try {
    const paused = await peer.rt.pause(id, watch.revision);
    assert.equal(paused.state, 'paused');
    assert.equal(paused.delivery_enabled, true);
    await assert.rejects(async () => peer.rt.checkNow(id, paused.revision, 3), /watch_paused/);
    const delivery = peer.store.claimDelivery(h.now());
    assert.equal(delivery.event_id, original.event_id);
    assert.equal(delivery.body, original.body);
    assert.equal(peer.store.finishDelivery(delivery, { accepted: true, code: 'accepted', now: h.now() }), true);
    assert.equal((await peer.rt.resume(id, paused.revision)).state, 'active');
  } finally {
    gate.release();
  }
  assert.equal((await pending).state, 'superseded');
  const after = saved(h.store, id);
  assert.equal(after.checkpoint, before.checkpoint);
  assert.equal(after.sequence, before.sequence);
  assert.equal(h.rt.get(id).manual_check.state, 'superseded');
  assert.equal(h.rt.get(id).delivery.accepted, 1);
});

test('delivery edits preserve live observation, ordered event bytes and terminal state across a restart', async t => {
  const h = await environment(t);
  const watch = await h.rt.createConfigured(changes, 'delivery');
  const id = watch.watch_id;
  const before = saved(h.store, id);
  h.source.value = { n: 1 };
  const gate = h.source.blockNext();
  const pending = h.rt.checkNow(id, watch.revision, 1);
  await gate.entered;
  let disabled;
  try {
    disabled = await h.connect().rt.update(id, watch.revision, setDelivery(false));
    assert.equal(disabled.delivery_enabled, false);
    assert.equal(saved(h.store, id).checkpoint, before.checkpoint);
  } finally {
    gate.release();
  }
  assert.equal((await pending).state, 'completed');
  assert.equal(h.rt.get(id).sequence, 1);
  const [first] = eventBytes(h.store, id);
  assert.equal(h.store.claimDelivery(h.now()), null);
  h.source.value = { n: 2 };
  await h.rt.checkNow(id, disabled.revision, 2);
  assert.equal(h.rt.get(id).sequence, 2);
  assert.deepEqual(eventBytes(h.store, id)[0], first);
  const canceled = await h.rt.cancel(id);
  assert.equal(canceled.state, 'canceled');
  const terminal = saved(h.store, id);
  const original = eventBytes(h.store, id);
  assert.deepEqual(original.map(e => JSON.parse(e.body).kind), ['changed', 'changed', 'canceled']);
  assert.equal(h.store.claimDelivery(h.now()), null);
  await h.reopen();
  const release = await h.rt.update(id, canceled.revision, setDelivery(true));
  assert.equal(release.delivery_enabled, true);
  assert.equal(release.state, 'canceled');
  assert.deepEqual(saved(h.store, id), terminal);
  assert.deepEqual(eventBytes(h.store, id), original);
  for (const event of original) {
    const claim = h.store.claimDelivery(h.now());
    assert.equal(claim.event_id, event.event_id);
    assert.equal(claim.body, event.body);
    assert.equal(h.store.finishDelivery(claim, { accepted: true, code: 'accepted', now: h.now() }), true);
  }
  assert.equal(h.store.claimDelivery(h.now()), null);
  await assert.rejects(async () => h.rt.resume(id, release.revision), /watch_terminal/);
  await assert.rejects(async () => h.rt.pause(id, release.revision), /watch_terminal/);
  await assert.rejects(async () => h.rt.update(id, release.revision, { version: 1, interval_ms: 50 }), /watch_terminal/);
  await assert.rejects(async () => h.rt.checkNow(id, release.revision, 3), /watch_terminal/);
  assert.equal((await h.rt.cancel(id)).sequence, original.length);
});

test('a queued check persisted before any read claim survives reopening without duplicate collection', async t => {
  const h = await environment(t);
  const busy = await h.rt.createConfigured(until, 'occupy');
  const target = await h.rt.createConfigured(until, 'queued');
  const id = target.watch_id;
  const gate = h.source.blockNext();
  const first = h.rt.checkNow(busy.watch_id, busy.revision, 1);
  await gate.entered;
  try {
    const queued = await h.rt.checkNow(id, target.revision, 1);
    assert.equal(queued.state, 'queued');
    assert.deepEqual(await h.rt.checkNow(id, target.revision, 1), queued);
    await assert.rejects(async () => h.rt.checkNow(id, target.revision, 2), /check_pending/);
    assert.equal(h.rt.get(id).reads, 1);
    assert.equal(h.source.calls, 3);
  } finally {
    gate.release();
  }
  await first;
  await h.reopen();
  assert.equal(h.rt.get(id).manual_check.state, 'queued');
  await h.rt.tick();
  const recovered = h.rt.get(id);
  assert.deepEqual([recovered.manual_check.state, recovered.manual_check.operation_seq, recovered.reads], ['completed', 1, 2]);
  const duplicate = await h.rt.checkNow(id, target.revision, 1);
  assert.deepEqual(duplicate, recovered.manual_check);
  assert.equal(h.source.calls, 4);
});

test('a manual request arriving during a scheduled read survives that earlier read and a restart', async t => {
  const h = await environment(t);
  const watch = await h.rt.createConfigured({ ...until, interval_ms: 20, ...held }, 'already-reading');
  const id = watch.watch_id;
  h.advance(20);
  const gate = h.source.blockNext();
  const scheduled = h.rt.tick();
  await gate.entered;
  try {
    assert.equal((await h.rt.checkNow(id, watch.revision, 1)).state, 'queued');
    h.source.value = { phase: 'ready' };
  } finally {
    gate.release();
  }
  await scheduled;
  const earlier = h.rt.get(id);
  assert.deepEqual([earlier.truth, earlier.reads, earlier.manual_check.state], ['FALSE', 2, 'queued']);
  await h.reopen();
  await h.rt.tick();
  const finished = h.rt.get(id);
  assert.deepEqual([finished.state, finished.reads, finished.sequence, finished.manual_check.state], ['ready', 3, 1, 'completed']);
  assert.equal(h.source.calls, 3);
});

test('a reclaimed manual lease rejects the stale completion from its former owner', async t => {
  const h = await environment(t);
  const watch = await h.rt.createConfigured(until, 'lease');
  const id = watch.watch_id;
  h.source.value = { phase: 'ready' };
  const gate = h.source.blockNext();
  const oldOwner = h.rt.checkNow(id, watch.revision, 1);
  await gate.entered;
  const newOwner = h.connect();
  let committed;
  try {
    assert.equal(newOwner.rt.get(id).manual_check.state, 'running');
    h.advance(60_001);
    h.source.value = { phase: 'waiting' };
    await newOwner.rt.tick();
    const recovered = newOwner.rt.get(id);
    assert.deepEqual([recovered.state, recovered.truth, recovered.manual_check.state, recovered.reads],
      ['active', 'FALSE', 'completed', 2]);
    committed = saved(newOwner.store, id);
  } finally {
    gate.release();
  }
  await oldOwner;
  assert.deepEqual(saved(h.store, id), committed);
  assert.equal(h.rt.history(id).events.length, 0);
  assert.equal(h.source.calls, 3);
});

test('a predicate edit, cancellation or absolute deadline supersedes an in-flight manual sample', async t => {
  for (const transition of ['predicate', 'canceled', 'deadline']) {
    await t.test(transition, async t => {
      const h = await environment(t);
      const watch = await h.rt.createConfigured({ ...until, ...held, deadline_ms: h.now() + 10 }, 'fence');
      const id = watch.watch_id;
      const before = saved(h.store, id);
      h.source.value = { phase: 'ready' };
      const gate = h.source.blockNext();
      const pending = h.rt.checkNow(id, watch.revision, 1);
      await gate.entered;
      const peer = h.connect();
      try {
        if (transition === 'predicate') {
          await peer.rt.update(id, watch.revision, { version: 1, predicate: { path: '/phase', equals: 'next' } });
        } else if (transition === 'canceled') {
          assert.equal((await peer.rt.cancel(id)).state, 'canceled');
        } else {
          h.advance(10);
          assert.equal(peer.rt.get(id).state, 'deadline');
        }
      } finally {
        gate.release();
      }
      assert.equal((await pending).state, 'superseded');
      assert.equal(saved(h.store, id).checkpoint, before.checkpoint);
      const status = h.rt.get(id);
      assert.equal(status.manual_check.state, 'superseded');
      if (transition === 'predicate') {
        assert.equal(status.state, 'active');
        assert.equal(status.sequence, 0);
        h.source.value = { phase: 'next' };
        await h.rt.checkNow(id, status.revision, 2);
        assert.equal(h.rt.get(id).state, 'ready');
      } else {
        assert.equal(status.state, transition);
        assert.deepEqual(h.rt.history(id).events.map(e => e.kind), [transition]);
      }
    });
  }
});

test('a paused watch reaches its deadline even when no collection is due', async t => {
  const h = await environment(t);
  const watch = await h.rt.createConfigured({ ...until, ...held, deadline_ms: h.now() + 10 }, 'paused-deadline');
  const id = watch.watch_id;
  const pause = await h.rt.pause(id, watch.revision);
  h.advance(9);
  await h.rt.tick();
  assert.equal(h.rt.get(id).state, 'paused');
  h.advance(1);
  await assert.rejects(async () => h.rt.update(id, pause.revision, { version: 1, deadline_ms: null }),
    /watch_terminal|revision_conflict/);
  const ended = h.rt.get(id);
  assert.equal(ended.state, 'deadline');
  await assert.rejects(async () => h.rt.resume(id, ended.revision), /watch_terminal/);
  assert.deepEqual(h.rt.history(id).events.map(e => e.kind), ['deadline']);
  assert.equal(h.source.calls, 1);
});

test('manual reads respect event capacity and require a source that can be collected', async t => {
  const h = await environment(t, { maxEventsPerWatch: 2 });
  const watch = await h.rt.createConfigured({ ...changes, ...held }, 'capacity');
  h.source.value = { n: 1 };
  await h.rt.checkNow(watch.watch_id, watch.revision, 1);
  h.source.value = { n: 2 };
  await h.rt.checkNow(watch.watch_id, watch.revision, 2);
  const ended = h.rt.get(watch.watch_id);
  assert.deepEqual([ended.state, ended.code, ended.sequence], ['error', 'capacity', 2]);
  assert.deepEqual(h.rt.history(watch.watch_id).events.map(e => e.kind), ['changed', 'error']);
  await assert.rejects(async () => h.rt.checkNow(watch.watch_id, ended.revision, 3), /watch_terminal/);
  const push = await h.rt.createConfigured({ ...changes, mode: 'emitted', source_id: 'push' }, 'push');
  await assert.rejects(async () => h.rt.checkNow(push.watch_id, push.revision, 1), /unsupported_control/);
  assert.equal(h.rt.get(push.watch_id).manual_check, null);
});
