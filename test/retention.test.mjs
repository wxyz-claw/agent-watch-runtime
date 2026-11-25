import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { WatchRuntime } from '../src/runtime.mjs';
import { WatchStore } from '../src/store.mjs';
import { Inbox } from '../src/delivery.mjs';
import { sealEvent } from '../src/sdk.mjs';

const DAY = 86400000;
const request = {source_id:'source',consumer_id:'worker',request_id:'request'};
const occurrence = (source_event_id,value) => ({
  version:1,contract:'source_emitted',coverage:'complete',authority:'authoritative',source_event_id,value,
});

async function environment(t, {store:storeOptions = {}, runtime:runtimeOptions = {}} = {}) {
  const dir = await mkdtemp(join(tmpdir(),'watch-retention-test-'));
  const database = join(dir,'runtime.sqlite');
  let time = 100000;
  const sources = new Map([['source',null]]);
  const routes = new Map([['worker',{}]]);
  let store, runtime;
  const open = () => {
    store = new WatchStore(database,storeOptions);
    runtime = new WatchRuntime({store,sources,routes,now:() => time,intervalMs:20,...runtimeOptions});
  };
  open();
  t.after(async () => { await runtime.stop(); store.close(); await rm(dir,{recursive:true,force:true}); });
  return {dir,get store(){return store;},get rt(){return runtime;},now:() => time,
    advance:n => { time += n; },reopen:async () => { await runtime.stop(); store.close(); open(); }};
}

// Settle the next deliverable event with a receiver outcome, without a network round trip.
function settle(h, outcome = 'accepted') {
  const claim = h.store.claimDelivery(h.now());
  if (!claim) return null;
  h.store.finishDelivery(claim,{accepted:outcome === 'accepted',code:outcome,now:h.now()});
  return claim;
}

function exhaust(h) {
  let claim;
  while ((claim = h.store.claimDelivery(h.now()))) {
    h.store.finishDelivery(claim,{accepted:false,code:'http_rejected',now:h.now()});
    h.advance(DAY);
  }
}

test('retention lets a long-lived emitted watch outgrow the per-watch event limit', async t => {
  const h = await environment(t,{store:{maxEventsPerWatch:4},runtime:{retentionMs:1000}});
  const {watch_id} = await h.rt.create('emitted',request);
  for (let n = 1; n <= 12; n++) {
    assert.equal(h.rt.ingest('source',watch_id,occurrence('occurrence-'+n,n)).state,'active',`occurrence ${n}`);
    assert.ok(settle(h));
    h.rt.prune();
    h.advance(1000);
  }
  const status = h.rt.get(watch_id);
  assert.equal(status.sequence,12);
  assert.equal(status.pruned_through,11);
  assert.equal(status.retained_events,1);
  const page = h.rt.history(watch_id);
  assert.deepEqual(page.events.map(e => e.sequence),[12]);
  assert.equal(page.pruned_through,11);
  // Deduplication outlives pruned history.
  h.rt.ingest('source',watch_id,occurrence('occurrence-3',3));
  assert.equal(h.rt.get(watch_id).sequence,12);
});

test('without settled delivery the retained-event limit still ends the watch', async t => {
  const h = await environment(t,{store:{maxEventsPerWatch:3},runtime:{retentionMs:0}});
  const {watch_id} = await h.rt.create('emitted',request);
  for (let n = 1; n <= 3; n++) h.rt.ingest('source',watch_id,occurrence('o-'+n,n));
  h.rt.prune();
  const status = h.rt.get(watch_id);
  assert.equal(status.state,'error');
  assert.equal(status.code,'capacity');
  assert.equal(status.retained_events,3);
  assert.equal(status.pruned_through,0);
});

test('pruning keeps pending, dead and recent events and their order', async t => {
  const h = await environment(t,{store:{maxAttempts:1},runtime:{retentionMs:DAY}});
  const {watch_id} = await h.rt.create('emitted',request);
  for (let n = 1; n <= 3; n++) h.rt.ingest('source',watch_id,occurrence('o-'+n,n));
  settle(h);
  settle(h,'http_rejected');
  h.advance(DAY);
  assert.deepEqual(h.rt.prune(),{watches:0,events:1});
  const status = h.rt.get(watch_id);
  assert.equal(status.retained_events,2);
  assert.equal(status.dead_letter.sequence,2);
  assert.equal(status.delivery_head.sequence,2);
  assert.equal(h.store.claimDelivery(h.now()),null);
  assert.deepEqual(h.rt.history(watch_id).events.map(e => e.sequence),[2,3]);
});

test('a finished watch is removed only after every delivery settled and the retention period passed', async t => {
  const h = await environment(t,{store:{maxAttempts:1},runtime:{retentionMs:DAY}});
  const done = await h.rt.create('emitted',request);
  const stuck = await h.rt.create('emitted',{...request,request_id:'stuck'});
  h.rt.ingest('source',done.watch_id,occurrence('a',1));
  h.rt.ingest('source',stuck.watch_id,occurrence('b',1));
  h.rt.cancel(done.watch_id);
  h.rt.cancel(stuck.watch_id);
  let claim;
  while ((claim = h.store.claimDelivery(h.now()))) {
    const accepted = claim.watch_id === done.watch_id;
    h.store.finishDelivery(claim,{accepted,code:accepted ? 'accepted' : 'http_rejected',now:h.now()});
  }
  assert.equal(h.rt.get(done.watch_id).delivery.accepted,2);
  h.advance(DAY - 1);
  assert.deepEqual(h.rt.prune(),{watches:0,events:0});
  h.advance(1);
  // The first watch is gone. The second keeps its undelivered events.
  assert.deepEqual(h.rt.prune(),{watches:1,events:2});
  assert.throws(() => h.rt.get(done.watch_id),/watch_not_found/);
  assert.equal(h.rt.get(stuck.watch_id).retained_events,2);
  // Its request identity is released, so the same request creates a fresh watch.
  const again = await h.rt.create('emitted',request);
  assert.notEqual(again.watch_id,done.watch_id);
});

test('redrive returns a dead letter to the queue and releases the events behind it', async t => {
  const h = await environment(t,{store:{maxAttempts:2,baseDelayMs:1,maxDelayMs:1}});
  const {watch_id} = await h.rt.create('emitted',request);
  h.rt.ingest('source',watch_id,occurrence('first',1));
  h.rt.ingest('source',watch_id,occurrence('second',2));
  exhaust(h);
  let status = h.rt.get(watch_id);
  assert.equal(status.delivery.dead,1);
  assert.equal(status.delivery.pending,1);
  const dead = status.dead_letter.event_id;
  assert.throws(() => h.rt.redrive(watch_id,{event_id:randomUUID()}),/not_dead_letter/);
  assert.throws(() => h.rt.redrive('missing-watch'),/watch_not_found/);
  const redriven = h.rt.redrive(watch_id,{event_id:dead});
  assert.equal(redriven.redriven,1);
  assert.equal(redriven.status.dead_letter,null);
  assert.equal(redriven.status.delivery_head.code,'operator_redriven');
  await h.reopen();
  const claim = h.store.claimDelivery(h.now());
  assert.equal(claim.event_id,dead);
  assert.equal(claim.attempt,1);
  h.store.finishDelivery(claim,{accepted:true,code:'accepted',now:h.now()});
  assert.equal(settle(h).sequence,2);
  status = h.rt.get(watch_id);
  assert.equal(status.delivery.accepted,2);
  assert.equal(h.rt.redrive(watch_id).redriven,0);
});

test('the runtime pulse prunes on its interval and retention can be disabled', async t => {
  const h = await environment(t,{runtime:{retentionMs:10,pruneIntervalMs:1000}});
  const {watch_id} = await h.rt.create('emitted',request);
  h.rt.ingest('source',watch_id,occurrence('a',1));
  settle(h);
  h.advance(10);
  await h.rt.tick();
  assert.equal(h.rt.get(watch_id).pruned_through,1);
  h.rt.ingest('source',watch_id,occurrence('b',2));
  settle(h);
  h.advance(10);
  await h.rt.tick();
  assert.equal(h.rt.get(watch_id).pruned_through,1, 'next pass waits for the prune interval');
  h.advance(1000);
  await h.rt.tick();
  assert.equal(h.rt.get(watch_id).pruned_through,2);

  const off = await environment(t,{runtime:{retentionMs:null}});
  const kept = await off.rt.create('emitted',request);
  off.rt.ingest('source',kept.watch_id,occurrence('a',1));
  settle(off);
  off.advance(10 * DAY);
  await off.rt.tick();
  assert.deepEqual(off.rt.prune(),{watches:0,events:0});
  assert.equal(off.rt.get(kept.watch_id).retained_events,1);
  assert.throws(() => new WatchRuntime({store:off.store,sources:new Map(),routes:new Map(),retentionMs:-1}),/invalid_request/);
});

test('a v4 store upgrades with settled history and finished watches starting their retention clock', async t => {
  const h = await environment(t,{runtime:{retentionMs:0}});
  const {watch_id} = await h.rt.create('emitted',request);
  h.rt.ingest('source',watch_id,occurrence('a',1));
  settle(h);
  h.rt.cancel(watch_id);
  settle(h);
  h.store.db.exec(`DROP INDEX delivery_settled; DROP INDEX watch_ended; ALTER TABLE outbox DROP COLUMN settled_ms;
    ALTER TABLE watches DROP COLUMN ended_ms; ALTER TABLE watches DROP COLUMN pruned_through; PRAGMA user_version=4;`);
  const before = Date.now();
  await h.reopen();
  assert.equal(h.store.one('PRAGMA user_version').user_version,5);
  const row = h.store.one('SELECT ended_ms FROM watches WHERE watch_id=?',watch_id);
  assert.ok(row.ended_ms >= before);
  assert.ok(h.store.one("SELECT settled_ms FROM outbox WHERE state='accepted'").settled_ms >= before);
  assert.equal(h.store.one("SELECT COUNT(*) AS n FROM outbox WHERE state!='accepted' AND settled_ms IS NOT NULL").n,0);
  // The clock starts at upgrade time, so the fixture's earlier clock keeps everything.
  assert.deepEqual(h.rt.prune(),{watches:0,events:0});
  assert.deepEqual(h.store.prune(Date.now() + 1,0),{watches:1,events:2});
});

function inboxWorkspace(t) {
  return mkdtemp(join(tmpdir(),'watch-inbox-retention-')).then(dir => {
    const inbox = new Inbox(join(dir,'inbox.sqlite'),{maxAttempts:1});
    t.after(async () => { inbox.close(); await rm(dir,{recursive:true,force:true}); });
    return inbox;
  });
}

const CONSUMER = 'worker';
const watchEvent = (watch_id,sequence) => sealEvent({version:1,event_id:randomUUID(),watch_id,sequence,
  kind:'emitted',observed_at:1000,evidence:{}});
const body = event => Buffer.from(JSON.stringify(event));

test('inbox pruning keeps each watch’s newest completed receipt and treats resends of pruned events as duplicates', async t => {
  const inbox = await inboxWorkspace(t);
  const watch = randomUUID();
  const events = [1,2,3].map(n => watchEvent(watch,n));
  for (const event of events) assert.equal(inbox.accept(CONSUMER,body(event),1000),'accepted');
  for (let n = 0; n < 3; n++) assert.equal((await inbox.workOnce(async () => {},{now:() => 2000})).state,'done');
  assert.equal(inbox.prune(2000 + DAY - 1,DAY),0, 'receipts inside the retention period stay');
  assert.equal(inbox.prune(2000 + DAY,DAY),2);
  assert.equal(inbox.status(CONSUMER,events[0].event_id),null);
  assert.equal(inbox.status(CONSUMER,events[2].event_id).state,'done');
  assert.equal(inbox.accept(CONSUMER,body(events[0]),3000),'duplicate');
  assert.equal(inbox.accept(CONSUMER,body(events[2]),3000),'duplicate');
  // The next event still finds its retained predecessor.
  const next = watchEvent(watch,4);
  assert.equal(inbox.accept(CONSUMER,body(next),3000),'accepted');
  assert.equal((await inbox.workOnce(async () => {},{now:() => 3000})).event_id,next.event_id);
});

test('an inbox dead job can be retried or skipped to release its successors', async t => {
  const inbox = await inboxWorkspace(t);
  const watch = randomUUID();
  const [first,second,third] = [1,2,3].map(n => watchEvent(watch,n));
  for (const event of [first,second,third]) inbox.accept(CONSUMER,body(event),1000);
  const fail = async () => { throw new Error('fixture failure'); };
  assert.equal((await inbox.workOnce(fail,{now:() => 1000})).state,'dead');
  assert.equal(await inbox.workOnce(async () => {},{now:() => 1000}),null, 'a dead job blocks its watch');
  assert.deepEqual(inbox.deadLetters(CONSUMER).map(row => row.sequence),[1]);
  assert.throws(() => inbox.redrive(CONSUMER,second.event_id),/not_dead_letter/);
  assert.equal(inbox.redrive(CONSUMER,first.event_id,{now:1500}).state,'queued');
  assert.equal((await inbox.workOnce(fail,{now:() => 1500})).state,'dead');
  const skipped = inbox.redrive(CONSUMER,first.event_id,{action:'skip',now:1600});
  assert.equal(skipped.state,'done');
  assert.equal(skipped.code,'operator_skipped');
  assert.equal(inbox.accept(CONSUMER,body(first),1700),'duplicate');
  const handled = [];
  for (let n = 0; n < 2; n++) await inbox.workOnce(async event => { handled.push(event.sequence); },{now:() => 1700});
  assert.deepEqual(handled,[2,3]);
  assert.throws(() => inbox.redrive(CONSUMER,first.event_id,{action:'drop'}),/invalid_request/);
});
