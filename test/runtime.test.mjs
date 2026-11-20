import test from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import { WatchRuntime } from '../src/runtime.mjs';
import { WatchStore } from '../src/store.mjs';
import { createReceiver, deliverOnce, Inbox } from '../src/delivery.mjs';
import { syntheticSource } from '../src/sources.mjs';
import { validateEvent } from '../src/sdk.mjs';

const request = {source_id:'source',consumer_id:'worker',request_id:'request'};
const condition = {path:'/phase',equals:'ready'};
const until = {...request,predicate:condition};
const frame = (value) => ({value});
const event = (source_event_id,value) => ({
  version:1,contract:'source_emitted',coverage:'complete',authority:'authoritative',source_event_id,value,
});

async function environment(t, frames, options = {}) {
  const dir = await mkdtemp(join(tmpdir(),'watch-runtime-test-'));
  const database = join(dir,'runtime.sqlite');
  let time = 100000;
  const sources = new Map([['source',Array.isArray(frames) ? syntheticSource(frames) : frames]]);
  const routes = new Map([['worker',{}]]);
  let store, runtime;
  function open() {
    store = new WatchStore(database,options.store);
    runtime = new WatchRuntime({store,sources,routes,now:() => time,intervalMs:20,...options.runtime});
  }
  open();
  t.after(async () => { await runtime.stop(); store.close(); await rm(dir,{recursive:true,force:true}); });
  return {dir,sources,routes,get store(){return store;},get rt(){return runtime;},now:() => time,
    advance:n => { time += n; },reopen:async () => { await runtime.stop(); store.close(); open(); }};
}

function events(store,watchId) {
  return store.db.prepare('SELECT body FROM events WHERE watch_id=? ORDER BY sequence').all(watchId)
    .map(({body}) => validateEvent(JSON.parse(body)));
}

test('an initially ready watch commits exactly one immutable event across identical requests and restarts',async t => {
  const h = await environment(t,[frame({phase:'ready'})]);
  const [first,second] = await Promise.all([h.rt.create('until',until),h.rt.create('until',until)]);
  assert.equal(first.watch_id,second.watch_id);
  assert.equal(first.state,'ready');
  assert.equal(first.truth,'TRUE');
  assert.equal(first.delivery.pending,1);
  const original = events(h.store,first.watch_id);
  assert.deepEqual(original.map(e => [e.sequence,e.kind]),[[1,'ready']]);
  await h.reopen();
  assert.deepEqual(await h.rt.create('until',until),first);
  assert.deepEqual(events(h.store,first.watch_id),original);
  await assert.rejects(h.rt.create('until',{...until,predicate:{path:'/phase',equals:'waiting'}}),/request_conflict/);
  assert.equal(h.rt.cancel(first.watch_id).state,'ready');
});

test('complete false, missing, advisory, partial and failed reads have distinct truth and preserve the baseline',async t => {
  const h = await environment(t,[
    frame({phase:'waiting'}),frame({}),{value:{phase:'ready'},authority:'advisory'},
    {value:{phase:'ready'},coverage:'partial'},{coverage:'failed',code:'source_unavailable'},frame({phase:'ready'}),
  ]);
  let s = await h.rt.create('until',until);
  assert.equal(s.truth,'FALSE');
  for (const [truth,coverage,authority] of [['UNKNOWN','complete','authoritative'],['UNKNOWN','complete','advisory'],
    ['UNKNOWN','partial','authoritative'],['UNKNOWN','failed','authoritative']]) {
    h.advance(20); await h.rt.tick(); s = h.rt.get(s.watch_id);
    assert.deepEqual([s.truth,s.coverage,s.authority,s.sequence],[truth,coverage,authority,0]);
  }
  const checkpoint = JSON.parse(h.store.one('SELECT checkpoint FROM watches').checkpoint);
  assert.deepEqual(checkpoint,{reads:5,cursor:'1',value:{}});
  await h.reopen(); h.advance(20); await h.rt.tick();
  assert.equal(h.rt.get(s.watch_id).state,'ready');
});

test('gaps across a restart compare reliable snapshots and ignore equal object reorderings',async t => {
  const h = await environment(t,[frame({b:1,a:0}),frame({a:0,b:1}),{coverage:'partial',value:{a:5}},
    {coverage:'failed',code:'source_error'},frame({a:2,b:1})]);
  const {watch_id} = await h.rt.create('changes',request);
  for (let i=0; i<3; i++) { h.advance(20); await h.rt.tick(); }
  assert.equal(h.rt.get(watch_id).sequence,0);
  await h.reopen(); h.advance(20); await h.rt.tick();
  const [changed] = events(h.store,watch_id);
  assert.equal(changed.kind,'changed');
  assert.deepEqual(changed.evidence.diff.changes.map(item => item.op),['replace']);
  assert.equal(h.rt.get(watch_id).state,'active');
});

test('deadline and cancellation fence late completion without resetting the last truth',async t => {
  let release, began;
  let starting = new Promise(resolve => { began = resolve; });
  const h = await environment(t,async () => {
    began();
    await new Promise(resolve => { release = resolve; });
    return {version:1,contract:'durable_ready',coverage:'complete',authority:'authoritative',value:{phase:'ready'}};
  });
  const pending = h.rt.create('until',{...until,deadline_ms:h.now()+5});
  await starting;
  const row = h.store.one('SELECT watch_id FROM watches');
  h.advance(5);
  assert.equal(h.rt.get(row.watch_id).state,'deadline');
  release();
  assert.equal((await pending).truth,'UNKNOWN');
  assert.deepEqual(events(h.store,row.watch_id).map(e => e.kind),['deadline']);
  starting = new Promise(resolve => { began = resolve; });
  const other = h.rt.create('until',{...until,request_id:'cancel'});
  await starting;
  const second = h.store.one('SELECT watch_id FROM watches WHERE request_id=?','cancel');
  assert.equal(h.rt.cancel(second.watch_id).state,'canceled');
  release();
  await other;
  assert.deepEqual(events(h.store,second.watch_id).map(e => e.kind),['canceled']);
  const past = await h.rt.create('until',{...until,request_id:'late',deadline_ms:0});
  assert.equal(past.state,'deadline');
  assert.equal(past.reads,0);
});

test('a timed-out adapter cannot occupy healthy sources, delivery or an unbounded set of attempts',async t => {
  let calls = 0;
  const h = await environment(t,async () => { calls++; return new Promise(() => {}); },{runtime:{collectTimeoutMs:20}});
  h.sources.set('healthy',syntheticSource([frame({phase:'ready'})]));
  const slow = h.rt.create('until',until);
  const healthy = await h.rt.create('until',{...until,source_id:'healthy',request_id:'healthy'});
  assert.equal(healthy.state,'ready');
  await h.rt.tick();
  assert.equal(h.store.one('SELECT attempt FROM outbox').attempt,1);
  assert.equal((await slow).code,'source_timeout');
  h.advance(20); await h.rt.tick();
  assert.equal(h.rt.get((await slow).watch_id).truth,'UNKNOWN');
  assert.equal(calls,1);
});

test('discrete upstream occurrences deduplicate through restarts and reject identity/content conflicts',async t => {
  const h = await environment(t,null);
  const {watch_id} = await h.rt.create('emitted',request);
  assert.equal(h.rt.ingest('source',watch_id,event('first',{n:1})).sequence,1);
  await h.reopen();
  assert.equal(h.rt.ingest('source',watch_id,event('first',{n:1})).sequence,1);
  const conflict = h.rt.ingest('source',watch_id,event('first',{n:2}));
  assert.deepEqual([conflict.sequence,conflict.truth,conflict.code],[1,'UNKNOWN','source_invalid']);
  assert.equal(h.rt.ingest('source',watch_id,event('second',{n:2})).sequence,2);
  assert.deepEqual(events(h.store,watch_id).map(e => [e.sequence,e.kind]),[[1,'emitted'],[2,'emitted']]);
});

test('failed outbox insert rolls back the checkpoint, event and emitted identity together',async t => {
  const h = await environment(t,null,{store:{leaseMs:10}});
  const {watch_id} = await h.rt.create('emitted',request);
  h.store.db.exec("CREATE TEMP TRIGGER fixture_abort BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'fixture_rollback'); END");
  assert.throws(() => h.rt.ingest('source',watch_id,event('first',1)),/fixture_rollback/);
  for (const table of ['emitted','events','outbox']) assert.equal(h.store.one('SELECT count(*) AS n FROM '+table).n,0);
  assert.equal(h.rt.get(watch_id).reads,0);
  await h.reopen(); h.advance(10);
  assert.equal(h.rt.ingest('source',watch_id,event('first',1)).sequence,1);
});

test('sender retries the same event after a lost 2xx, process restart and callback/key change',async t => {
  const h = await environment(t,[frame({phase:'ready'})],{store:{baseDelayMs:20,maxDelayMs:40}});
  const path = join(h.dir,'inbox.sqlite');
  let inbox = new Inbox(path);
  let server;
  const close = async () => {
    if (server?.listening) await new Promise((resolve,reject) => server.close(err => err ? reject(err) : resolve()));
    inbox.close();
  };
  t.after(close);
  async function listen(keyId,drop) {
    const key = randomBytes(32);
    server = createReceiver({inbox,consumerId:'worker',keys:new Map([[keyId,key]]),now:h.now});
    if (drop) server.prependListener('request', (_req,res) => { res.end = function () { this.destroy(); return this; }; });
    server.listen(0,'127.0.0.1'); await once(server,'listening');
    h.routes.set('worker',{url:'http://127.0.0.1:'+server.address().port+'/inbox',keyId,key});
  }
  await listen('first',true);
  const {watch_id} = await h.rt.create('until',until);
  const [original] = events(h.store,watch_id);
  const failed = await deliverOnce(h.store,h.routes,{now:h.now});
  assert.equal(failed.accepted,false);
  assert.equal(inbox.status('worker',original.event_id).state,'queued');
  assert.equal(h.rt.get(watch_id).delivery.pending,1);
  await close(); await h.reopen();
  inbox = new Inbox(path); await listen('second',false);
  assert.equal(await deliverOnce(h.store,h.routes,{now:h.now}),null);
  h.advance(20);
  const result = await deliverOnce(h.store,h.routes,{now:h.now});
  assert.equal(result.accepted,true);
  assert.equal(result.event_id,original.event_id);
  assert.deepEqual(events(h.store,watch_id),[original]);
  assert.equal(h.rt.get(watch_id).delivery.accepted,1);
  assert.equal(inbox.status('worker',original.event_id).state,'queued');
  let effects = 0;
  await inbox.workOnce(async item => { assert.deepEqual(item,original); effects++; },{now:h.now});
  assert.equal(await inbox.workOnce(async () => { effects++; },{now:h.now}),null);
  assert.equal(effects,1);
});

test('persistent retry limits leave a visible dead letter and fence stale acknowledgements',async t => {
  const h = await environment(t,[frame({phase:'ready'})],{store:{baseDelayMs:5,maxDelayMs:10,maxAttempts:2,leaseMs:20}});
  const {watch_id} = await h.rt.create('until',until);
  const first = h.store.claimDelivery(h.now());
  await h.reopen(); h.advance(19);
  assert.equal(h.store.claimDelivery(h.now()),null);
  h.advance(1);
  const second = h.store.claimDelivery(h.now());
  assert.equal(second.event_id,first.event_id);
  assert.equal(second.body,first.body);
  assert.notEqual(second.token,first.token);
  assert.equal(h.store.finishDelivery(first,{accepted:true,code:'accepted',now:h.now()}),false);
  assert.equal(h.store.finishDelivery(second,{accepted:false,code:'route_unavailable',now:h.now()}),true);
  assert.deepEqual(h.rt.get(watch_id).dead_letter,
    {code:'route_unavailable',event_id:first.event_id,sequence:1,attempt:2,max_attempts:2});
  assert.equal(h.store.claimDelivery(h.now()+1000),null);
});

test('storage bounds reserve a terminal event and keep separate watches independent',async t => {
  const h = await environment(t,null,{store:{maxEventsPerWatch:2,maxWatches:2}});
  const {watch_id} = await h.rt.create('emitted',request);
  h.rt.ingest('source',watch_id,event('first',1));
  const final = h.rt.ingest('source',watch_id,event('second',2));
  assert.deepEqual([final.state,final.code,final.sequence],['error','capacity',2]);
  assert.deepEqual(events(h.store,watch_id).map(e => e.kind),['emitted','error']);
  const other = await h.rt.create('emitted',{...request,request_id:'other'});
  assert.equal(h.rt.ingest('source',other.watch_id,event('first',1)).state,'active');
  await assert.rejects(h.rt.create('emitted',{...request,request_id:'third'}),/capacity/);
});

test('adapter errors and unrecognized observation shapes expose bounded diagnostics',async t => {
  let round = 0;
  const marker = 'generated-private-fixture-value';
  const h = await environment(t,async () => {
    if (!round++) throw Error(marker);
    return {version:1,contract:'durable_ready',coverage:'other',authority:'authoritative',code:marker};
  });
  const s = await h.rt.create('until',until); assert.equal(s.code,'source_error');
  h.advance(20); await h.rt.tick();
  const failed = h.rt.get(s.watch_id);
  assert.equal(failed.code,'invalid_observation');
  assert.equal(JSON.stringify(failed).includes(marker),false);
  assert.equal(failed.sequence,0);
});

test('watch store declines an unrelated SQLite file',async t => {
  const h = await environment(t,null);
  const path = join(h.dir,'unrelated.sqlite');
  const db = new DatabaseSync(path);
  db.exec('CREATE TABLE fixture(value INTEGER)');
  assert.throws(() => new WatchStore(path),/store_error/);
  assert.deepEqual(db.prepare("SELECT name FROM sqlite_schema WHERE type='table'").all().map(r => r.name),['fixture']);
  db.close();
});

test('matching upstream replay can advance its continuation without appending an event',async t => {
  const h = await environment(t,async ({checkpoint}) => {
    if (checkpoint.cursor === null) return {...event('a',1),cursor:'first-position'};
    if (checkpoint.cursor === 'first-position') return {...event('a',1),cursor:'second-position'};
    return {...event('b',2),cursor:'third-position'};
  });
  const {watch_id} = await h.rt.create('emitted',request);
  h.advance(20); await h.rt.tick();
  assert.equal(h.rt.get(watch_id).sequence,1);
  await h.reopen(); h.advance(20); await h.rt.tick();
  assert.deepEqual(events(h.store,watch_id).map(item => item.sequence),[1,2]);
});

test('read-only history resumes from an exclusive per-watch sequence with no delivery side effects',async t => {
  const h = await environment(t,null);
  const {watch_id} = await h.rt.create('emitted',request);
  const other = await h.rt.create('emitted',{...request,request_id:'other'});
  h.rt.ingest('source',other.watch_id,event('different',0));
  for (let n=1; n<=5; n++) h.rt.ingest('source',watch_id,event('occurrence-'+n,n));
  const status = h.rt.get(watch_id);
  const first = h.rt.history(watch_id,{limit:2});
  assert.deepEqual(first.events.map(e => validateEvent(e).sequence),[1,2]);
  assert.equal(first.has_more,true);
  await h.reopen();
  assert.deepEqual(h.rt.history(watch_id,{limit:2}),first);
  const rest = h.rt.history(watch_id,{after_sequence:first.next_sequence});
  assert.deepEqual(rest.events.map(e => e.sequence),[3,4,5]);
  assert.equal(rest.has_more,false);
  assert.deepEqual(h.rt.history(watch_id,{after_sequence:rest.next_sequence}),{events:[],has_more:false,next_sequence:5});
  assert.deepEqual(h.rt.get(watch_id),status);
  assert.throws(() => h.rt.history(watch_id,{limit:51}),/invalid_request/);
});

test('history pages respect the encoded size bound even for dense structural diffs',async t => {
  const many = n => Object.fromEntries(Array.from({length:32},(_,i) => ['field'+i,n]));
  const h = await environment(t,async ({checkpoint}) => ({
    version:1,contract:'observe_change',coverage:'complete',authority:'authoritative',value:many(checkpoint.reads),
  }));
  const {watch_id} = await h.rt.create('changes',request);
  for (let i=0;i<32;i++) { h.advance(20); await h.rt.tick(); }
  const page = h.rt.history(watch_id,{limit:50});
  assert.equal(page.has_more,true);
  assert.ok(page.events.length>0 && page.events.length<32);
  assert.ok(Buffer.byteLength(JSON.stringify(page))<65536);
  assert.equal(h.rt.history(watch_id,{after_sequence:page.next_sequence}).events[0].sequence,page.next_sequence+1);
});

test('a delivery retains its first sender’s retry policy when process defaults change',async t => {
  const options = {store:{maxAttempts:2,baseDelayMs:19,maxDelayMs:40}};
  const h = await environment(t,[frame({phase:'ready'})],options);
  const {watch_id} = await h.rt.create('until',until);
  await deliverOnce(h.store,h.routes,{now:h.now});
  options.store = {maxAttempts:8,baseDelayMs:1,maxDelayMs:1000};
  await h.reopen();
  assert.equal(await deliverOnce(h.store,h.routes,{now:h.now}),null);
  const status = h.rt.get(watch_id);
  assert.equal(status.delivery_head.max_attempts,2);
  assert.equal(status.delivery_head.next_ms,h.now()+19);
  h.advance(19); await deliverOnce(h.store,h.routes,{now:h.now});
  assert.equal(h.rt.get(watch_id).dead_letter.attempt,2);
});

test('v1 store upgrades preserve existing immutable bytes and seed durable retry policy',async t => {
  const h = await environment(t,[frame({phase:'ready'})]);
  const {watch_id} = await h.rt.create('until',until);
  const original = events(h.store,watch_id);
  h.store.db.exec(`ALTER TABLE outbox DROP COLUMN max_attempts; ALTER TABLE outbox DROP COLUMN retry_base_ms;
    ALTER TABLE outbox DROP COLUMN retry_cap_ms; ALTER TABLE outbox DROP COLUMN retry_jitter_ms;
    DROP TABLE manual_checks; DROP TABLE watch_configs; DROP TABLE delivery_routes; PRAGMA user_version=1;`);
  await h.reopen();
  assert.equal(h.store.one('PRAGMA user_version').user_version,4);
  assert.deepEqual(events(h.store,watch_id),original);
  assert.equal(h.rt.get(watch_id).delivery_head.max_attempts,6);
});

test('one slow consumer leaves a delivery slot for another consumer',async t => {
  const h = await environment(t,[frame({phase:'ready'})],{runtime:{parallelism:2,deliveryTimeoutMs:1000}});
  let received;
  const arrival = new Promise(resolve => { received = resolve; });
  const slow = createServer((req,res) => { req.resume(); received(res); });
  slow.listen(0,'127.0.0.1'); await once(slow,'listening');
  t.after(() => { slow.closeAllConnections(); return new Promise(resolve => slow.close(resolve)); });
  h.routes.set('worker',{url:'http://127.0.0.1:'+slow.address().port+'/inbox',keyId:'fixture',key:randomBytes(32)});
  h.routes.set('other',{});
  await h.rt.create('until',until);
  const waiting = await h.rt.create('until',{...until,request_id:'next'});
  const independent = await h.rt.create('until',{...until,consumer_id:'other'});
  const pulse = h.rt.tick();
  const response = await arrival;
  try {
    assert.equal(h.rt.get(waiting.watch_id).delivery_head.attempt,0);
    assert.equal(h.rt.get(independent.watch_id).delivery_head.code,'route_unavailable');
  } finally { response.writeHead(503).end(); await pulse; }
});

async function peer(t, handler) {
  const server = createServer(handler);
  server.listen(0,'127.0.0.1'); await once(server,'listening');
  t.after(() => {
    server.closeAllConnections();
    return new Promise(resolve => server.close(resolve));
  });
  return {url:'http://127.0.0.1:'+server.address().port+'/inbox',keyId:'fixture',key:randomBytes(32)};
}

test('429 Retry-After sets a durable bounded minimum delay from the sender clock',async t => {
  const h = await environment(t,[frame({phase:'ready'})],{store:{baseDelayMs:5,maxDelayMs:2500}});
  let requests = 0;
  const route = await peer(t,(req,res) => {
    req.resume(); requests++;
    res.writeHead(429,{'Retry-After':requests === 1 ? '2' : '60'}).end();
  });
  h.routes.set('worker',route);
  const {watch_id} = await h.rt.create('until',until);
  assert.equal((await deliverOnce(h.store,h.routes,{now:h.now})).code,'rate_limited');
  assert.equal(h.rt.get(watch_id).delivery_head.next_ms,h.now()+2000);
  await h.reopen(); h.advance(1999);
  assert.equal(await deliverOnce(h.store,h.routes,{now:h.now}),null);
  h.advance(1); await deliverOnce(h.store,h.routes,{now:h.now});
  assert.equal(requests,2);
  assert.equal(h.rt.get(watch_id).delivery_head.next_ms,h.now()+2500);
});

test('a current 410 pauses the logical route across restart until operator repair',async t => {
  const h = await environment(t,[frame({phase:'ready'})],{store:{maxAttempts:1}});
  let requests = 0;
  const bodies = [];
  const route = await peer(t,async (req,res) => {
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    bodies.push(Buffer.concat(chunks));
    res.writeHead(++requests === 1 ? 410 : 503).end();
  });
  h.routes.set('worker',route);
  const first = await h.rt.create('until',until);
  await deliverOnce(h.store,h.routes,{now:h.now});
  assert.equal(h.rt.get(first.watch_id).delivery_paused,true);
  assert.equal(h.rt.get(first.watch_id).delivery_head.state,'paused');
  const later = await h.rt.create('until',{...until,request_id:'same-consumer'});
  assert.equal(later.delivery.paused,1);
  await h.reopen(); h.advance(100000);
  assert.equal(await deliverOnce(h.store,h.routes,{now:h.now}),null);
  assert.equal(requests,1);
  assert.equal(h.rt.resumeDelivery('worker'),true);
  assert.equal(h.rt.resumeDelivery('worker'),false);
  await deliverOnce(h.store,h.routes,{now:h.now});
  assert.deepEqual(bodies[1],bodies[0]);
  assert.equal(h.rt.get(first.watch_id).dead_letter.code,'http_rejected');
});

test('a rotated route fences a late 410 even on the final attempt and delivers the original bytes',async t => {
  const h = await environment(t,[frame({phase:'ready'})],{store:{maxAttempts:1}});
  let arrived;
  const waiting = new Promise(resolve => { arrived = resolve; });
  const old = await peer(t,async (req,res) => {
    const chunks=[]; for await (const chunk of req) chunks.push(chunk);
    arrived({res,bytes:Buffer.concat(chunks)});
  });
  const inbox = new Inbox(join(h.dir,'accepted.sqlite')); t.after(() => inbox.close());
  const key = randomBytes(32);
  const current = createReceiver({inbox,consumerId:'worker',keys:new Map([['next',key]]),now:h.now});
  current.listen(0,'127.0.0.1'); await once(current,'listening');
  t.after(() => new Promise(resolve => current.close(resolve)));
  h.routes.set('worker',old);
  const {watch_id} = await h.rt.create('until',until);
  const late = deliverOnce(h.store,h.routes,{now:h.now});
  const retiring = await waiting;
  h.routes.set('worker',{url:'http://127.0.0.1:'+current.address().port+'/inbox',keyId:'next',key});
  retiring.res.writeHead(410).end();
  assert.equal((await late).code,'route_changed');
  assert.equal(h.rt.get(watch_id).delivery_paused,false);
  const pending = h.rt.get(watch_id).delivery_head;
  assert.deepEqual([pending.state,pending.attempt],['pending',0]);
  await h.reopen();
  const retry = await deliverOnce(h.store,h.routes,{now:h.now});
  assert.equal(retry.accepted,true);
  const original = JSON.parse(retiring.bytes);
  assert.equal(retry.event_id,original.event_id);
  assert.equal(inbox.status('worker',retry.event_id).digest,original.digest);
  assert.equal(h.rt.get(watch_id).sequence,1);
});

test('a new destination repairs a persisted 410 pause and a stale token cannot pause it again',async t => {
  const h = await environment(t,[frame({phase:'ready'})],{store:{maxAttempts:1}});
  const {watch_id} = await h.rt.create('until',until);
  const old = await peer(t,(req,res) => { req.resume(); res.writeHead(410).end(); });
  h.routes.set('worker',old);
  await deliverOnce(h.store,h.routes,{now:h.now});
  assert.equal(h.rt.get(watch_id).delivery_head.state,'paused');
  await h.reopen();
  const replacement = await peer(t,(req,res) => { req.resume(); res.writeHead(429,{'Retry-After':'1'}).end(); });
  h.routes.set('worker',replacement);
  const attempt = await deliverOnce(h.store,h.routes,{now:h.now});
  assert.equal(attempt.code,'rate_limited');
  assert.equal(h.rt.get(watch_id).delivery_paused,false);
  assert.equal(h.store.finishDelivery({event_id:attempt.event_id,token:'retired'},
    {accepted:false,code:'endpoint_gone',now:h.now()}),false);
  assert.equal(h.rt.get(watch_id).delivery_paused,false);
});
