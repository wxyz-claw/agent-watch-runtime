import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { canonical, digest, diffEvidence, evaluatePredicate, isAuthoritativeObservation, sealEvent } from './sdk.mjs';

const APP_ID = 0x41575231;
const deliveryCodes = new Set(['accepted', 'route_unavailable', 'invalid_event', 'delivery_timeout',
  'transport_error', 'response_too_large', 'http_rejected']);
const schema = `
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
    attempt INTEGER NOT NULL DEFAULT 0, next_ms INTEGER NOT NULL, token TEXT, lease_ms INTEGER, code TEXT
  );
  CREATE INDEX delivery_due ON outbox(state,next_ms);
`;

function validBound(n, min, max) {
  return Number.isSafeInteger(n) && n >= min && n <= max;
}

export class WatchStore {
  constructor(path, {maxWatches = 512, maxEventsPerWatch = 1024, maxAttempts = 6,
    leaseMs = 10000, baseDelayMs = 250, maxDelayMs = 60000} = {}) {
    if (!validBound(maxWatches, 1, 10000) || !validBound(maxEventsPerWatch, 2, 10000) ||
        !validBound(maxAttempts, 1, 32) || !validBound(leaseMs, 1, 600000) ||
        !validBound(baseDelayMs, 1, 600000) || !validBound(maxDelayMs, baseDelayMs, 3600000)) {
      throw Error('invalid_request');
    }
    Object.assign(this, {maxWatches,maxEventsPerWatch,maxAttempts,leaseMs,baseDelayMs,maxDelayMs});
    this.db = new DatabaseSync(path);
    try {
      this.db.exec('PRAGMA busy_timeout=2000');
      this.transaction(() => {
        const {application_id} = this.one('PRAGMA application_id');
        const {user_version} = this.one('PRAGMA user_version');
        if (application_id === APP_ID && user_version === 1) return;
        if (application_id || user_version || this.one('SELECT name FROM sqlite_schema LIMIT 1')) throw Error('store_error');
        this.db.exec(schema);
        this.db.exec(`PRAGMA application_id=${APP_ID}`);
        this.db.exec('PRAGMA user_version=1');
      });
      this.db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON');
    } catch {
      this.db.close();
      throw Error('store_error');
    }
  }

  close() { this.db.close(); }
  one(sql, ...args) { return this.db.prepare(sql).get(...args); }
  run(sql, ...args) { return this.db.prepare(sql).run(...args); }

  transaction(fn) {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }

  create(definition, now, scheduled) {
    const encoded = canonical(definition);
    const {source_id,consumer_id,request_id,mode,deadline_ms,interval_ms} = definition;
    return this.transaction(() => {
      const existing = this.one('SELECT watch_id,definition FROM watches WHERE consumer_id=? AND request_id=?',consumer_id,request_id);
      if (existing) {
        if (existing.definition !== encoded) throw Error('request_conflict');
        return {watch_id:existing.watch_id, created:false};
      }
      if (this.one('SELECT COUNT(*) AS n FROM watches').n >= this.maxWatches) throw Error('capacity');
      const watch_id = randomUUID();
      this.run(`INSERT INTO watches(watch_id,consumer_id,request_id,source_id,mode,definition,deadline_ms,interval_ms,next_read_ms)
        VALUES (?,?,?,?,?,?,?,?,?)`,watch_id,consumer_id,request_id,source_id,mode,encoded,deadline_ms,interval_ms,scheduled ? now : null);
      return {watch_id, created:true};
    });
  }

  get(watchId) {
    const row = this.one(`SELECT watch_id,source_id,consumer_id,mode,state,sequence,truth,coverage,authority,code,
      next_read_ms,deadline_ms,json_extract(checkpoint,'$.reads') AS reads FROM watches WHERE watch_id=?`,watchId);
    if (!row) throw Error('watch_not_found');
    const delivery = {pending:0,running:0,accepted:0,dead:0};
    for (const item of this.db.prepare(`SELECT o.state,COUNT(*) AS n FROM outbox o JOIN events e USING(event_id)
      WHERE e.watch_id=? GROUP BY o.state`).all(watchId)) delivery[item.state] = item.n;
    const failure = this.one(`SELECT o.code,e.sequence FROM outbox o JOIN events e USING(event_id)
      WHERE e.watch_id=? AND o.state='dead' ORDER BY e.sequence LIMIT 1`,watchId);
    return {...row,delivery,dead_letter:failure ? {...failure} : null};
  }

  append(row, kind, evidence, now) {
    const sequence = row.sequence + 1;
    const event = sealEvent({version:1,event_id:randomUUID(),watch_id:row.watch_id,sequence,kind,observed_at:now,evidence});
    const body = JSON.stringify(event);
    this.run('INSERT INTO events(event_id,watch_id,sequence,body) VALUES (?,?,?,?)',event.event_id,row.watch_id,sequence,body);
    this.run('INSERT INTO outbox(event_id,next_ms) VALUES (?,?)',event.event_id,now);
    this.run('UPDATE watches SET sequence=? WHERE watch_id=?',sequence,row.watch_id);
  }

  terminal(row, kind, code, now) {
    this.append(row,kind,code ? {code} : {},now);
    this.run(`UPDATE watches SET state=?,code=?,next_read_ms=NULL,collect_token=NULL,collect_until_ms=NULL
      WHERE watch_id=?`,kind,code,row.watch_id);
  }

  cancel(watchId, now) {
    return this.transaction(() => {
      const row = this.one('SELECT * FROM watches WHERE watch_id=?',watchId);
      if (!row) throw Error('watch_not_found');
      if (row.state === 'active') {
        if (row.deadline_ms !== null && now >= row.deadline_ms) this.terminal(row,'deadline','deadline_reached',now);
        else this.terminal(row,'canceled','canceled',now);
      }
      return this.get(watchId);
    });
  }

  expire(now, watchId = null) {
    return this.transaction(() => {
      const rows = watchId === null
        ? this.db.prepare("SELECT * FROM watches WHERE state='active' AND deadline_ms<=? ORDER BY deadline_ms LIMIT 64").all(now)
        : this.db.prepare("SELECT * FROM watches WHERE state='active' AND deadline_ms<=? AND watch_id=?").all(now,watchId);
      for (const row of rows) this.terminal(row,'deadline','deadline_reached',now);
      return rows.length;
    });
  }

  claimObservation(now, {watchId = null, skipSources = [], force = false, leaseMs = this.leaseMs} = {}) {
    return this.transaction(() => {
      const terms = ["state='active'",'(deadline_ms IS NULL OR deadline_ms>?)','(collect_token IS NULL OR collect_until_ms<=?)'];
      const args = [now,now];
      if (!force) { terms.push('next_read_ms<=?'); args.push(now); }
      if (watchId !== null) { terms.push('watch_id=?'); args.push(watchId); }
      if (skipSources.length) {
        terms.push(`source_id NOT IN (${skipSources.map(() => '?').join(',')})`);
        args.push(...skipSources);
      }
      const row = this.one(`SELECT watch_id,source_id,mode,checkpoint FROM watches WHERE ${terms.join(' AND ')}
        ORDER BY next_read_ms, rowid LIMIT 1`,...args);
      if (!row) return null;
      const token = randomUUID();
      this.run('UPDATE watches SET collect_token=?,collect_until_ms=? WHERE watch_id=?',token,now+leaseMs,row.watch_id);
      return {...row,checkpoint:JSON.parse(row.checkpoint),token};
    });
  }

  apply(claim, obs, now, scheduled = true) {
    return this.transaction(() => {
      const row = this.one("SELECT * FROM watches WHERE watch_id=? AND collect_token=? AND state='active'",claim.watch_id,claim.token);
      if (!row) return false;
      if (row.deadline_ms !== null && now >= row.deadline_ms) {
        this.terminal(row,'deadline','deadline_reached',now);
        return false;
      }

      const checkpoint = JSON.parse(row.checkpoint);
      checkpoint.reads++;
      let {coverage,authority,code = null} = obs;
      let truth = 'UNKNOWN';
      let kind = null;
      let evidence = {};
      let seenKey;
      let payloadHash;
      if (isAuthoritativeObservation(obs)) {
        payloadHash = digest(Object.hasOwn(obs,'value') ? obs.value : null);
        let advance = true;
        evidence = {source_digest:payloadHash};
        if (obs.evidence) evidence.observation = obs.evidence;
        if (row.mode === 'until') {
          truth = evaluatePredicate(obs.value,JSON.parse(row.definition).predicate);
          if (truth === 'TRUE') kind = 'ready';
        } else if (row.mode === 'changes') {
          if (Object.hasOwn(checkpoint,'value')) {
            truth = digest(checkpoint.value) === payloadHash ? 'FALSE' : 'TRUE';
            if (truth === 'TRUE') {
              kind = 'changed';
              evidence.diff = diffEvidence(checkpoint.value,obs.value);
            }
          }
        } else {
          seenKey = digest(obs.source_event_id);
          const prior = this.one('SELECT payload_digest FROM emitted WHERE watch_id=? AND source_event_digest=?',row.watch_id,seenKey);
          if (prior) {
            advance = false;
            if (prior.payload_digest === payloadHash) truth = 'FALSE';
            else { coverage = 'failed'; code = 'source_invalid'; }
          } else {
            truth = 'TRUE';
            kind = 'emitted';
            evidence.source_event_digest = seenKey;
          }
        }

        // Reserve one event slot for a terminal outcome, including explicit capacity exhaustion.
        if (kind && kind !== 'ready' && row.sequence >= this.maxEventsPerWatch - 1) {
          this.terminal(row,'error','capacity',now);
          return false;
        }
        if (advance) {
          if (Object.hasOwn(obs,'cursor')) checkpoint.cursor = obs.cursor;
          if (row.mode !== 'emitted') checkpoint.value = obs.value;
        }
      } else if (!code) {
        code = coverage === 'failed' ? 'coverage_failed' : coverage === 'partial' ? 'coverage_partial' : 'advisory';
      }
      if (kind === 'emitted') this.run('INSERT INTO emitted VALUES (?,?,?)',row.watch_id,seenKey,payloadHash);
      if (kind) this.append(row,kind,evidence,now);
      const state = kind === 'ready' ? 'ready' : 'active';
      this.run(`UPDATE watches SET checkpoint=?,coverage=?,authority=?,truth=?,code=?,state=?,next_read_ms=?,
        collect_token=NULL,collect_until_ms=NULL WHERE watch_id=?`,
        JSON.stringify(checkpoint),coverage,authority,truth,code,state,
        state === 'active' && scheduled ? now+row.interval_ms : null,row.watch_id);
      return true;
    });
  }

  claimDelivery(now) {
    return this.transaction(() => {
      this.run(`UPDATE outbox SET state='dead',code='lease_expired',token=NULL,lease_ms=NULL
        WHERE state='running' AND lease_ms<=? AND attempt>=?`,now,this.maxAttempts);
      const row = this.one(`SELECT e.event_id,e.watch_id,e.sequence,e.body,w.consumer_id,o.attempt FROM outbox o
        JOIN events e USING(event_id) JOIN watches w USING(watch_id)
        WHERE ((o.state='pending' AND o.next_ms<=?) OR (o.state='running' AND o.lease_ms<=?))
        AND NOT EXISTS (SELECT 1 FROM events p JOIN outbox prev USING(event_id)
          WHERE p.watch_id=e.watch_id AND p.sequence<e.sequence AND prev.state!='accepted')
        ORDER BY o.next_ms,e.rowid LIMIT 1`,now,now);
      if (!row) return null;
      const token = randomUUID();
      this.run(`UPDATE outbox SET state='running',attempt=attempt+1,token=?,lease_ms=?
        WHERE event_id=?`,token,now+this.leaseMs,row.event_id);
      return {...row,attempt:row.attempt+1,token};
    });
  }

  finishDelivery(claim, {accepted,code,now}) {
    if (!deliveryCodes.has(code) || typeof accepted !== 'boolean' || !Number.isSafeInteger(now) ||
        (code === 'accepted') !== accepted) throw Error('invalid_request');
    const state = accepted ? 'accepted' : claim.attempt >= this.maxAttempts ? 'dead' : 'pending';
    const delay = Math.min(this.maxDelayMs,this.baseDelayMs * 2 ** (claim.attempt-1));
    return this.run(`UPDATE outbox SET state=?,next_ms=?,code=?,token=NULL,lease_ms=NULL
      WHERE event_id=? AND state='running' AND token=?`,state,now+delay,code,claim.event_id,claim.token).changes === 1;
  }
}
