import { createHmac, randomUUID, timingSafeEqual } from 'node:crypto';
import { createServer, request } from 'node:http';
import { DatabaseSync } from 'node:sqlite';
import { MAX_EVENT_BYTES, validateEvent } from './sdk.mjs';

const APP_ID = 0x41574942;
const MAX_REPLY_BYTES = 4096;
const MAX_SKEW_MS = 300_000;
const MAX_RETRY_MS = 60_000;
const ID = /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$/;
const ROUTE = /^http:\/\/127\.0\.0\.1:([1-9]\d{0,4})\/inbox$/;
const utf8 = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

function integer(value, min, max) {
  return Number.isSafeInteger(value) && value >= min && value <= max;
}

function instant(clock) {
  const now = typeof clock === 'function' ? clock() : clock;
  if (!integer(now, 0, Number.MAX_SAFE_INTEGER)) throw new Error('invalid_clock');
  return now;
}

function safeId(id) {
  return typeof id === 'string' && ID.test(id);
}

function validKey(key) {
  return Buffer.isBuffer(key) && key.length >= 32 && key.length <= 256;
}

function mac(key, consumer, keyId, timestamp, body) {
  const prefix = `agent-watch-webhook-v1\n${consumer}\n${keyId}\n${timestamp}\n`;
  return createHmac('sha256', key).update(prefix).update(body).digest();
}

function parseEvent(body) {
  if (body.length === 0 || body.length > MAX_EVENT_BYTES) throw new Error('invalid_event');
  return validateEvent(JSON.parse(utf8.decode(body)));
}

function post(port, body, headers, timeoutMs) {
  return new Promise(resolve => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    let completed = false;
    function finish(code) {
      if (completed) return;
      completed = true;
      clearTimeout(timeout);
      resolve(code);
    }
    const req = request({
      hostname: '127.0.0.1', port, path: '/inbox', method: 'POST', agent: false,
      headers, signal: controller.signal, maxHeaderSize: 4096,
    }, res => {
      let size = 0;
      res.on('data', chunk => {
        size += chunk.length;
        if (size > MAX_REPLY_BYTES) {
          finish('response_too_large');
          res.destroy();
        }
      });
      const interrupted = () => finish(controller.signal.aborted ? 'delivery_timeout' : 'transport_error');
      res.on('aborted', interrupted);
      res.on('error', interrupted);
      res.on('end', () => {
        const success = res.statusCode >= 200 && res.statusCode < 300;
        finish(success ? 'accepted' : 'http_rejected');
      });
    });
    req.on('error', () => finish(controller.signal.aborted ? 'delivery_timeout' : 'transport_error'));
    req.end(body);
  });
}

/** Claim one immutable envelope, resolving the current route and key on each attempt. */
export async function deliverOnce(store, routes, { now = Date.now, timeoutMs = 3000 } = {}) {
  if (!integer(timeoutMs, 1, 30_000)) throw new Error('invalid_timeout');
  const started = instant(now);
  const claim = store.claimDelivery(started);
  if (!claim) return null;

  const route = routes.get(claim.consumer_id);
  const match = typeof route?.url === 'string' && ROUTE.exec(route.url);
  let code = 'route_unavailable';
  if (match && Number(match[1]) <= 65535 && safeId(claim.consumer_id) &&
      safeId(route.keyId) && validKey(route.key)) {
    let body;
    try {
      if (typeof claim.body !== 'string' && !(claim.body instanceof Uint8Array)) {
        throw new Error('invalid_event');
      }
      if (Buffer.byteLength(claim.body) > MAX_EVENT_BYTES) throw new Error('invalid_event');
      const bytes = Buffer.from(claim.body);
      const event = parseEvent(bytes);
      if (event.event_id !== claim.event_id || event.watch_id !== claim.watch_id ||
          event.sequence !== claim.sequence) throw new Error('invalid_event');
      body = bytes;
    } catch {
      code = 'invalid_event';
    }
    if (body) {
      const timestamp = String(started);
      code = await post(Number(match[1]), body, {
        'content-type': 'application/json',
        'content-length': body.length,
        'x-watch-consumer': claim.consumer_id,
        'x-watch-key-id': route.keyId,
        'x-watch-timestamp': timestamp,
        'x-watch-signature': mac(route.key, claim.consumer_id, route.keyId, timestamp, body).toString('hex'),
      }, timeoutMs);
    }
  }
  const accepted = code === 'accepted';
  const recorded = store.finishDelivery(claim, { accepted, code, now: instant(now) }) !== false;
  return { event_id: claim.event_id, consumer_id: claim.consumer_id, accepted, recorded, code };
}

const PREDECESSOR_DONE = `(
  i.sequence = 1 OR EXISTS (
    SELECT 1 FROM inbox prev JOIN jobs p USING(consumer_id,event_id)
    WHERE prev.consumer_id=i.consumer_id AND prev.watch_id=i.watch_id
      AND prev.sequence=i.sequence-1 AND p.state='done'
  )
)`;

const SCHEMA = `
  CREATE TABLE inbox (
    consumer_id TEXT NOT NULL, event_id TEXT NOT NULL, watch_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence > 0), digest TEXT NOT NULL,
    body BLOB NOT NULL, accepted_at INTEGER NOT NULL,
    PRIMARY KEY (consumer_id,event_id), UNIQUE (consumer_id,watch_id,sequence)
  ) STRICT;
  CREATE TABLE jobs (
    consumer_id TEXT NOT NULL, event_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('queued','running','done','dead')),
    attempts INTEGER NOT NULL, max_attempts INTEGER NOT NULL, lease_ms INTEGER NOT NULL,
    retry_base_ms INTEGER NOT NULL, available_at INTEGER NOT NULL,
    lease_until INTEGER, token TEXT, code TEXT NOT NULL,
    PRIMARY KEY (consumer_id,event_id),
    FOREIGN KEY (consumer_id,event_id) REFERENCES inbox(consumer_id,event_id)
  ) STRICT;
  CREATE INDEX jobs_due ON jobs(state,available_at);
  CREATE INDEX jobs_leases ON jobs(state,lease_until);
  PRAGMA application_id=${APP_ID};
  PRAGMA user_version=1;
`;

function atomic(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = fn();
    db.exec('COMMIT');
    return result;
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

function initialize(db) {
  db.exec('PRAGMA busy_timeout=2000; PRAGMA foreign_keys=ON;');
  atomic(db, () => {
    const id = db.prepare('PRAGMA application_id').get().application_id;
    const version = db.prepare('PRAGMA user_version').get().user_version;
    if (id === APP_ID && version === 1) return;
    const existing = db.prepare("SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' LIMIT 1").get();
    if (id || version || existing) throw new Error('incompatible_inbox_database');
    db.exec(SCHEMA);
  });
  db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
}

/** A committed inbox receipt and a durable, independently retryable job. */
export class Inbox {
  #db;
  #policy;
  constructor(path, { maxAttempts = 5, leaseMs = 30_000, retryBaseMs = 250, maxRows = 10_000 } = {}) {
    if (!integer(maxAttempts, 1, 32) || !integer(leaseMs, 1, 600_000) ||
        !integer(retryBaseMs, 1, MAX_RETRY_MS) || !integer(maxRows, 1, 1_000_000)) {
      throw new Error('invalid_inbox_limits');
    }
    this.#policy = { maxAttempts, leaseMs, retryBaseMs, maxRows };
    this.#db = new DatabaseSync(path);
    try {
      initialize(this.#db);
    } catch (error) {
      this.#db.close();
      throw error;
    }
  }

  close() { this.#db.close(); }

  /** Called after the HTTP signature is verified. Bind the receipt to the exact validated bytes. */
  accept(consumerId, body, now) {
    if (!safeId(consumerId) || !Buffer.isBuffer(body) || !integer(now, 0, Number.MAX_SAFE_INTEGER)) {
      return 'invalid';
    }
    let event;
    try { event = parseEvent(body); } catch { return 'invalid'; }
    const { event_id, watch_id, sequence, digest } = event;
    const { maxAttempts, leaseMs, retryBaseMs, maxRows } = this.#policy;
    return atomic(this.#db, () => {
      const receipt = this.#db.prepare(
        'SELECT digest FROM inbox WHERE consumer_id=? AND event_id=?'
      ).get(consumerId, event_id);
      if (receipt) return receipt.digest === digest ? 'duplicate' : 'conflict';
      const occupied = this.#db.prepare(
        'SELECT 1 FROM inbox WHERE consumer_id=? AND watch_id=? AND sequence=?'
      ).get(consumerId, watch_id, sequence);
      if (occupied) return 'conflict';
      if (this.#db.prepare('SELECT count(*) AS n FROM inbox').get().n >= maxRows) return 'full';
      this.#db.prepare('INSERT INTO inbox VALUES(?,?,?,?,?,?,?)')
        .run(consumerId, event_id, watch_id, sequence, digest, body, now);
      this.#db.prepare(`INSERT INTO jobs
        (consumer_id,event_id,state,attempts,max_attempts,lease_ms,retry_base_ms,available_at,code)
        VALUES(?,?,'queued',0,?,?,?,?,'accepted')`)
        .run(consumerId, event_id, maxAttempts, leaseMs, retryBaseMs, now);
      return 'accepted';
    });
  }

  status(consumerId, eventId) {
    const row = this.#db.prepare(`
      SELECT i.consumer_id,i.event_id,i.watch_id,i.sequence,i.digest,i.accepted_at,
        j.state,j.attempts,j.available_at,j.lease_until,j.code,
        CASE WHEN j.state='queued' AND NOT ${PREDECESSOR_DONE} THEN 'predecessor' END AS blocked_by
      FROM inbox i JOIN jobs j USING(consumer_id,event_id)
      WHERE i.consumer_id=? AND i.event_id=?
    `).get(consumerId, eventId);
    return row ? { ...row } : null;
  }

  summary(consumerId) {
    const rows = this.#db.prepare(`
      SELECT j.state, count(*) AS n,
        sum(CASE WHEN j.state='queued' AND NOT ${PREDECESSOR_DONE} THEN 1 ELSE 0 END) AS blocked
      FROM inbox i JOIN jobs j USING(consumer_id,event_id)
      WHERE i.consumer_id=? GROUP BY j.state
    `).all(consumerId);
    const counts = { queued: 0, running: 0, done: 0, dead: 0, blocked: 0 };
    for (const row of rows) {
      counts[row.state] = row.n;
      counts.blocked += row.blocked;
    }
    return counts;
  }

  #claim(now, consumerId) {
    return atomic(this.#db, () => {
      this.#db.prepare(`UPDATE jobs SET
        state=CASE WHEN attempts>=max_attempts THEN 'dead' ELSE 'queued' END,
        available_at=lease_until+min(?, retry_base_ms*(1 << (attempts-1))),
        token=NULL, lease_until=NULL, code='lease_expired'
        WHERE state='running' AND lease_until<=?`).run(MAX_RETRY_MS, now);
      const row = this.#db.prepare(`
        SELECT i.consumer_id,i.event_id,i.body,j.attempts,j.lease_ms,j.max_attempts,j.retry_base_ms
        FROM jobs j JOIN inbox i USING(consumer_id,event_id)
        WHERE j.state='queued' AND j.available_at<=? AND (? IS NULL OR i.consumer_id=?)
          AND ${PREDECESSOR_DONE}
        ORDER BY j.available_at,i.accepted_at,i.event_id LIMIT 1
      `).get(now, consumerId, consumerId);
      if (!row) return null;
      const token = randomUUID();
      this.#db.prepare(`UPDATE jobs SET state='running',attempts=attempts+1,
        lease_until=?,token=?,code='working' WHERE consumer_id=? AND event_id=?`)
        .run(now + row.lease_ms, token, row.consumer_id, row.event_id);
      return { ...row, attempts: row.attempts + 1, token };
    });
  }

  /** Workers may run in another process. Cooperative handlers receive an abort signal. */
  async workOnce(handler, { now = Date.now, consumerId = null } = {}) {
    if (typeof handler !== 'function' || (consumerId !== null && !safeId(consumerId))) {
      throw new Error('invalid_worker');
    }
    const row = this.#claim(instant(now), consumerId);
    if (!row) return null;
    const abort = new AbortController();
    let timer;
    let code = 'completed';
    try {
      const event = parseEvent(row.body);
      const deadline = new Promise((_, reject) => {
        timer = setTimeout(() => { reject(new Error('handler_timeout')); abort.abort(); }, row.lease_ms);
      });
      await Promise.race([
        Promise.resolve().then(() => handler(event, {
          consumerId: row.consumer_id, attempt: row.attempts, signal: abort.signal,
        })),
        deadline,
      ]);
    } catch {
      code = abort.signal.aborted ? 'handler_timeout' : 'handler_failed';
    } finally {
      clearTimeout(timer);
    }
    let state = 'done';
    if (code !== 'completed') state = row.attempts >= row.max_attempts ? 'dead' : 'queued';
    const finished = instant(now);
    const delay = Math.min(MAX_RETRY_MS, row.retry_base_ms * 2 ** (row.attempts - 1));
    const result = this.#db.prepare(`UPDATE jobs SET state=?,code=?,available_at=?,token=NULL,lease_until=NULL
      WHERE consumer_id=? AND event_id=? AND state='running' AND token=?`)
      .run(state, code, state === 'queued' ? finished + delay : finished, row.consumer_id, row.event_id, row.token);
    if (!result.changes) return { consumer_id: row.consumer_id, event_id: row.event_id, state: 'superseded' };
    return this.status(row.consumer_id, row.event_id);
  }
}

function reply(res, status) {
  res.writeHead(status, { 'content-length': 0, 'connection': 'close' });
  res.end();
}

function header(req, name) {
  const values = req.headersDistinct[name];
  return values?.length === 1 ? values[0] : undefined;
}

function loopback(address) {
  return address === '127.0.0.1' || address === '::ffff:127.0.0.1';
}

/** Bind the returned server with listen(port, '127.0.0.1'). Each response has an empty body. */
export function createReceiver({ inbox, consumerId, keys, now = Date.now, maxSkewMs = MAX_SKEW_MS }) {
  if (!safeId(consumerId) || !(keys instanceof Map) || !integer(maxSkewMs, 0, MAX_SKEW_MS)) {
    throw new Error('invalid_receiver');
  }
  const server = createServer({
    maxHeaderSize: 4096, requestTimeout: 5000, headersTimeout: 5000,
    connectionsCheckingInterval: 500, keepAliveTimeout: 1000,
  }, (req, res) => {
    if (!loopback(req.socket.remoteAddress) || !loopback(req.socket.localAddress)) return reply(res, 403);
    if (req.url !== '/inbox' || req.method !== 'POST') return reply(res, 404);
    if (!/^application\/json(?:\s*;\s*charset=utf-8)?$/i.test(header(req, 'content-type') ?? '') ||
        req.headers['content-encoding'] !== undefined) return reply(res, 415);
    if (Number(req.headers['content-length']) > MAX_EVENT_BYTES) return reply(res, 413);
    const keyId = header(req, 'x-watch-key-id');
    const stamp = header(req, 'x-watch-timestamp');
    const signature = header(req, 'x-watch-signature');
    const key = keys.get(keyId);
    if (header(req, 'x-watch-consumer') !== consumerId || !safeId(keyId) || !validKey(key) ||
        !/^(0|[1-9]\d{0,15})$/.test(stamp ?? '') || !Number.isSafeInteger(Number(stamp)) ||
        !/^[0-9a-f]{64}$/.test(signature ?? '')) return reply(res, 401);
    const chunks = [];
    let size = 0;
    req.on('error', () => { if (!res.destroyed) res.destroy(); });
    req.on('data', chunk => {
      if (res.writableEnded) return;
      size += chunk.length;
      if (size > MAX_EVENT_BYTES) return reply(res, 413);
      chunks.push(chunk);
    });
    req.on('end', () => {
      if (res.writableEnded || res.destroyed) return;
      try {
        const received = instant(now);
        const body = Buffer.concat(chunks, size);
        if (Math.abs(received - Number(stamp)) > maxSkewMs ||
            !timingSafeEqual(mac(key, consumerId, keyId, stamp, body), Buffer.from(signature, 'hex'))) {
          return reply(res, 401);
        }
        const outcome = inbox.accept(consumerId, body, received);
        const status = { accepted: 202, duplicate: 200, conflict: 409, invalid: 400, full: 503 }[outcome];
        reply(res, status ?? 503);
      } catch {
        reply(res, 503);
      }
    });
  });
  server.maxConnections = 32;
  server.maxRequestsPerSocket = 1;
  server.setTimeout(5000, socket => socket.destroy());
  server.on('clientError', (_error, socket) => socket.destroy());
  return server;
}
