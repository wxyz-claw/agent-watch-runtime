import { createHash, createHmac, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { closeSync, constants, fstatSync, lstatSync, mkdirSync, openSync, readSync,
  realpathSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { createRequire } from 'node:module';
import { changedetectionSource, jsonSource, syntheticSource } from './sources.mjs';

const marker = 'watch-dev.json';
const contact = 'receiver.json';
const format = 'agent-watch-runtime/dev-v1';
const uuid = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/;
const consumerId = 'worker';
const leaseMs = 5000;
const setupFaults = new Set(['receiver_already_running', 'receiver_registration_lost',
  'invalid_development_directory', 'invalid_state_file', 'invalid_state_key', 'already_exists',
  'missing_state', 'local_access_denied', 'receiver_bind_failed', 'storage_full',
  'storage_io_error', 'storage_busy', 'storage_read_only', 'setup_failed']);
const systemFaults = new Map([['EEXIST', 'already_exists'], ['ENOENT', 'missing_state'],
  ['EPERM', 'local_access_denied'], ['EACCES', 'local_access_denied'], ['EADDRINUSE', 'receiver_bind_failed'],
  ['ENOSPC', 'storage_full'], ['EDQUOT', 'storage_full'], ['EIO', 'storage_io_error'], ['EROFS', 'storage_read_only']]);
const sqliteFaults = new Map([[5, 'storage_busy'], [6, 'storage_busy'], [8, 'storage_read_only'],
  [10, 'storage_io_error'], [13, 'storage_full']]);

/** Constrain subprocess diagnostics to static labels before reporting them. */
export function setupFailure(error) {
  if (setupFaults.has(error?.message)) return error.message;
  const system = systemFaults.get(error?.code);
  if (system) return system;
  if (error?.code === 'ERR_SQLITE_ERROR' && Number.isInteger(error.errcode)) {
    return sqliteFaults.get(error.errcode & 255) ?? 'setup_failed';
  }
  return 'setup_failed';
}

function freshFile(path, data) {
  writeFileSync(path, data, {flag: 'wx', mode: 0o600});
}

function readLocal(path, max) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.nlink !== 1 || stat.size > max) throw Error('invalid_state_file');
    const buffer = Buffer.alloc(stat.size + 1);
    let used = 0;
    while (used < buffer.length) {
      const n = readSync(fd, buffer, used, buffer.length - used, used);
      if (!n) break;
      used += n;
    }
    if (used !== stat.size) throw Error('invalid_state_file');
    return buffer.subarray(0, used);
  } finally {
    closeSync(fd);
  }
}

function databasePath(directory, name) {
  const file = join(directory, name);
  for (const suffix of ['', '-wal', '-shm', '-journal']) {
    const stat = lstatSync(file + suffix, {throwIfNoEntry: false});
    if (suffix && !stat) continue;
    if (!stat?.isFile() || stat.nlink !== 1) throw Error('invalid_state_file');
  }
  return file;
}

export function initDevelopment(path) {
  const directory = resolve(path);
  // Exclusive creation keeps repeated init calls away from preexisting data.
  mkdirSync(directory, {mode: 0o700});
  freshFile(join(directory, '.gitignore'), '*\n');
  freshFile(join(directory, 'webhook.key'), randomBytes(32));
  freshFile(join(directory, 'sample.json'), '{"phase":"waiting"}\n');
  freshFile(join(directory, 'runtime.sqlite'), Buffer.alloc(0));
  freshFile(join(directory, 'inbox.sqlite'), Buffer.alloc(0));
  freshFile(join(directory, 'receiver.lock'), Buffer.alloc(0));
  // Write the marker last. Partial setup cannot become a runnable instance.
  freshFile(join(directory, marker), JSON.stringify({format, namespace: randomUUID()}) + '\n');
}

function contactMac(key, record) {
  return createHmac('sha256', key).update([
    'agent-watch-runtime/local-receiver-v1', record.namespace, record.instance,
    record.pid, record.port, record.expires,
  ].join('\n')).digest();
}

function readContact(directory, namespace, key) {
  try {
    const r = JSON.parse(readLocal(join(directory, contact), 1024));
    if (r.namespace !== namespace || !uuid.test(r.instance) ||
        !Number.isSafeInteger(r.pid) || r.pid < 1 || !Number.isSafeInteger(r.port) ||
        r.port < 1 || r.port > 65535 || !Number.isSafeInteger(r.expires) ||
        typeof r.mac !== 'string' || !/^[0-9a-f]{64}$/.test(r.mac) ||
        !timingSafeEqual(contactMac(key, r), Buffer.from(r.mac, 'hex'))) return null;
    return r;
  } catch {
    return null;
  }
}

function activeContact(directory, namespace, key) {
  const record = readContact(directory, namespace, key);
  const now = Date.now();
  if (!record || record.expires < now || record.expires > now + leaseMs) return null;
  try {
    // Signal zero checks the lifetime of our authenticated receiver registration.
    process.kill(record.pid, 0);
  } catch {
    return null;
  }
  return record;
}

function optionalWebSource(directory) {
  const path = join(directory, 'web-source.json');
  if (!lstatSync(path, {throwIfNoEntry: false})) return null;
  let reader;
  return async context => {
    if (!reader) {
      try {
        const bytes = readLocal(path, 4096);
        const options = JSON.parse(new TextDecoder('utf-8', {fatal: true}).decode(bytes));
        const fields = ['baseUrl', 'watchId', 'apiKey', 'maxAgeMs', 'timeoutMs'];
        if (!options || typeof options !== 'object' || Array.isArray(options) ||
            !Object.hasOwn(options, 'baseUrl') || !Object.hasOwn(options, 'watchId') ||
            Object.keys(options).some(key => !fields.includes(key))) throw Error('invalid_web_source');
        reader = changedetectionSource(options);
      } catch {
        // Keep configuration failures local to this source. Its raw options
        // and parsing diagnostics can contain a development API key.
        reader = ({mode}) => ({
          version: 1, contract: {until: 'durable_ready', changes: 'observe_change', emitted: 'source_emitted'}[mode],
          coverage: 'failed', authority: 'advisory', code: 'source_invalid',
        });
      }
    }
    return reader(context);
  };
}

export function openDevelopment(path) {
  const selected = resolve(path);
  if (!lstatSync(selected).isDirectory()) throw Error('invalid_development_directory');
  const directory = realpathSync(selected);
  const instance = JSON.parse(readLocal(join(directory, marker), 256));
  if (instance.format !== format || !uuid.test(instance.namespace)) throw Error('invalid_development_directory');
  const key = readLocal(join(directory, 'webhook.key'), 32);
  if (key.length !== 32) throw Error('invalid_state_key');
  const keyId = 'dev-' + createHash('sha256').update(key).digest('hex').slice(0, 16);
  const {namespace} = instance;
  const fixture = join(directory, 'sample.json');
  const routes = new Map([[consumerId, {keyId, key, get url() {
    const route = activeContact(directory, namespace, key);
    return route ? 'http://127.0.0.1:' + route.port + '/inbox' : undefined;
  }}]]);
  const sources = new Map([
    ['sample', syntheticSource([{value: {phase: 'waiting'}}, {value: {phase: 'ready'}}])],
    ['json', jsonSource(fixture)],
    ['ticks', syntheticSource([
      {source_event_id: 'tick-1', value: {count: 1}},
      {source_event_id: 'tick-2', value: {count: 2}},
    ])],
  ]);
  const web = optionalWebSource(directory);
  if (web) sources.set('web', web);
  return {directory, namespace, consumerId, key, keyId, routes, sources, fixture,
    runtimeDb: databasePath(directory, 'runtime.sqlite'), inboxDb: databasePath(directory, 'inbox.sqlite'),
    receiverLock: databasePath(directory, 'receiver.lock')};
}

// Claim ownership before opening the inbox or binding any network listener.
export function claimReceiver({directory, namespace, key, receiverLock}) {
  // Hold an OS-backed writer lock on a dedicated empty file. There are no
  // database rows or journal writes here. Process exit releases ownership.
  const {DatabaseSync} = createRequire(import.meta.url)('node:sqlite');
  const ownership = new DatabaseSync(receiverLock);
  try {
    ownership.exec('PRAGMA busy_timeout=0; BEGIN IMMEDIATE');
  } catch (error) {
    ownership.close();
    if (error.errcode === 5 || error.errcode === 6) throw Error('receiver_already_running');
    throw Error('invalid_state_file');
  }
  const instance = randomUUID();
  const file = join(directory, contact);
  const temporary = join(directory, '.receiver-' + instance + '.tmp');
  let closed = false;
  let port;
  function writeLease() {
    const record = {namespace, instance, pid: process.pid, port, expires: Date.now() + leaseMs};
    const mac = contactMac(key, record).toString('hex');
    freshFile(temporary, JSON.stringify({...record, mac}) + '\n');
    renameSync(temporary, file);
  }
  return {
    // Publish only the actual bound port. Pending attempts discover it afresh.
    publish(boundPort) {
      if (closed || port !== undefined || !Number.isInteger(boundPort) || boundPort < 1 || boundPort > 65535) {
        throw Error('invalid_receiver_port');
      }
      port = boundPort;
      writeLease();
    },
    refresh() {
      if (closed || port === undefined || readContact(directory, namespace, key)?.instance !== instance) {
        throw Error('receiver_registration_lost');
      }
      writeLease();
    },
    close() {
      if (closed) return;
      closed = true;
      try {
        if (readContact(directory, namespace, key)?.instance === instance) unlinkSync(file);
      } finally {
        ownership.close();
      }
    },
  };
}
