import { createHash } from 'node:crypto';
import { types } from 'node:util';

export const MAX_EVENT_BYTES = 32_768;
export const MAX_VALUE_BYTES = 65_536;

const JSON_LIMIT = { bytes: 262_144, depth: 40, nodes: 16_384 };
const VALUE_LIMIT = { bytes: MAX_VALUE_BYTES, depth: 32, nodes: 8_192 };
const PREDICATE_LIMIT = { ...VALUE_LIMIT, bytes: 4_096 };
const MAX_PATH_BYTES = 512;
const MAX_CHANGES = 32;
const MAX_CHANGE_LIST_BYTES = 12_288;

const contracts = new Set(['durable_ready', 'observe_change', 'source_emitted']);
const kinds = new Set(['ready', 'changed', 'emitted', 'deadline', 'canceled', 'error']);
const codes = new Set([
  'source_unavailable', 'source_timeout', 'source_invalid', 'source_stale', 'source_error',
  'coverage_partial', 'coverage_failed', 'advisory', 'history_gap', 'unsupported_contract',
  'invalid_observation', 'invalid_predicate', 'deadline_reached', 'canceled', 'capacity',
]);
const digestPattern = /^[0-9a-f]{64}$/u;
const idPattern = /^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/u;
const own = Object.hasOwn;

function reject(code) {
  throw Object.assign(new TypeError(code), { code });
}

function isRecord(value) {
  if (value === null || typeof value !== 'object' || types.isProxy(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

function isDataField(record, key) {
  const descriptor = Object.getOwnPropertyDescriptor(record, key);
  return descriptor?.enumerable && own(descriptor, 'value');
}

function fields(record, required, optional, code) {
  if (!isRecord(record)) reject(code);
  const keys = Reflect.ownKeys(record);
  if (keys.length > required.length + optional.length) reject(code);
  for (const key of keys) {
    if ((!required.includes(key) && !optional.includes(key)) || !isDataField(record, key)) reject(code);
  }
  if (required.some(key => !own(record, key))) reject(code);
}

function boundedString(value, max, allowEmpty = false) {
  return typeof value === 'string' && (allowEmpty || value.length > 0)
    && value.length <= max && Buffer.byteLength(value) <= max;
}

function count(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function isDigest(value) {
  return typeof value === 'string' && value.length === 64 && digestPattern.test(value);
}

function isId(value) {
  return boundedString(value, 128) && idPattern.test(value);
}

function arrayShape(value, maxLength, code) {
  if (types.isProxy(value) || !Array.isArray(value) || value.length > maxLength
      || Reflect.ownKeys(value).length !== value.length + 1) reject(code);
  for (let index = 0; index < value.length; index++) {
    if (!isDataField(value, index)) reject(code);
  }
}

// Sorting the encoded object members here also orders keys that look like array indices.
// Calling JSON.stringify on a rebuilt object would reorder those keys numerically.
function encode(value, limits, code) {
  const chunks = [];
  const active = new Set();
  let bytes = 0;
  let nodes = 0;

  function write(chunk) {
    bytes += Buffer.byteLength(chunk);
    if (bytes > limits.bytes) reject(code);
    chunks.push(chunk);
  }

  function quoted(string) {
    if (string.length > limits.bytes - bytes) reject(code);
    write(JSON.stringify(string));
  }

  function visit(item, depth) {
    if (++nodes > limits.nodes || depth > limits.depth) reject(code);
    if (typeof item === 'string') return quoted(item);
    if (item === null || typeof item === 'boolean') return write(JSON.stringify(item));
    if (typeof item === 'number' && Number.isFinite(item)) return write(JSON.stringify(item));
    if (typeof item !== 'object' || types.isProxy(item) || active.has(item)) reject(code);

    active.add(item);
    if (Array.isArray(item)) {
      arrayShape(item, limits.nodes - nodes, code);
      write('[');
      for (let index = 0; index < item.length; index++) {
        if (index) write(',');
        visit(item[index], depth + 1);
      }
      write(']');
    } else {
      if (!isRecord(item)) reject(code);
      const keys = Reflect.ownKeys(item);
      if (keys.length > limits.nodes - nodes || keys.some(key => typeof key !== 'string')) reject(code);
      keys.sort();
      write('{');
      for (const [index, key] of keys.entries()) {
        if (!isDataField(item, key)) reject(code);
        if (index) write(',');
        quoted(key);
        write(':');
        visit(item[key], depth + 1);
      }
      write('}');
    }
    active.delete(item);
  }

  visit(value, 0);
  return chunks.join('');
}

export function canonical(value) {
  return encode(value, JSON_LIMIT, 'invalid_json');
}

function sha256(text) {
  return createHash('sha256').update(text, 'utf8').digest('hex');
}

export function digest(value) {
  return sha256(canonical(value));
}

const sourceHashes = ['snapshot_digest', 'diff_digest'];
const sourceCounts = ['snapshot_bytes', 'diff_bytes', 'history_count'];

function observationEvidence(raw, code) {
  fields(raw, [], [...sourceHashes, ...sourceCounts, 'history_complete'], code);
  const evidence = {};
  for (const key of sourceHashes) {
    if (own(raw, key)) {
      if (!isDigest(raw[key])) reject(code);
      evidence[key] = raw[key];
    }
  }
  for (const key of sourceCounts) {
    if (own(raw, key)) {
      if (!count(raw[key])) reject(code);
      evidence[key] = raw[key];
    }
  }
  if (own(raw, 'history_complete')) {
    if (raw.history_complete !== false) reject(code);
    evidence.history_complete = false;
  }
  return evidence;
}

export function normalizeObservation(raw, expectedContract) {
  const code = 'invalid_observation';
  fields(raw, ['version', 'contract', 'coverage', 'authority'],
    ['value', 'cursor', 'source_event_id', 'evidence', 'code'], code);
  if (raw.version !== 1 || !contracts.has(raw.contract)
      || (expectedContract !== undefined && raw.contract !== expectedContract)) reject('unsupported_contract');
  if (!['complete', 'partial', 'failed'].includes(raw.coverage)
      || !['authoritative', 'advisory'].includes(raw.authority)) reject(code);

  const observation = { version: 1, contract: raw.contract, coverage: raw.coverage, authority: raw.authority };
  if (own(raw, 'value')) observation.value = JSON.parse(encode(raw.value, VALUE_LIMIT, code));
  if (own(raw, 'cursor')) {
    if (!boundedString(raw.cursor, 128)) reject(code);
    observation.cursor = raw.cursor;
  }
  if (own(raw, 'source_event_id')) {
    if (raw.contract !== 'source_emitted' || !boundedString(raw.source_event_id, 128)) reject(code);
    observation.source_event_id = raw.source_event_id;
  }
  if (raw.coverage === 'complete') {
    if (raw.contract === 'source_emitted' && !own(raw, 'source_event_id')) reject(code);
    if (raw.contract !== 'source_emitted' && raw.authority === 'authoritative' && !own(raw, 'value')) reject(code);
  }
  if (own(raw, 'evidence')) observation.evidence = observationEvidence(raw.evidence, code);
  if (own(raw, 'code')) {
    if (!codes.has(raw.code)) reject(code);
    observation.code = raw.code;
  }
  return observation;
}

/** Accepts an observation returned by normalizeObservation. */
export function isAuthoritativeObservation(observation) {
  return observation.coverage === 'complete' && observation.authority === 'authoritative'
    && (observation.contract === 'source_emitted' ? own(observation, 'source_event_id') : own(observation, 'value'));
}

function pointerParts(path, code) {
  if (!boundedString(path, MAX_PATH_BYTES, true)) reject(code);
  if (path === '') return [];
  if (!path.startsWith('/') || /~(?![01])/u.test(path)) reject(code);
  const parts = path.slice(1).split('/');
  if (parts.length > VALUE_LIMIT.depth) reject(code);
  return parts.map(part => part.replaceAll('~1', '/').replaceAll('~0', '~'));
}

export function validatePredicate(raw) {
  const code = 'invalid_predicate';
  fields(raw, ['path'], ['equals', 'contains'], code);
  pointerParts(raw.path, code);
  if (own(raw, 'equals') === own(raw, 'contains')) reject(code);
  if (own(raw, 'equals')) return { path: raw.path, equals: JSON.parse(encode(raw.equals, PREDICATE_LIMIT, code)) };
  if (!boundedString(raw.contains, 1_024)) reject(code);
  return { path: raw.path, contains: raw.contains };
}

function jsonType(value) {
  if (value === null) return 'null';
  return Array.isArray(value) ? 'array' : typeof value;
}

/** Call only for complete, authoritative snapshots. Missing fields and incompatible types are unknown. */
export function evaluatePredicate(value, rawPredicate) {
  const predicate = validatePredicate(rawPredicate);
  let selected;
  try {
    selected = JSON.parse(encode(value, VALUE_LIMIT, 'invalid_json'));
  } catch {
    return 'UNKNOWN';
  }
  for (const key of pointerParts(predicate.path, 'invalid_predicate')) {
    if (selected === null || typeof selected !== 'object' || !own(selected, key)) return 'UNKNOWN';
    if (Array.isArray(selected) && !/^(0|[1-9][0-9]*)$/u.test(key)) return 'UNKNOWN';
    selected = selected[key];
  }
  if (own(predicate, 'equals')) {
    if (jsonType(selected) !== jsonType(predicate.equals)) return 'UNKNOWN';
    return canonical(selected) === canonical(predicate.equals) ? 'TRUE' : 'FALSE';
  }
  if (typeof selected !== 'string') return 'UNKNOWN';
  return selected.includes(predicate.contains) ? 'TRUE' : 'FALSE';
}

function pointerChild(path, key) {
  return path + '/' + String(key).replaceAll('~', '~0').replaceAll('/', '~1');
}

function allowedPaths(options) {
  const code = 'invalid_diff_options';
  fields(options, [], ['publicPaths'], code);
  const paths = new Set();
  if (!own(options, 'publicPaths')) return paths;
  const input = options.publicPaths;
  let entries;
  if (types.isSet(input)) entries = Set.prototype.values.call(input);
  else {
    arrayShape(input, 64, code);
    entries = Array.prototype.values.call(input);
  }
  let count = 0;
  for (const path of entries) {
    if (++count > 64) reject(code);
    pointerParts(path, code);
    paths.add(path);
  }
  return paths;
}

/** Paths are digests of canonical JSON pointer strings. Operators can disclose individual pointers explicitly. */
export function diffEvidence(before, after, options = {}) {
  const publicPaths = allowedPaths(options);
  const left = encode(before, VALUE_LIMIT, 'invalid_json');
  const right = encode(after, VALUE_LIMIT, 'invalid_json');
  const evidence = { before_digest: sha256(left), after_digest: sha256(right), changes: [], truncated: false };
  if (left === right) return evidence;

  const absent = Symbol();
  let changeBytes = 2;
  function change(path, op) {
    if (evidence.changes.length >= MAX_CHANGES) {
      evidence.truncated = true;
      return;
    }
    const entry = publicPaths.has(path) ? { path, op } : { path_digest: digest(path), op };
    const size = Buffer.byteLength(JSON.stringify(entry)) + (evidence.changes.length ? 1 : 0);
    if (changeBytes + size > MAX_CHANGE_LIST_BYTES) {
      evidence.truncated = true;
      return;
    }
    changeBytes += size;
    evidence.changes.push(entry);
  }

  function walk(a, b, path) {
    if (a === b || (evidence.truncated && evidence.changes.length === MAX_CHANGES)) return;
    if (a === absent) return change(path, 'add');
    if (b === absent) return change(path, 'remove');
    if (jsonType(a) !== jsonType(b) || a === null || typeof a !== 'object') return change(path, 'replace');
    if (Array.isArray(a)) {
      for (let i = 0; i < Math.max(a.length, b.length); i++) {
        walk(i < a.length ? a[i] : absent, i < b.length ? b[i] : absent, pointerChild(path, i));
      }
    } else {
      const keys = [...new Set([...Object.keys(a), ...Object.keys(b)])].sort();
      for (const key of keys) {
        walk(own(a, key) ? a[key] : absent, own(b, key) ? b[key] : absent, pointerChild(path, key));
      }
    }
  }

  walk(JSON.parse(left), JSON.parse(right), '');
  return evidence;
}

function diffSummary(raw, code) {
  fields(raw, ['before_digest', 'after_digest', 'changes', 'truncated'], [], code);
  if (!isDigest(raw.before_digest) || !isDigest(raw.after_digest) || typeof raw.truncated !== 'boolean') reject(code);
  arrayShape(raw.changes, MAX_CHANGES, code);
  const changes = [];
  for (let i = 0; i < raw.changes.length; i++) {
    const entry = raw.changes[i];
    fields(entry, ['op'], ['path', 'path_digest'], code);
    if (own(entry, 'path') === own(entry, 'path_digest')) reject(code);
    if (!['add', 'remove', 'replace'].includes(entry.op)) reject(code);
    if (own(entry, 'path')) {
      pointerParts(entry.path, code);
      changes.push({ path: entry.path, op: entry.op });
    } else {
      if (!isDigest(entry.path_digest)) reject(code);
      changes.push({ path_digest: entry.path_digest, op: entry.op });
    }
  }
  if (Buffer.byteLength(JSON.stringify(changes)) > MAX_CHANGE_LIST_BYTES) reject(code);
  return { before_digest: raw.before_digest, after_digest: raw.after_digest, changes, truncated: raw.truncated };
}

function eventEvidence(raw, code) {
  fields(raw, [], ['source_digest', 'source_event_digest', 'observation', 'diff', 'code'], code);
  const evidence = {};
  for (const key of ['source_digest', 'source_event_digest']) {
    if (own(raw, key)) {
      if (!isDigest(raw[key])) reject(code);
      evidence[key] = raw[key];
    }
  }
  if (own(raw, 'observation')) evidence.observation = observationEvidence(raw.observation, code);
  if (own(raw, 'diff')) evidence.diff = diffSummary(raw.diff, code);
  if (own(raw, 'code')) {
    if (!codes.has(raw.code)) reject(code);
    evidence.code = raw.code;
  }
  return evidence;
}

function freezeTree(value) {
  if (value !== null && typeof value === 'object') {
    for (const child of Object.values(value)) freezeTree(child);
    Object.freeze(value);
  }
  return value;
}

const eventFields = ['version', 'event_id', 'watch_id', 'sequence', 'kind', 'observed_at', 'evidence'];

function eventValue(raw, sealed) {
  const code = 'invalid_event';
  fields(raw, sealed ? [...eventFields, 'digest'] : eventFields, [], code);
  if (raw.version !== 1 || !isId(raw.event_id) || !isId(raw.watch_id) || !count(raw.sequence)
      || raw.sequence < 1 || !kinds.has(raw.kind) || !count(raw.observed_at)) reject(code);
  const event = {
    version: 1,
    event_id: raw.event_id,
    watch_id: raw.watch_id,
    sequence: raw.sequence,
    kind: raw.kind,
    observed_at: raw.observed_at,
    evidence: eventEvidence(raw.evidence, code),
  };
  const body = encode(event, { ...JSON_LIMIT, bytes: MAX_EVENT_BYTES }, code);
  event.digest = sha256(body);
  if (sealed && raw.digest !== event.digest) reject(code);
  if (Buffer.byteLength(JSON.stringify(event)) > MAX_EVENT_BYTES) reject(code);
  return freezeTree(event);
}

export function sealEvent(fields) {
  return eventValue(fields, false);
}

export function validateEvent(event) {
  return eventValue(event, true);
}
