import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { open, realpath } from 'node:fs/promises';
import { get } from 'node:http';
import { tmpdir } from 'node:os';
import { basename, dirname, isAbsolute, join, parse, relative, resolve, sep } from 'node:path';
import { normalizeObservation } from './sdk.mjs';

const VALUE_BYTES = 65_536;
const META_BYTES = 131_072;
const DEFAULT_TIMEOUT_MS = 2_000;
const MODES = new Map([
  ['until', 'durable_ready'],
  ['changes', 'observe_change'],
  ['emitted', 'source_emitted'],
]);
const TIMESTAMP = /^[1-9][0-9]{0,10}$/;
const UUID = /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/i;
const utf8 = new TextDecoder('utf-8', {fatal: true, ignoreBOM: true});

class SourceFault extends Error {
  constructor(code, coverage = 'failed') {
    super(code);
    this.code = code;
    this.coverage = coverage;
  }
}

function failed(contract, code, coverage = 'failed') {
  return {version: 1, contract, coverage, authority: 'advisory', code};
}

function complete(contract, fields) {
  try {
    return normalizeObservation({
      version: 1, contract, coverage: 'complete', authority: 'authoritative', ...fields,
    }, contract);
  } catch {
    throw new SourceFault('source_invalid');
  }
}

function integer(value, min, max) {
  return Number.isSafeInteger(value) && value >= min && value <= max;
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function sha256(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

function withBudget(timeoutMs, reader) {
  if (!integer(timeoutMs, 1, 30_000)) throw new TypeError('invalid_source_timeout');
  return async (context) => {
    const contract = MODES.get(context.mode);
    if (!contract || context.mode === 'emitted') {
      return failed('observe_change', 'unsupported_contract');
    }
    const timeout = AbortSignal.timeout(timeoutMs);
    const signal = context.signal ? AbortSignal.any([context.signal, timeout]) : timeout;
    let onAbort;
    try {
      signal.throwIfAborted();
      const abort = new Promise((_, reject) => {
        onAbort = () => reject(signal.reason);
        signal.addEventListener('abort', onAbort, {once: true});
      });
      return await Promise.race([abort, reader(context, contract, signal)]);
    } catch (error) {
      if (signal.aborted) {
        return failed(contract, signal.reason?.name === 'TimeoutError' ? 'source_timeout' : 'canceled');
      }
      if (error instanceof SourceFault) return failed(contract, error.code, error.coverage);
      return failed(contract, 'source_unavailable');
    } finally {
      if (onAbort) signal.removeEventListener('abort', onAbort);
    }
  };
}

/** A fixture frame supplies a value and, optionally, coverage, authority and an emitted ID. */
export function syntheticSource(frames) {
  if (!Array.isArray(frames) || !integer(frames.length, 1, 256)) {
    throw new TypeError('invalid_synthetic_frames');
  }
  let totalBytes = 0;
  const records = frames.map((frame) => {
    if (!isRecord(frame)) throw new TypeError('invalid_synthetic_frame');
    const {coverage = 'complete', authority = 'authoritative', source_event_id, code} = frame;
    const contract = source_event_id === undefined ? 'observe_change' : 'source_emitted';
    const record = normalizeObservation({
      version: 1, contract, coverage, authority,
      ...(Object.hasOwn(frame, 'value') && {value: frame.value}),
      ...(source_event_id !== undefined && {source_event_id}),
      ...(code !== undefined && {code}),
    }, contract);
    totalBytes += Buffer.byteLength(JSON.stringify(record));
    if (totalBytes > 1_048_576) throw new TypeError('invalid_synthetic_frames');
    return record;
  });
  return async ({checkpoint, mode, signal}) => {
    const contract = MODES.get(mode);
    if (!contract) return failed('observe_change', 'unsupported_contract');
    if (signal?.aborted) return failed(contract, 'canceled');
    const reads = checkpoint?.reads ?? 0;
    if (!integer(reads, 0, Number.MAX_SAFE_INTEGER)) return failed(contract, 'source_invalid');
    const index = Math.min(reads, records.length - 1);
    const {version, coverage, authority, value, source_event_id, code} = records[index];
    if ((mode === 'emitted') !== (source_event_id !== undefined)) {
      return failed(records[index].contract, 'unsupported_contract');
    }
    return normalizeObservation({
      version, contract, coverage, authority, cursor: String(index),
      ...(value !== undefined && {value}),
      ...(mode === 'emitted' && {source_event_id}),
      ...(code !== undefined && {code}),
    }, contract);
  };
}

async function fixturePath(file) {
  const dir = dirname(file);
  // System temporary directories can themselves be aliases (e.g. on macOS).
  // Resolve the selected root, then require the path beneath it to be literal.
  const roots = [resolve(tmpdir()), '/tmp'].sort((a, b) => b.length - a.length);
  const root = roots.find((item) => dir === item || dir.startsWith(item + sep)) ?? parse(dir).root;
  const physical = join(await realpath(root), relative(root, dir));
  if (await realpath(dir) !== physical) throw new SourceFault('source_invalid');
  return join(physical, basename(file));
}

/** An explicitly configured regular UTF-8 JSON file, sampled in a bounded read. */
export function jsonSource(path, {timeoutMs = DEFAULT_TIMEOUT_MS} = {}) {
  if (typeof path !== 'string' || path.length > 4_096 || !isAbsolute(path)
      || path.includes('\0') || resolve(path) !== path) {
    throw new TypeError('invalid_source_path');
  }
  return withBudget(timeoutMs, async (_context, contract, signal) => {
    const file = await fixturePath(path);
    signal.throwIfAborted();
    let handle;
    try {
      handle = await open(file, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
      const before = await handle.stat({bigint: true});
      if (!before.isFile()) throw new SourceFault('source_invalid');
      if (before.size > BigInt(VALUE_BYTES)) throw new SourceFault('capacity');
      const bytes = Buffer.alloc(Number(before.size) + 1);
      let length = 0;
      while (length < bytes.length) {
        signal.throwIfAborted();
        const {bytesRead} = await handle.read(bytes, length, bytes.length - length, length);
        if (!bytesRead) break;
        length += bytesRead;
      }
      const after = await handle.stat({bigint: true});
      signal.throwIfAborted();
      if (before.size !== BigInt(length) || after.size !== before.size
          || after.mtimeNs !== before.mtimeNs || after.ctimeNs !== before.ctimeNs) {
        throw new SourceFault('coverage_partial', 'partial');
      }
      const body = bytes.subarray(0, length);
      let value;
      try {
        value = JSON.parse(utf8.decode(body));
      } catch {
        throw new SourceFault('source_invalid');
      }
      const digest = sha256(body);
      return complete(contract, {value, cursor: digest, evidence: {snapshot_digest: digest, snapshot_bytes: length}});
    } catch (error) {
      if (error.code === 'ELOOP') throw new SourceFault('source_invalid');
      throw error;
    } finally {
      await handle?.close();
    }
  });
}

/** Read exactly one allowed v1 path using a direct numeric-address HTTP connection. */
function readApi(port, path, key, format, limit, signal) {
  return new Promise((resolveBody, reject) => {
    const headers = {accept: format, 'accept-encoding': 'identity'};
    if (key !== undefined) headers['x-api-key'] = key;
    const request = get({hostname: '127.0.0.1', port, path, headers, signal, agent: false, maxHeaderSize: 8_192},
      (response) => {
        const stop = (code) => {
          reject(new SourceFault(code));
          response.destroy();
        };
        if (response.statusCode !== 200) return stop('source_unavailable');
        const type = (response.headers['content-type'] ?? '').split(';')[0].trim().toLowerCase();
        const encoding = response.headers['content-encoding'] ?? 'identity';
        if (type !== format || encoding !== 'identity') return stop('source_invalid');
        if (Number(response.headers['content-length']) > limit) return stop('capacity');
        const parts = [];
        let length = 0;
        response.on('data', (part) => {
          length += part.length;
          if (length > limit) return stop('capacity');
          parts.push(part);
        });
        response.on('error', reject);
        response.on('end', () => resolveBody(Buffer.concat(parts, length)));
      });
    request.on('error', reject);
  });
}

function decodeJson(bytes) {
  try {
    return JSON.parse(utf8.decode(bytes));
  } catch {
    throw new SourceFault('source_invalid');
  }
}

function fresh(seconds, now, maxAgeMs) {
  return integer(seconds, 1, 99_999_999_999) && seconds * 1_000 <= now && now - seconds * 1_000 <= maxAgeMs;
}

function readHealth(meta, watchId, now, maxAgeMs) {
  if (!isRecord(meta) || meta.uuid !== watchId || meta.processor !== 'text_json_diff'
      || typeof meta.paused !== 'boolean' || !Object.hasOwn(meta, 'last_error')
      || !integer(meta.last_checked, 0, 99_999_999_999)
      || !integer(meta.last_changed, 0, 99_999_999_999) || !integer(meta.history_n, 0, 1_024)) {
    throw new SourceFault('source_invalid');
  }
  if (![false, null, ''].includes(meta.last_error) || meta.paused || meta.ignore_status_codes === true
      || (meta.consecutive_filter_failures ?? 0) !== 0) {
    throw new SourceFault('source_error');
  }
  if (!fresh(meta.last_checked, now, maxAgeMs)) throw new SourceFault('source_stale', 'partial');
  return {checked: meta.last_checked, changed: meta.last_changed, count: meta.history_n};
}

function retainedKeys(history) {
  if (!isRecord(history)) throw new SourceFault('source_invalid');
  const keys = Object.keys(history);
  if (!keys.length) throw new SourceFault('history_gap', 'partial');
  if (keys.length > 1_024) throw new SourceFault('capacity');
  if (!keys.every((key) => TIMESTAMP.test(key))) throw new SourceFault('source_invalid');
  return keys.sort((a, b) => Number(a) - Number(b));
}

/** Read one fresh retained text snapshot from an operator-configured loopback API. */
export function changedetectionSource({baseUrl, watchId, apiKey, maxAgeMs = 300_000, timeoutMs = DEFAULT_TIMEOUT_MS}) {
  const host = typeof baseUrl === 'string' && /^http:\/\/127\.0\.0\.1:([1-9][0-9]{0,4})\/?$/.exec(baseUrl);
  if (!host || Number(host[1]) > 65_535 || typeof watchId !== 'string' || !UUID.test(watchId)
      || (apiKey !== undefined && (typeof apiKey !== 'string' || !/^[!-~]{1,256}$/.test(apiKey)))
      || !integer(maxAgeMs, 1, 86_400_000)) {
    throw new TypeError('invalid_web_source');
  }
  const port = Number(host[1]);
  const prefix = '/api/v1/watch/' + watchId;
  return withBudget(timeoutMs, async ({checkpoint, now, mode}, contract, signal) => {
    if (!integer(now, 0, Number.MAX_SAFE_INTEGER)) throw new SourceFault('source_invalid');
    const read = (suffix, format = 'application/json', limit = META_BYTES, budget = signal) =>
      readApi(port, prefix + suffix, apiKey, format, limit, budget);
    const health = async () => readHealth(decodeJson(await read('')), watchId, now, maxAgeMs);
    const before = await health();
    const keys = retainedKeys(decodeJson(await read('/history')));
    const latest = keys.at(-1);
    const firstSnapshot = keys.length === 1 && before.changed === 0;
    if (before.count !== keys.length || (!firstSnapshot && before.changed !== Number(latest))) {
      throw new SourceFault('coverage_partial', 'partial');
    }
    if (!fresh(Number(latest), now, maxAgeMs)) throw new SourceFault('source_stale', 'partial');
    const previous = checkpoint?.cursor;
    if (previous != null && (typeof previous !== 'string' || !TIMESTAMP.test(previous))) {
      throw new SourceFault('source_invalid');
    }
    if (previous != null && Number(previous) > Number(latest)) throw new SourceFault('history_gap', 'partial');
    const body = await read('/history/' + (mode === 'until' ? 'latest' : latest), 'text/plain', VALUE_BYTES);
    let text;
    try {
      text = utf8.decode(body);
    } catch {
      throw new SourceFault('source_invalid');
    }
    // A one-entry history can report last_changed=0 even as retention replaces
    // its sole entry. Check the index itself on both sides of the body read.
    const confirmed = retainedKeys(decodeJson(await read('/history')));
    const after = await health();
    if (confirmed.length !== keys.length || confirmed.at(-1) !== latest
        || before.checked !== after.checked || before.changed !== after.changed || before.count !== after.count) {
      throw new SourceFault('coverage_partial', 'partial');
    }
    const evidence = {
      snapshot_digest: sha256(body), snapshot_bytes: body.length,
      history_count: keys.length, history_complete: false,
    };
    let code;
    if (mode === 'changes' && previous != null && previous !== latest) {
      if (keys.includes(previous)) {
        // Upstream diff is supplementary. A retained pair may disappear independently.
        try {
          const diffBudget = AbortSignal.any([signal, AbortSignal.timeout(Math.max(1, Math.min(250, Math.floor(timeoutMs / 4))))]);
          const diff = await read('/difference/' + previous + '/' + latest + '?format=text', 'text/plain', VALUE_BYTES, diffBudget);
          evidence.diff_digest = sha256(diff);
          evidence.diff_bytes = diff.length;
        } catch {
          signal.throwIfAborted();
          code = 'history_gap';
        }
      } else {
        code = 'history_gap';
      }
    }
    return complete(contract, {value: {text}, cursor: latest, evidence, ...(code && {code})});
  });
}
