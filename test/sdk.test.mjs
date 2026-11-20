import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  canonical, diffEvidence, digest, evaluatePredicate, isAuthoritativeObservation,
  MAX_EVENT_BYTES, normalizeObservation, sealEvent, validateEvent, validatePredicate,
} from '../src/sdk.mjs';

const snapshot = {
  version: 1, contract: 'durable_ready', coverage: 'complete', authority: 'authoritative', value: { ready: true },
};
const fields = { version: 1, event_id: 'e', watch_id: 'w', sequence: 1, kind: 'ready', observed_at: 123, evidence: {} };
const eventDigest = 'e50978a483f11f7e559933719811536b70db4fe37838fdf42c3aefb131796314';
const error = code => ({ name: 'TypeError', message: code, code });

test('canonical JSON sorts all object keys by code units and preserves array positions', () => {
  assert.equal(canonical({ '2': 2, '10': 10, a: [{ z: -0, a: 1 }, false, null] }),
    '{"10":10,"2":2,"a":[{"a":1,"z":0},false,null]}');
  assert.equal(digest({ b: 2, a: 1 }), '43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777');
  assert.notEqual(digest([2, 1]), digest([1, 2]));
});

test('JSON boundaries reject non-JSON values and bound byte count, nesting and nodes', () => {
  const circular = {};
  circular.value = circular;
  const nested = Array.from({ length: 42 }).reduce(value => [value], false);
  const accessor = Object.defineProperty({}, 'field', {
    enumerable: true, get() { throw new Error('unexpected accessor'); },
  });
  for (const sample of [new Date(), new Array(1), circular, accessor, 1n, undefined, NaN, Infinity,
    { field: undefined }, Object.assign([], { field: true }), nested, 'é'.repeat(131_072), new Array(17_000).fill(0)]) {
    assert.throws(() => canonical(sample), error('invalid_json'));
  }
});

test('the three observation contracts carry distinct admission criteria', () => {
  for (const contract of ['durable_ready', 'observe_change']) {
    const raw = { ...snapshot, contract, evidence: { snapshot_digest: digest(true), snapshot_bytes: 4 } };
    const observation = normalizeObservation(raw, contract);
    assert.deepEqual(observation, raw);
    assert.ok(isAuthoritativeObservation(observation));
    assert.notEqual(observation.value, raw.value);
    assert.throws(() => normalizeObservation(raw, 'source_emitted'), error('unsupported_contract'));
  }
  const emission = { version: 1, contract: 'source_emitted', coverage: 'complete', authority: 'authoritative',
    source_event_id: 'sample-1', cursor: 'page-1' };
  assert.ok(isAuthoritativeObservation(normalizeObservation(emission, 'source_emitted')));
  const { source_event_id, ...withoutIdentity } = emission;
  assert.throws(() => normalizeObservation(withoutIdentity), error('invalid_observation'));
  assert.throws(() => normalizeObservation({ ...snapshot, source_event_id }), error('invalid_observation'));
  assert.throws(() => normalizeObservation({ ...snapshot, version: 2 }), error('unsupported_contract'));
});

test('bounded degradation keeps partial, failed, and advisory evidence inconclusive', () => {
  for (const coverage of ['complete', 'partial', 'failed']) {
    for (const authority of ['authoritative', 'advisory']) {
      const normalized = normalizeObservation({ ...snapshot, coverage, authority });
      assert.equal(isAuthoritativeObservation(normalized), coverage === 'complete' && authority === 'authoritative');
    }
  }
  const { value, ...withoutValue } = snapshot;
  assert.throws(() => normalizeObservation(withoutValue), error('invalid_observation'));
  assert.ok(!isAuthoritativeObservation(normalizeObservation({ ...withoutValue, coverage: 'failed', code: 'source_error' })));
  assert.ok(isAuthoritativeObservation(normalizeObservation({ ...snapshot, value: null })));
});

test('observation inputs bound source values and expose only registered evidence and diagnostic codes', () => {
  for (const override of [
    { code: 'unregistered sample details' }, { extra: 3 }, { evidence: { history_complete: true } },
    { evidence: { diff_bytes: -1 } }, { evidence: { snapshot_digest: '0' } }, { evidence: { body: 'sample' } },
    { cursor: 'é'.repeat(65) }, { value: '0'.repeat(65_535) }, { coverage: 'ok' }, { authority: 'unchecked' },
    { value: Array.from({ length: 33 }).reduce(value => [value], true) },
    { value: new Array(8_192).fill(0) },
  ]) {
    assert.throws(() => normalizeObservation({ ...snapshot, ...override }), error('invalid_observation'));
  }
  assert.equal(normalizeObservation({ ...snapshot, value: '0'.repeat(65_534) }).value.length, 65_534);
  assert.ok(normalizeObservation({ ...snapshot, value: new Array(8_191).fill(0) }));
});

test('three-valued predicates distinguish false, missing data, and incompatible types', () => {
  const value = { reports: [{ score: 0 }], text: 'sample is ready', 'a/b': { '~key': 'ok' }, empty: null };
  for (const [path, equals, expected] of [
    ['/reports/0/score', 0, 'TRUE'], ['/reports/0/score', 1, 'FALSE'], ['/reports/0/score', '0', 'UNKNOWN'],
    ['/reports/0/missing', 0, 'UNKNOWN'], ['/reports/00', null, 'UNKNOWN'], ['/reports/length', 1, 'UNKNOWN'],
    ['/reports/1', null, 'UNKNOWN'], ['/a~1b/~0key', 'ok', 'TRUE'], ['/empty', null, 'TRUE'],
    ['/empty', false, 'UNKNOWN'], ['/constructor', 'ok', 'UNKNOWN'], ['', value, 'TRUE'],
  ]) assert.equal(evaluatePredicate(value, { path, equals }), expected);
  assert.equal(evaluatePredicate(value, { path: '/text', contains: 'is ready' }), 'TRUE');
  assert.equal(evaluatePredicate(value, { path: '/text', contains: 'pending' }), 'FALSE');
  assert.equal(evaluatePredicate(value, { path: '/empty', contains: 'pending' }), 'UNKNOWN');
  assert.equal(evaluatePredicate(undefined, { path: '', equals: null }), 'UNKNOWN');
});

test('predicate validation enforces exactly one supported operation and a well-formed bounded pointer', () => {
  for (const predicate of [
    { path: '/a' }, { path: '/a', equals: 1, contains: 'a' }, { path: '', contains: '' },
    { path: '', contains: true }, { path: '', contains: 'é'.repeat(513) }, { path: '', equals: '0'.repeat(4_095) },
    { path: 'a', equals: null }, { path: '/bad~x', equals: true }, { path: '/x'.repeat(33), equals: true },
    { path: '/' + 'a'.repeat(512), equals: true }, { path: '', equals: true, mode: 'any' },
  ]) {
    assert.throws(() => validatePredicate(predicate), error('invalid_predicate'));
  }
  assert.deepEqual(validatePredicate({ path: '/', equals: null }), { path: '/', equals: null });
});

test('opted-in diff paths use sorted and escaped pointers and preserve array order', () => {
  const before = { group: ['blue', 'gold'], 'gone/field': { scalar: 1 }, '~': 4 };
  const after = { '~': 4, 'new/field': 'sample', group: ['gold', 'blue', false] };
  const publicPaths = new Set(['/gone~1field', '/group/0', '/group/1', '/group/2', '/new~1field']);
  const diff = diffEvidence(before, after, { publicPaths });
  assert.deepEqual(diff, {
    before_digest: digest(before), after_digest: digest(after), truncated: false,
    changes: [{ path: '/gone~1field', op: 'remove' }, { path: '/group/0', op: 'replace' },
      { path: '/group/1', op: 'replace' }, { path: '/group/2', op: 'add' }, { path: '/new~1field', op: 'add' }],
  });
  assert.doesNotMatch(JSON.stringify(diff), /blue|gold|sample/u);
  assert.deepEqual(diffEvidence({ a: 1, b: 2 }, { b: 2, a: 1 }).changes, []);
  assert.deepEqual(diffEvidence('old', { now: 'structured' }, { publicPaths: [''] }).changes, [{ path: '', op: 'replace' }]);
  assert.deepEqual(diffEvidence({ '~': 1 }, { '~': 2 }, { publicPaths: ['/~0'] }).changes, [{ path: '/~0', op: 'replace' }]);
});

test('default diffs conceal input values and dynamic keys, including descendants of an allowlisted pointer', () => {
  const before = { group: { 'fixture-dynamic-name': 'old sample' } };
  const after = { group: { 'fixture-dynamic-name': 'new sample' } };
  const changes = [{ path_digest: digest('/group/fixture-dynamic-name'), op: 'replace' }];
  assert.deepEqual(diffEvidence(before, after).changes, changes);
  assert.deepEqual(diffEvidence(before, after, { publicPaths: ['/group'] }).changes, changes);
  const event = sealEvent({ ...fields, kind: 'changed', evidence: { diff: diffEvidence(before, after) } });
  assert.doesNotMatch(JSON.stringify(event), /group|fixture-dynamic-name|old sample|new sample/u);
  assert.throws(() => diffEvidence(before, after, { publicPaths: ['bad pointer'] }), error('invalid_diff_options'));
  assert.throws(() => diffEvidence(before, after, { publicPaths: Array(65).fill('/group') }), error('invalid_diff_options'));
});

test('truncated path summaries retain complete endpoint digests', () => {
  const after = Array.from({ length: 33 }, (_, index) => index);
  const diff = diffEvidence([], after);
  assert.equal(diff.changes.length, 32);
  assert.equal(diff.truncated, true);
  assert.equal(diff.after_digest, digest(after));
  const longKey = 'a'.repeat(513);
  const long = diffEvidence({ [longKey]: 1 }, { [longKey]: 2 });
  assert.deepEqual(long.changes, [{ path_digest: digest('/' + longKey), op: 'replace' }]);
  assert.ok(sealEvent({ ...fields, evidence: { diff: long } }));
  const escapedPaths = Object.fromEntries(Array.from({ length: 25 }, (_, i) => [String.fromCharCode(1).repeat(250) + i, i]));
  const escapedSummary = diffEvidence({}, escapedPaths, { publicPaths: Object.keys(escapedPaths).map(key => '/' + key) });
  assert.ok(escapedSummary.truncated);
  assert.ok(escapedSummary.changes.length > 0 && escapedSummary.changes.length < 25);
  assert.ok(sealEvent({ ...fields, evidence: { diff: escapedSummary } }));
});

test('an event envelope binds identity, ordering, observation time, event kind, and evidence to its digest', () => {
  const event = sealEvent(fields);
  assert.equal(event.digest, eventDigest);
  assert.deepEqual(validateEvent(JSON.parse(JSON.stringify(event))), event);
  assert.deepEqual(sealEvent(Object.fromEntries(Object.entries(fields).reverse())), event);
  for (const update of [
    { version: 2 }, { event_id: 'new' }, { watch_id: 'other' }, { sequence: 2 }, { kind: 'changed' },
    { observed_at: 124 }, { evidence: { source_digest: digest('new') } }, { digest: digest('other') }, { extra: true },
  ]) assert.throws(() => validateEvent({ ...event, ...update }), error('invalid_event'));
  assert.ok(Object.isFrozen(event) && Object.isFrozen(event.evidence));
  assert.ok(Buffer.byteLength(JSON.stringify(event)) <= MAX_EVENT_BYTES);
});

test('envelope bounds and field allowlists apply before any consumer accepts evidence', () => {
  for (const override of [
    { event_id: 'a b' }, { watch_id: 'w'.repeat(129) }, { sequence: 0 }, { observed_at: -1 },
    { kind: 'invalid' }, { evidence: { sample: 'unregistered' } }, { evidence: { code: 'unknown detail' } },
    { evidence: { observation: { history_complete: true } } }, { evidence: { source_event_digest: '' } },
  ]) assert.throws(() => sealEvent({ ...fields, ...override }), error('invalid_event'));
  const evidence = {
    observation: { snapshot_digest: digest(false), history_count: 2, history_complete: false },
    diff: diffEvidence(false, true), source_event_digest: digest('fixture-event'),
  };
  const changed = sealEvent({ ...fields, kind: 'changed', evidence });
  evidence.diff.changes[0].op = 'sample';
  assert.equal(changed.evidence.diff.changes[0].op, 'replace');
  assert.deepEqual(validateEvent(changed), changed);
});
