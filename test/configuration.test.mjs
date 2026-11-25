import assert from 'node:assert/strict';
import { test } from 'node:test';
import { configSummary, normalizeConfig, normalizePatch } from '../src/configuration.mjs';
import { digest } from '../src/sdk.mjs';

const base = { version: 1, mode: 'changes', source_id: 'sample', consumer_id: 'worker' };
const until = { ...base, mode: 'until', predicate: { path: '/phase', equals: 'ready' } };
const invalid = { name: 'TypeError', code: 'invalid_request', message: 'invalid_request' };

test('full configurations normalize equivalent omissions, null deadlines, and explicit defaults', () => {
  const complete = { ...base, interval_ms: 1000, deadline_ms: null, delivery: { enabled: true } };
  assert.deepEqual(normalizeConfig(base), complete);
  assert.deepEqual(normalizeConfig({ ...base, deadline_ms: null }), complete);
  assert.deepEqual(normalizeConfig(complete), complete);
  assert.deepEqual(configSummary(base), configSummary(complete));
  const custom = normalizeConfig(base, 250);
  assert.equal(custom.interval_ms, 250);
  assert.equal(configSummary(custom).config_digest, digest(custom));
  assert.equal(normalizeConfig({ ...base, interval_ms: 30 }, 250).interval_ms, 30);
});

test('watch mode determines the required predicate and summaries bind its exact semantics', () => {
  const summary = configSummary(until);
  assert.deepEqual(summary, {
    ...base, mode: 'until', interval_ms: 1000, deadline_ms: null, delivery: { enabled: true },
    config_digest: '02a9cbf70effbfd9fbbb00d04774857f9698661120c9684987feaf6ece6c1de1',
    predicate_digest: 'c34f9b2c4bab1d49399c550993efa194b0f0f73861fb5f1d2c334ca39f5e6cb7',
  });
  assert.ok(!Object.hasOwn(summary, 'predicate'));
  assert.notEqual(configSummary({ ...until, delivery: { enabled: false } }).config_digest, summary.config_digest);
  const alias = configSummary({ ...until, predicate: { equals: 'ready', path: '/phase' } });
  assert.deepEqual(summary, alias);
  assert.notEqual(configSummary({ ...until, predicate: { path: '/phase', equals: 'waiting' } }).predicate_digest, summary.predicate_digest);
  assert.throws(() => normalizeConfig({ ...base, mode: 'until' }), invalid);
  for (const mode of ['changes', 'emitted']) {
    assert.throws(() => normalizeConfig({ ...until, mode }), invalid);
    const summary = configSummary({ ...base, mode });
    assert.ok(!Object.hasOwn(summary, 'predicate_digest'));
  }
});

test('patches preserve omission and distinguish explicitly enabling, disabling and clearing controls', () => {
  for (const edits of [
    { interval_ms: 20 }, { delivery: { enabled: false } }, { delivery: { enabled: true } },
    { deadline_ms: null }, { deadline_ms: 0 }, { predicate: until.predicate },
    { predicate: until.predicate, interval_ms: 750, deadline_ms: null },
  ]) {
    const patch = { version: 1, ...edits };
    assert.deepEqual(normalizePatch(patch), patch);
  }
  const prior = normalizeConfig({ ...until, deadline_ms: 5, delivery: { enabled: false } });
  assert.equal(normalizeConfig({ ...prior, ...normalizePatch({ version: 1, interval_ms: 500 }) }).deadline_ms, 5);
  assert.deepEqual(normalizeConfig({ ...prior, ...normalizePatch({ version: 1, deadline_ms: null }) }),
    { ...prior, deadline_ms: null });
  for (const patch of [{}, { version: 1 }, { version: 1, deadline_ms: undefined },
    { version: 1, interval_ms: undefined }, { version: 1, delivery: undefined }]) {
    assert.throws(() => normalizePatch(patch), invalid);
  }
});

test('source and consumer IDs use the existing runtime slug grammar', () => {
  for (const field of ['source_id', 'consumer_id']) {
    for (const value of ['x', 'A8-._b', 'x'.repeat(128)]) {
      assert.equal(normalizeConfig({ ...base, [field]: value })[field], value);
    }
    for (const value of ['', null, 5, 'x'.repeat(129), '-x', 'a:b', 'see text', 'é', 'x/next', 'x\n']) {
      assert.throws(() => normalizeConfig({ ...base, [field]: value }), invalid);
    }
  }
});

test('intervals and absolute deadlines admit exact integer boundaries only', () => {
  for (const value of [20, 21, 86_400_000]) {
    assert.equal(normalizeConfig(base, value).interval_ms, value);
    assert.equal(normalizePatch({ version: 1, interval_ms: value }).interval_ms, value);
  }
  for (const value of [19, 86_400_001, 40.5, Infinity, NaN, -1, '20', null]) {
    assert.throws(() => normalizeConfig(base, value), invalid);
    assert.throws(() => normalizeConfig({ ...base, interval_ms: value }), invalid);
    assert.throws(() => normalizePatch({ version: 1, interval_ms: value }), invalid);
  }
  for (const value of [null, 0, 20, Number.MAX_SAFE_INTEGER]) {
    assert.equal(normalizeConfig({ ...base, deadline_ms: value }).deadline_ms, value);
    assert.equal(normalizePatch({ version: 1, deadline_ms: value }).deadline_ms, value);
  }
  for (const value of [-1, 0.75, Number.MAX_SAFE_INTEGER + 1, Infinity, NaN, '100']) {
    assert.throws(() => normalizeConfig({ ...base, deadline_ms: value }), invalid);
    assert.throws(() => normalizePatch({ version: 1, deadline_ms: value }), invalid);
  }
});

test('closed records reject unknown fields, versions, and immutable identity edits', () => {
  for (const value of [null, [], 1, { ...base, version: '1' }, { ...base, mode: 'all' }, { ...base, version: 2 },
    { ...base, request_id: 'run' }, { ...base, extension: {} }, { ...until, predicate: { path: '/phase' } },
    { ...base, delivery: {} }, { ...base, delivery: { enabled: 1 } }, { ...base, delivery: { enabled: true, extra: 1 } }]) {
    assert.throws(() => normalizeConfig(value), invalid);
    assert.throws(() => configSummary(value), invalid);
  }
  for (const value of [null, [], { interval_ms: 200 }, { version: 2, deadline_ms: null },
    { version: 1, source_id: 'sample', interval_ms: 200 }, { version: 1, consumer_id: 'worker' },
    { version: 1, mode: 'changes' }, { version: 1, delivery: { enabled: true, extra: true } },
    { version: 1, predicate: null }, { version: 1, more: {} }]) assert.throws(() => normalizePatch(value), invalid);
});

test('plain data admission never invokes input accessors and diagnostics omit input text', () => {
  let reads = 0;
  const body = Object.defineProperty({ ...until }, 'predicate', {
    enumerable: true, get() { reads++; throw new Error('sample-input-text'); },
  });
  const instance = Object.assign(Object.create({ origin: 'sample-input-text' }), base);
  const inherited = Object.create(base);
  const extra = Object.assign({ ...base }, { [Symbol('sample-input-text')]: 'sample-input-text' });
  const hidden = Object.defineProperty({ ...base }, 'unknown', { value: 'sample-input-text' });
  const cyclic = {};
  cyclic.node = cyclic;
  for (const value of [body, instance, inherited, extra, hidden, { ...until, predicate: { path: '', equals: cyclic } }]) {
    assert.throws(() => normalizeConfig(value), invalid);
  }
  assert.equal(reads, 0);
  const summary = configSummary({ ...until, predicate: { path: '/sample-dynamic-path', contains: 'sample-input-text' } });
  assert.doesNotMatch(JSON.stringify(summary), /sample-dynamic-path|sample-input-text/u);
});

test('normalization severs aliases and freezes nested predicates, delivery and summaries', () => {
  const input = { ...until, predicate: { path: '/sample', equals: { stages: ['ready'] } }, delivery: { enabled: false } };
  const config = normalizeConfig(input);
  const patch = normalizePatch({ version: 1, predicate: input.predicate, delivery: input.delivery });
  const summary = configSummary(config);
  input.predicate.equals.stages.push('later');
  input.delivery.enabled = true;
  assert.deepEqual(config.predicate.equals.stages, ['ready']);
  assert.deepEqual(patch.predicate.equals.stages, ['ready']);
  assert.equal(config.delivery.enabled, false);
  assert.equal(patch.delivery.enabled, false);
  assert.equal(summary.delivery.enabled, false);
  for (const value of [config, patch]) {
    assert.ok(Object.isFrozen(value));
    assert.throws(() => { value.predicate.equals.stages.push('later'); }, TypeError);
    assert.throws(() => { value.delivery.enabled = true; }, TypeError);
  }
  assert.ok(Object.isFrozen(summary));
  assert.throws(() => { summary.delivery.enabled = true; }, TypeError);
});
