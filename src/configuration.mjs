import { canonical, digest, validatePredicate } from './sdk.mjs';

const editable = ['interval_ms', 'deadline_ms', 'predicate', 'delivery'];
const configKeys = ['version', 'mode', 'source_id', 'consumer_id', ...editable];
const patchKeys = ['version', ...editable];
const slug = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const modes = new Set(['until', 'changes', 'emitted']);
const own = Object.hasOwn;

function invalid() {
  throw Object.assign(new TypeError('invalid_request'), { code: 'invalid_request' });
}

function checkKeys(obj, allowed) {
  if (obj === null || typeof obj !== 'object' || Array.isArray(obj)
      || Object.keys(obj).some(key => !allowed.includes(key))) invalid();
}

function versionedInput(raw, allowed) {
  let input;
  try {
    // Canonical encoding validates bounded, plain JSON without invoking user-supplied getters.
    input = JSON.parse(canonical(raw));
  } catch {
    invalid();
  }
  checkKeys(input, allowed);
  if (!own(input, 'version') || input.version !== 1) invalid();
  return input;
}

function interval(value) {
  if (!Number.isSafeInteger(value) || value < 20 || value > 86_400_000) invalid();
  return value;
}

function readEdits(input) {
  const result = {};
  if (own(input, 'interval_ms')) result.interval_ms = interval(input.interval_ms);
  if (own(input, 'deadline_ms')) {
    const deadline = input.deadline_ms;
    if (deadline !== null && (!Number.isSafeInteger(deadline) || deadline < 0)) invalid();
    result.deadline_ms = deadline;
  }
  if (own(input, 'delivery')) {
    checkKeys(input.delivery, ['enabled']);
    if (!own(input.delivery, 'enabled') || typeof input.delivery.enabled !== 'boolean') invalid();
    result.delivery = { enabled: input.delivery.enabled };
  }
  if (own(input, 'predicate')) {
    try {
      result.predicate = validatePredicate(input.predicate);
    } catch {
      invalid();
    }
  }
  return result;
}

function freeze(value) {
  if (value !== null && typeof value === 'object') {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
}

export function normalizeConfig(raw, defaultInterval = 1000) {
  const input = versionedInput(raw, configKeys);
  if (!own(input, 'mode') || !modes.has(input.mode) || own(input, 'predicate') !== (input.mode === 'until')) invalid();
  for (const field of ['source_id', 'consumer_id']) {
    if (!own(input, field) || typeof input[field] !== 'string' || !slug.test(input[field])) invalid();
  }
  return freeze({
    version: 1,
    mode: input.mode,
    source_id: input.source_id,
    consumer_id: input.consumer_id,
    interval_ms: interval(defaultInterval),
    deadline_ms: null,
    delivery: { enabled: true },
    ...readEdits(input),
  });
}

export function normalizePatch(raw) {
  const input = versionedInput(raw, patchKeys);
  if (!editable.some(key => own(input, key))) invalid();
  return freeze({ version: 1, ...readEdits(input) });
}

export function configSummary(raw) {
  const config = normalizeConfig(raw);
  const { predicate, ...summary } = config;
  if (config.mode === 'until') summary.predicate_digest = digest(predicate);
  summary.config_digest = digest(config);
  return freeze(summary);
}
