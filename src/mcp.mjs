import { pipeline } from 'node:stream/promises';
import { normalizeConfig, normalizePatch } from './configuration.mjs';
import { canonical, validatePredicate } from './sdk.mjs';

const versions = ['2025-11-25', '2025-06-18'];
const maxFrameBytes = 64 * 1024;
const identifier = { type: 'string', minLength: 1, maxLength: 128, pattern: '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' };
const idPattern = new RegExp(identifier.pattern);
const fixedErrors = new Set([
  'invalid_request', 'source_not_found', 'consumer_not_found', 'watch_not_found',
  'request_conflict', 'capacity', 'store_error', 'revision_conflict', 'watch_terminal',
  'watch_paused', 'operation_conflict', 'check_pending', 'unsupported_control', 'not_dead_letter',
]);
const createProperties = {
  source_id: { ...identifier, description: 'Registered source identifier.' },
  consumer_id: { ...identifier, description: 'Registered delivery consumer.' },
  request_id: { ...identifier, description: 'Stable creation request identifier. Reuse when retrying.' },
  deadline_ms: { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER, description: 'Absolute Unix time in milliseconds.' },
  interval_ms: { type: 'integer', minimum: 20, maximum: 86400000, description: 'Collection interval in milliseconds.' },
};
const predicateSchema = {
  type: 'object',
  properties: {
    path: { type: 'string', maxLength: 512, description: 'JSON Pointer up to 32 tokens and 512 UTF-8 bytes. An empty string selects the root.' },
    equals: { description: 'JSON value to compare, up to 4096 encoded bytes.' },
    contains: { type: 'string', minLength: 1, maxLength: 1024, description: 'Substring of up to 1024 UTF-8 bytes to match in a string value.' },
  },
  required: ['path'],
  oneOf: [{ required: ['equals'] }, { required: ['contains'] }],
  additionalProperties: false,
};

function objectSchema(properties, required) {
  return { type: 'object', properties, required, additionalProperties: false };
}

const createFields = ['source_id', 'consumer_id', 'request_id'];
const createSchema = objectSchema(createProperties, createFields);
const untilSchema = objectSchema({ ...createProperties, predicate: predicateSchema }, [...createFields, 'predicate']);
const existingSchema = objectSchema({ watch_id: identifier }, ['watch_id']);
const historySchema = objectSchema({
  watch_id: identifier,
  after_sequence: { type: 'integer', minimum: 0, maximum: Number.MAX_SAFE_INTEGER, default: 0, description: 'Last sequence already read.' },
  limit: { type: 'integer', minimum: 1, maximum: 50, default: 20, description: 'Maximum events in this page.' },
}, ['watch_id']);

const configVersion = { type: 'integer', const: 1 };
const editable = {
  interval_ms: createProperties.interval_ms,
  deadline_ms: { ...createProperties.deadline_ms, type: ['integer', 'null'], description: 'Absolute Unix time in milliseconds, or null for no deadline.' },
  predicate: predicateSchema,
  delivery: objectSchema({ enabled: { type: 'boolean' } }, ['enabled']),
};
const configSchema = {
  ...objectSchema({
    version: configVersion, mode: { type: 'string', enum: ['until', 'changes', 'emitted'] },
    source_id: createProperties.source_id, consumer_id: createProperties.consumer_id, ...editable,
  }, ['version', 'mode', 'source_id', 'consumer_id']),
  if: { properties: { mode: { const: 'until' } }, required: ['mode'] },
  then: { required: ['predicate'] },
  else: { not: { required: ['predicate'] } },
};
const patchSchema = {
  ...objectSchema({ version: configVersion, ...editable }, ['version']),
  minProperties: 2,
};
const revisionFields = {
  watch_id: identifier,
  expected_revision: { type: 'integer', minimum: 1, maximum: Number.MAX_SAFE_INTEGER, description: 'Revision read from watch status.' },
};
const revisionSchema = objectSchema(revisionFields, ['watch_id', 'expected_revision']);
const listSchema = objectSchema({
  after: { ...identifier, description: 'Exclusive watch ID cursor from the previous page.' },
  limit: { type: 'integer', minimum: 1, maximum: 50, default: 20 },
  state: { type: 'string', enum: ['active', 'paused', 'ready', 'deadline', 'error', 'canceled'] },
}, []);

function tool(name, description, inputSchema, run) {
  return { definition: { name, description, inputSchema }, run };
}

const tools = [
  tool('watch_until', 'Observe a registered source until an authoritative snapshot satisfies the predicate.', untilSchema,
    (runtime, args) => runtime.create('until', args)),
  tool('watch_changes', 'Compare reliable snapshots and deliver observed changes.', createSchema,
    (runtime, args) => runtime.create('changes', args)),
  tool('watch_emitted', 'Deliver source-emitted events with durable source identity.', createSchema,
    (runtime, args) => runtime.create('emitted', args)),
  tool('get_watch', 'Read watch status and delivery diagnostics.', existingSchema,
    (runtime, { watch_id }) => runtime.get(watch_id)),
  tool('cancel_watch', 'Cancel an active or paused watch.', existingSchema,
    (runtime, { watch_id }) => runtime.cancel(watch_id)),
  tool('read_events', 'Read a page of committed events in per-watch sequence order.', historySchema,
    (runtime, { watch_id, after_sequence = 0, limit = 20 }) => runtime.history(watch_id, { after_sequence, limit })),
  tool('create_watch', 'Create a watch from a versioned configuration using a stable request ID.',
    objectSchema({ config: configSchema, request_id: createProperties.request_id }, ['config', 'request_id']),
    (runtime, { config, request_id }) => runtime.createConfigured(config, request_id)),
  tool('preview_watch', 'Preview the proposed configuration for a new or existing watch.',
    objectSchema({ config: configSchema, watch_id: identifier }, ['config']),
    (runtime, { config, watch_id }) => runtime.preview(config, watch_id)),
  tool('list_watches', 'List watch status in ascending watch ID order, optionally filtered by state.', listSchema,
    (runtime, args) => runtime.list(args)),
  tool('pause_watch', 'Pause collection at the expected watch revision.', revisionSchema,
    (runtime, { watch_id, expected_revision }) => runtime.pause(watch_id, expected_revision)),
  tool('resume_watch', 'Resume collection at the expected watch revision.', revisionSchema,
    (runtime, { watch_id, expected_revision }) => runtime.resume(watch_id, expected_revision)),
  tool('update_watch', 'Apply a versioned edit to a watch at its expected revision.',
    objectSchema({ ...revisionFields, patch: patchSchema }, ['watch_id', 'expected_revision', 'patch']),
    (runtime, { watch_id, expected_revision, patch }) => runtime.update(watch_id, expected_revision, patch)),
  tool('check_now', 'Request one immediate check. Reuse the operation sequence when retrying that request.',
    objectSchema({ ...revisionFields, operation_seq: { type: 'integer', minimum: 1, maximum: Number.MAX_SAFE_INTEGER } },
      ['watch_id', 'expected_revision', 'operation_seq']),
    (runtime, { watch_id, expected_revision, operation_seq }) => runtime.checkNow(watch_id, expected_revision, operation_seq)),
  tool('redrive_delivery', 'Requeue exhausted deliveries on a watch with a fresh attempt budget. Omit event_id to requeue every dead letter.',
    objectSchema({ watch_id: identifier, event_id: { ...identifier, description: 'One dead-letter event ID from get_watch.' } }, ['watch_id']),
    (runtime, { watch_id, event_id = null }) => runtime.redrive(watch_id, { event_id })),
];
const definitions = tools.map(tool => tool.definition);
const byName = new Map(tools.map(tool => [tool.definition.name, tool]));

function isObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function closedObject(value, keys) {
  return isObject(value) && Object.keys(value).every(key => keys.includes(key));
}

function safeInteger(value, min, max) {
  return Number.isSafeInteger(value) && value >= min && value <= max;
}

// Each published schema and the corresponding runtime decoder share the same
// required fields and ranges. The SDK owns JSON Pointer and JSON value limits.
function argumentsFor(tool, args) {
  const { properties, required } = tool.inputSchema;
  if (!closedObject(args, Object.keys(properties)) || !required.every(key => Object.hasOwn(args, key))) {
    throw new Error('invalid_request');
  }
  for (const [key, value] of Object.entries(args)) {
    const field = properties[key];
    if (field.pattern === identifier.pattern && (typeof value !== 'string' || !idPattern.test(value))) {
      throw new Error('invalid_request');
    }
    if (field.type === 'integer' && !safeInteger(value, field.minimum, field.maximum)) {
      throw new Error('invalid_request');
    }
    if (field.enum && !field.enum.includes(value)) throw new Error('invalid_request');
  }
  // Defaults come from the operator's runtime. Validate the caller's nested
  // data here and pass its original set of fields to the operation.
  if (Object.hasOwn(args, 'config')) normalizeConfig(args.config);
  if (Object.hasOwn(args, 'patch')) normalizePatch(args.patch);
  if (Object.hasOwn(args, 'predicate')) return { ...args, predicate: validatePredicate(args.predicate) };
  return args;
}

function errorResponse(id, code, message) {
  return { jsonrpc: '2.0', id, error: { code, message } };
}

function toolResult(data, isError = false) {
  const text = canonical(data);
  return { content: [{ type: 'text', text }], structuredContent: JSON.parse(text), isError };
}

async function callTool(runtime, tool, args) {
  let validated;
  try {
    validated = argumentsFor(tool.definition, args);
  } catch {
    return toolResult({ error: 'invalid_request' }, true);
  }
  try {
    const status = await tool.run(runtime, validated);
    if (status === null || status === undefined) return toolResult({ error: 'watch_not_found' }, true);
    if (!isObject(status)) return toolResult({ error: 'store_error' }, true);
    return toolResult(status);
  } catch (err) {
    const code = fixedErrors.has(err?.code) ? err.code : err?.message;
    return toolResult({ error: fixedErrors.has(code) ? code : 'store_error' }, true);
  }
}

function validRpcId(id) {
  return Number.isSafeInteger(id) || (typeof id === 'string' && Buffer.byteLength(id) <= 128);
}

function validParams(params) {
  return params === undefined || (isObject(params) && (!Object.hasOwn(params, '_meta') || isObject(params._meta)));
}

function validInit(params) {
  return isObject(params) && typeof params.protocolVersion === 'string' && params.protocolVersion.length > 0
    && isObject(params.capabilities) && isObject(params.clientInfo)
    && typeof params.clientInfo.name === 'string' && typeof params.clientInfo.version === 'string';
}

async function respond(runtime, session, message) {
  if (!isObject(message) || message.jsonrpc !== '2.0') return errorResponse(null, -32600, 'Invalid request');
  if (!Object.hasOwn(message, 'method') && (Object.hasOwn(message, 'result') || Object.hasOwn(message, 'error'))) {
    return null;
  }
  if (typeof message.method !== 'string') return errorResponse(null, -32600, 'Invalid request');
  const { id, method, params } = message;
  const notification = !Object.hasOwn(message, 'id');
  if (notification) {
    if (method === 'notifications/initialized' && session.phase === 'negotiated' && validParams(params)) {
      session.phase = 'ready';
    }
    return null;
  }
  if (!validRpcId(id)) return errorResponse(null, -32600, 'Invalid request');
  if (!validParams(params)) return errorResponse(id, -32602, 'Invalid params');
  const result = value => ({ jsonrpc: '2.0', id, result: value });
  if (method === 'ping') return result({});
  if (method === 'initialize') {
    if (session.phase !== 'new') return errorResponse(id, -32600, 'Session already initialized');
    if (!validInit(params)) return errorResponse(id, -32602, 'Invalid initialization parameters');
    session.phase = 'negotiated';
    return result({
      protocolVersion: versions.includes(params.protocolVersion) ? params.protocolVersion : versions[0],
      capabilities: { tools: {} },
      serverInfo: { name: 'agent-watch-runtime', version: '0.1.0' },
    });
  }
  if (session.phase !== 'ready') return errorResponse(id, -32002, 'Session initialization required');
  if (method === 'tools/list') {
    if (!closedObject(params ?? {}, ['_meta'])) return errorResponse(id, -32602, 'Invalid listing parameters');
    return result({ tools: definitions });
  }
  if (method !== 'tools/call') return errorResponse(id, -32601, 'Method not found');
  if (!closedObject(params, ['name', 'arguments', '_meta']) || typeof params.name !== 'string' ||
      (Object.hasOwn(params, 'arguments') && !isObject(params.arguments))) {
    return errorResponse(id, -32602, 'Invalid tool call');
  }
  const tool = byName.get(params.name);
  if (!tool) return errorResponse(id, -32602, 'Unknown tool');
  return result(await callTool(runtime, tool, params.arguments ?? {}));
}

// Keep one bounded frame while discarding an overlong line through its LF.
// Return an invalid frame at EOF if the peer omitted its terminating LF.
async function* frames(input) {
  const frame = Buffer.alloc(maxFrameBytes);
  let used = 0;
  let overflow = false;
  for await (const part of input) {
    const chunk = Buffer.isBuffer(part) ? part : Buffer.from(part);
    let start = 0;
    while (start < chunk.length) {
      const newline = chunk.indexOf(10, start);
      const end = newline === -1 ? chunk.length : newline;
      if (end - start > frame.length - used) overflow = true;
      if (!overflow) used += chunk.copy(frame, used, start, end);
      if (newline === -1) break;
      yield overflow ? null : frame.subarray(0, used);
      used = 0;
      overflow = false;
      start = end + 1;
    }
  }
  if (used || overflow) yield null;
}

export async function serveStdio(runtime, { input = process.stdin, output = process.stdout } = {}) {
  const session = { phase: 'new' };
  const decoder = new TextDecoder('utf-8', { fatal: true });
  // Stream backpressure bounds pending input and output, including when the
  // peer stops reading while a source observation is in flight.
  await pipeline(input, async function* (source) {
    for await (const frame of frames(source)) {
      let response;
      let request;
      try {
        if (frame === null) throw new Error();
        request = JSON.parse(decoder.decode(frame));
      } catch {
        response = errorResponse(null, -32700, 'Invalid JSON frame');
      }
      if (!response) response = await respond(runtime, session, request);
      if (response) yield JSON.stringify(response) + '\n';
    }
  }, output, { end: false });
}
