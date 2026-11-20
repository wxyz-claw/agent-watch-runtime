import { pipeline } from 'node:stream/promises';
import { canonical, validatePredicate } from './sdk.mjs';

const versions = ['2025-11-25', '2025-06-18'];
const maxFrameBytes = 64 * 1024;
const identifier = { type: 'string', minLength: 1, maxLength: 128, pattern: '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$' };
const idPattern = new RegExp(identifier.pattern);
const fixedErrors = new Set([
  'invalid_request', 'source_not_found', 'consumer_not_found', 'watch_not_found',
  'request_conflict', 'capacity', 'store_error',
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
const definitions = [
  { name: 'watch_until', description: 'Observe a registered source until an authoritative snapshot satisfies the predicate.', inputSchema: untilSchema },
  { name: 'watch_changes', description: 'Compare reliable snapshots and deliver observed changes.', inputSchema: createSchema },
  { name: 'watch_emitted', description: 'Deliver source-emitted events with durable source identity.', inputSchema: createSchema },
  { name: 'get_watch', description: 'Read watch status and delivery diagnostics.', inputSchema: existingSchema },
  { name: 'cancel_watch', description: 'Cancel an active watch.', inputSchema: existingSchema },
  { name: 'read_events', description: 'Read a page of committed events in per-watch sequence order.', inputSchema: historySchema },
];
const byName = new Map(definitions.map(tool => [tool.name, tool]));
const modes = new Map([['watch_until', 'until'], ['watch_changes', 'changes'], ['watch_emitted', 'emitted']]);

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
  }
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
    validated = argumentsFor(tool, args);
  } catch {
    return toolResult({ error: 'invalid_request' }, true);
  }
  try {
    let status;
    if (tool.name === 'get_watch') status = await runtime.get(validated.watch_id);
    else if (tool.name === 'cancel_watch') status = await runtime.cancel(validated.watch_id);
    else if (tool.name === 'read_events') {
      const { watch_id, after_sequence = 0, limit = 20 } = validated;
      status = await runtime.history(watch_id, { after_sequence, limit });
    } else status = await runtime.create(modes.get(tool.name), validated);
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
