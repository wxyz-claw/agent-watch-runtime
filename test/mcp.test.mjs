import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { Readable, Writable } from 'node:stream';
import test from 'node:test';
import { serveStdio } from '../src/mcp.mjs';

const initParams = {
  protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: 'fixture', version: '1' },
};
function request(id, method, params) {
  return { jsonrpc: '2.0', id, method, params };
}
function call(id, name, args) {
  return request(id, 'tools/call', { name, arguments: args });
}
function initialize(params = initParams) {
  return [
    request(1, 'initialize', params),
    { jsonrpc: '2.0', method: 'notifications/initialized' },
  ];
}
function encode(messages) {
  return messages.map(message => JSON.stringify(message) + '\n').join('');
}
async function exchange(parts, runtime = {}) {
  let transcript = '';
  const output = new Writable({
    write(chunk, _encoding, done) { transcript += chunk.toString(); done(); },
  });
  await serveStdio(runtime, { input: Readable.from(parts), output });
  return transcript.trimEnd().split('\n').filter(Boolean).map(line => JSON.parse(line));
}

test('negotiates each supported protocol and offers the current supported version for another proposal', async () => {
  for (const [proposed, expected] of [
    ['2025-11-25', '2025-11-25'], ['2025-06-18', '2025-06-18'], ['fixture-version', '2025-11-25'],
  ]) {
    const [init] = await exchange([encode(initialize({ ...initParams, protocolVersion: proposed }))]);
    assert.equal(init.id, 1);
    assert.equal(init.result.protocolVersion, expected);
    assert.deepEqual(init.result.capabilities, { tools: {} });
    assert.equal(init.result.serverInfo.name, 'agent-watch-runtime');
  }
});

test('tools become available after negotiated initialization and a client notification', async () => {
  const messages = [
    request(0, 'tools/list'),
    request(1, 'initialize', { protocolVersion: '2025-11-25' }),
    ...initialize().slice(0, 1),
    request(2, 'tools/list'),
    ...initialize().slice(1),
    request(3, 'tools/list', { _meta: {} }),
    request(4, 'initialize', initParams),
    request(5, 'ping'),
    { jsonrpc: '2.0', method: 'notifications/fixture' },
    { jsonrpc: '2.0', id: 6, result: {} },
  ];
  const results = await exchange([encode(messages)]);
  assert.deepEqual(results.map(x => x.id), [0, 1, 1, 2, 3, 4, 5]);
  assert.deepEqual(results.slice(0, 4).map(x => x.error?.code), [-32002, -32602, undefined, -32002]);
  const list = results[4].result.tools;
  assert.deepEqual(list.map(x => x.name), ['watch_until', 'watch_changes', 'watch_emitted', 'get_watch', 'cancel_watch']);
  assert.ok(list.every(tool => tool.inputSchema.additionalProperties === false));
  assert.deepEqual(list[0].inputSchema.required, ['source_id', 'consumer_id', 'request_id', 'predicate']);
  assert.deepEqual(list[0].inputSchema.properties.predicate.oneOf, [{ required: ['equals'] }, { required: ['contains'] }]);
  assert.equal(results[5].error.code, -32600);
  assert.deepEqual(results[6].result, {});
});

test('valid calls dispatch to the matching runtime operation and return both MCP representations', async () => {
  const trace = [];
  const runtime = {
    async create(mode, args) { trace.push([mode, args]); return { watch_id: 'watch-1', state: 'active' }; },
    async get(id) { trace.push(['get', id]); return { watch_id: id, state: 'active' }; },
    cancel(id) { trace.push(['cancel', id]); return { watch_id: id, state: 'canceled' }; },
  };
  const args = { source_id: 'source-1', consumer_id: 'consumer-1', request_id: 'request-1', interval_ms: 20, deadline_ms: 0 };
  const until = { ...args, predicate: { path: '/value', equals: null } };
  const results = await exchange([encode([
    ...initialize(), call(2, 'watch_until', until), call(3, 'watch_changes', args),
    call(4, 'watch_emitted', args), call(5, 'get_watch', { watch_id: 'watch-1' }),
    call(6, 'cancel_watch', { watch_id: 'watch-1' }),
  ])], runtime);
  assert.deepEqual(trace, [['until', until], ['changes', args], ['emitted', args], ['get', 'watch-1'], ['cancel', 'watch-1']]);
  for (const reply of results.slice(1)) {
    assert.equal(reply.result.isError, false);
    assert.deepEqual(reply.result.structuredContent, JSON.parse(reply.result.content[0].text));
  }
});

test('malformed calls have protocol errors and invalid tool arguments never reach a source', async () => {
  const args = { source_id: 'source-1', consumer_id: 'consumer-1', request_id: 'request-1' };
  const cases = [
    ['watch_changes', { ...args, consumer_id: '/generated/input' }],
    ['watch_changes', { ...args, interval_ms: 19 }],
    ['watch_changes', { ...args, interval_ms: 86400001 }],
    ['watch_changes', { ...args, deadline_ms: 1.5 }],
    ['watch_changes', { ...args, deadline_ms: -1 }],
    ['watch_changes', { ...args, request_id: 'a'.repeat(129) }],
    ['watch_changes', { ...args, predicate: { path: '', equals: true } }],
    ['watch_until', args],
    ['watch_until', { ...args, predicate: { path: 'value', equals: true } }],
    ['watch_until', { ...args, predicate: { path: '', contains: '', equals: true } }],
    ['get_watch', { watch_id: 'watch-1', route: 'generated' }],
    ['cancel_watch', {}],
  ];
  const results = await exchange([encode([
    ...initialize(), call(2, 'generated-input-label', {}),
    request(3, 'tools/call', { name: 'get_watch', arguments: [] }), request(4, 'generated-method'),
    ...cases.map(([name, input], i) => call(i + 5, name, input)),
  ])]);
  assert.deepEqual(results.slice(1, 4).map(r => r.error.code), [-32602, -32602, -32601]);
  assert.ok(!JSON.stringify(results).includes('generated-input-label'));
  const invalid = results.slice(4);
  assert.equal(invalid.length, cases.length);
  assert.ok(invalid.every(r => r.result.isError && r.result.structuredContent.error === 'invalid_request'));
});

test('runtime failures yield only fixed diagnostic codes', async () => {
  const runtime = {
    get(id) {
      if (id === 'absent') return null;
      if (id === 'known') throw Object.assign(new Error('synthetic-private-detail'), { code: 'capacity' });
      throw new Error('synthetic-private-detail');
    },
  };
  const results = await exchange([encode([...initialize(),
    ...['absent', 'known', 'other'].map((id, i) => call(i + 2, 'get_watch', { watch_id: id })),
  ])], runtime);
  assert.deepEqual(results.slice(1).map(r => r.result.structuredContent.error), ['watch_not_found', 'capacity', 'store_error']);
  assert.ok(!JSON.stringify(results).includes('synthetic-private-detail'));
});

test('preserves split UTF-8 and rejects oversized, malformed and unterminated frames without losing the next request', async () => {
  const split = Buffer.from(encode([request('glyph-☀', 'ping')]));
  const middle = split.indexOf(Buffer.from('☀')) + 1;
  const replies = await exchange([
    split.subarray(0, middle), split.subarray(middle),
    Buffer.alloc(64000, 32), Buffer.alloc(1537, 32), Buffer.from('\n'),
    Buffer.from([0xff, 10]), Buffer.from('{}\n'),
    encode([request(9, 'ping')]), JSON.stringify(request(10, 'ping')),
  ]);
  assert.deepEqual(replies.map(r => r.id), ['glyph-☀', null, null, null, 9, null]);
  assert.deepEqual(replies.map(r => r.error?.code), [undefined, -32700, -32700, -32600, undefined, -32700]);
});

test('a blocked response stream applies backpressure to later tool calls', async () => {
  let release;
  let firstWrite;
  const blocked = new Promise(resolve => { firstWrite = resolve; });
  const output = new Writable({
    highWaterMark: 1,
    write(_data, _encoding, done) {
      if (release) return done();
      release = done;
      firstWrite();
    },
  });
  let calls = 0;
  const requests = Array.from({ length: 20 }, (_, i) => call(i + 2, 'get_watch', { watch_id: 'watch-1' }));
  const runtime = { get() { calls++; return { state: 'active' }; } };
  const serving = serveStdio(runtime, { input: Readable.from([encode([...initialize(), ...requests])]), output });
  await blocked;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(calls, 0);
  release();
  await serving;
  assert.equal(calls, 20);
});

test('stdio answers a subprocess client before EOF and closes on input EOF', { timeout: 5000 }, async t => {
  const moduleUrl = new URL('../src/mcp.mjs', import.meta.url).href;
  const entry = `
    import { serveStdio } from ${JSON.stringify(moduleUrl)};
    await serveStdio({ create(mode) { return { state: mode === 'until' ? 'ready' : 'active' }; } });
  `;
  const child = spawn(process.execPath, ['--input-type=module', '-e', entry], { env: {}, stdio: ['pipe', 'pipe', 'pipe'] });
  const exited = once(child, 'close');
  t.after(() => child.kill());
  const lines = createInterface({ input: child.stdout })[Symbol.asyncIterator]();
  child.stdin.write(encode([initialize()[0]]));
  const init = JSON.parse((await lines.next()).value);
  assert.equal(init.result.protocolVersion, initParams.protocolVersion);
  child.stdin.write(encode([initialize()[1], call('client-2', 'watch_until', {
    source_id: 'source-1', consumer_id: 'consumer-1', request_id: 'request-1',
    predicate: { path: '/state', contains: 'ready' },
  })]));
  const reply = JSON.parse((await lines.next()).value);
  assert.equal(reply.id, 'client-2');
  assert.deepEqual(reply.result.structuredContent, { state: 'ready' });
  child.stdin.end();
  assert.deepEqual(await exited, [0, null]);
  assert.equal((await lines.next()).done, true);
});
