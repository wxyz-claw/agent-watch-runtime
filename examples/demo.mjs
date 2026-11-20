import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, rename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { openDevelopment } from '../src/config.mjs';
import { WatchStore } from '../src/store.mjs';
import { Inbox } from '../src/delivery.mjs';

const executable = fileURLToPath(new URL('../bin/watch.mjs', import.meta.url));
let stage = 'initialize';

function start(command, directory, children) {
  const child = spawn(process.execPath, [executable, command, directory], {env: {}, stdio: 'pipe'});
  const lines = createInterface({input: child.stdout, crlfDelay: Infinity})[Symbol.asyncIterator]();
  // Child stderr can include platform diagnostics. The demo reports fixed errors.
  child.stderr.resume();
  const exited = new Promise(resolve => {
    child.once('error', () => resolve(1));
    child.once('close', code => resolve(code ?? 1));
  });
  child.stdin.on('error', () => {});
  children.add(child);
  exited.then(() => children.delete(child));
  return {
    child, exited,
    send(frame) { child.stdin.write(JSON.stringify({jsonrpc: '2.0', ...frame}) + '\n'); },
    async read() {
      const {value, done} = await lines.next();
      if (done) throw Error('child_closed');
      return JSON.parse(value);
    },
  };
}

async function demo() {
  const temporary = await mkdtemp(join(tmpdir(), 'agent-watch-demo-'));
  const directory = join(temporary, 'state');
  const children = new Set();
  const stop = () => { for (const child of children) child.kill('SIGKILL'); };
  const timeout = setTimeout(stop, 15000);
  try {
    const init = start('init', directory, children);
    assert.equal((await init.read()).state, 'initialized');
    assert.equal(await init.exited, 0);

    stage = 'start_receiver';
    const receiver = start('receive', directory, children);
    assert.equal((await receiver.read()).state, 'listening');
    stage = 'create_watch';
    const client = start('serve', directory, children);
    client.send({id: 1, method: 'initialize', params: {
      protocolVersion: '2025-11-25', clientInfo: {name: 'fixture-client', version: '1'}, capabilities: {},
    }});
    assert.equal((await client.read()).result.protocolVersion, '2025-11-25');
    client.send({method: 'notifications/initialized'});
    client.send({id: 2, method: 'tools/call', params: {name: 'watch_until', arguments: {
      request_id: 'demo', source_id: 'json', consumer_id: 'worker', interval_ms: 40,
      predicate: {path: '/phase', equals: 'ready'},
    }}});
    const call = (await client.read()).result;
    assert.equal(call.isError, false);
    const before = call.structuredContent;
    assert.equal(before.state, 'active');
    assert.equal(before.truth, 'FALSE');
    assert.equal(before.sequence, 0);
    client.child.stdin.end();
    assert.equal(await client.exited, 0);

    // Collection continues after the MCP client disconnects. Introduce the
    // matching value only after its server process has exited.
    stage = 'resume_worker';
    const worker = start('worker', directory, children);
    assert.equal((await worker.read()).state, 'running');
    const dev = openDevelopment(directory);
    const update = join(directory, 'sample.next.json');
    await writeFile(update, '{"phase":"ready"}\n', {flag: 'wx', mode: 0o600});
    await rename(update, dev.fixture);
    stage = 'await_job';
    const completed = await receiver.read();
    assert.equal(completed.state, 'done');
    assert.equal(completed.watch_id, before.watch_id);
    assert.equal(completed.sequence, 1);
    stage = 'stop_workers';
    worker.child.kill('SIGTERM');
    assert.equal(await worker.exited, 0);
    receiver.child.kill('SIGTERM');
    assert.equal(await receiver.exited, 0);

    stage = 'verify_databases';
    const store = new WatchStore(dev.runtimeDb);
    try {
      const status = store.get(before.watch_id);
      assert.equal(status.state, 'ready');
      assert.deepEqual(status.delivery, {pending: 0, running: 0, accepted: 1, dead: 0});
    } finally { store.close(); }
    const inbox = new Inbox(dev.inboxDb);
    try {
      const receipt = inbox.status(dev.consumerId, completed.event_id);
      assert.equal(receipt.watch_id, before.watch_id);
      assert.equal(receipt.state, 'done');
      assert.equal(inbox.summary(dev.consumerId).done, 1);
    } finally { inbox.close(); }
    return {watch_id: before.watch_id, event_id: completed.event_id, sequence: 1,
      accepted: 1, processed: 1};
  } finally {
    clearTimeout(timeout);
    const closed = [...children].map(child => new Promise(resolve => child.once('close', resolve)));
    stop();
    await Promise.all(closed);
    await rm(temporary, {recursive: true, force: true});
  }
}

try {
  process.stdout.write(JSON.stringify(await demo()) + '\n');
} catch {
  process.stderr.write('demo_failed_at_' + stage + '\n');
  process.exitCode = 1;
}
