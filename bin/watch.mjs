#!/usr/bin/env node
import { setTimeout as delay } from 'node:timers/promises';

const write = value => process.stdout.write(JSON.stringify(value) + '\n');

async function runReceiver(dev, signal, claimReceiver) {
  const {Inbox, createReceiver} = await import('../src/delivery.mjs');
  const registration = claimReceiver(dev);
  let inbox, server;
  try {
    inbox = new Inbox(dev.inboxDb);
    server = createReceiver({inbox, consumerId: dev.consumerId, keys: new Map([[dev.keyId, dev.key]])});
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen({host: '127.0.0.1', port: 0, exclusive: true}, resolve);
    });
    registration.publish(server.address().port);
    write({state: 'listening'});
    let nextRefresh = Date.now() + 1000;
    while (!signal.aborted) {
      if (Date.now() >= nextRefresh) {
        registration.refresh();
        nextRefresh = Date.now() + 1000;
      }
      // The synthetic handler has no external effect. A worker row reaches done
      // only after this callback resolves and the inbox commits completion.
      const item = await inbox.workOnce(async () => {}, {consumerId: dev.consumerId});
      if (item?.state === 'done') {
        write({state: 'done', watch_id: item.watch_id, event_id: item.event_id, sequence: item.sequence});
      }
      if (!item) await delay(50, undefined, {signal});
    }
  } catch (error) {
    if (!signal.aborted || error.name !== 'AbortError') throw error;
  } finally {
    try {
      if (server?.listening) await new Promise(resolve => server.close(resolve));
      inbox?.close();
    } finally {
      registration.close();
    }
  }
}

async function runProducer(command, dev, signal) {
  const [{WatchStore}, {WatchRuntime}, {serveStdio}] = await Promise.all([
    import('../src/store.mjs'), import('../src/runtime.mjs'), import('../src/mcp.mjs'),
  ]);
  const store = new WatchStore(dev.runtimeDb);
  let runtime;
  try {
    runtime = new WatchRuntime({store, sources: dev.sources, routes: dev.routes});
    runtime.start({tickMs: 50});
    if (command === 'serve') {
      const interrupt = () => process.stdin.destroy();
      signal.addEventListener('abort', interrupt, {once: true});
      try {
        if (!signal.aborted) await serveStdio(runtime);
      } finally {
        signal.removeEventListener('abort', interrupt);
      }
    } else {
      write({state: 'running'});
      while (!signal.aborted) await delay(60000, undefined, {signal});
    }
  } catch (error) {
    if (!signal.aborted) throw error;
  } finally {
    try { await runtime?.stop(); }
    finally { store.close(); }
  }
}

async function main() {
  const [command, directory, ...rest] = process.argv.slice(2);
  if (rest.length || !directory || !['init', 'serve', 'worker', 'receive'].includes(command)) {
    process.stderr.write('Usage: agent-watch <init|receive|serve|worker> <development-directory>\n');
    return 2;
  }
  const [major, minor] = process.versions.node.split('.').map(Number);
  if (!((major === 22 && minor >= 13) || major === 24 || major === 26)) {
    process.stderr.write('Use Node 22.13+, 24.x or 26.x with built-in node:sqlite.\n');
    return 2;
  }
  const {initDevelopment, openDevelopment, claimReceiver} = await import('../src/config.mjs');
  if (command === 'init') {
    initDevelopment(directory);
    write({state: 'initialized'});
    return 0;
  }
  const dev = openDevelopment(directory);
  const control = new AbortController();
  const stop = () => control.abort();
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  try {
    if (command === 'receive') await runReceiver(dev, control.signal, claimReceiver);
    else await runProducer(command, dev, control.signal);
    return 0;
  } finally {
    process.removeListener('SIGINT', stop);
    process.removeListener('SIGTERM', stop);
  }
}

try {
  process.exitCode = await main();
} catch (error) {
  const {setupFailure} = await import('../src/config.mjs');
  process.stderr.write(setupFailure(error) + '\n');
  process.exitCode = 1;
}
