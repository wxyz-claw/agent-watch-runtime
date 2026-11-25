import assert from 'node:assert/strict';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { initDevelopment, openDevelopment } from '../src/config.mjs';
import { changedetectionSource, jsonSource, syntheticSource } from '../src/sources.mjs';

const now = Date.UTC(2030, 5, 4);
const initial = Object.freeze({reads: 0, cursor: null});
const context = (mode, checkpoint = initial) => Object.freeze({mode, checkpoint, now});

async function directory(t) {
  const path = await mkdtemp(join(tmpdir(), 'watch-preview-'));
  t.after(() => rm(path, {recursive: true, force: true}));
  return path;
}

test('synthetic preview returns a detached emitted occurrence without consuming the checkpoint', async () => {
  const read = syntheticSource([
    {source_event_id: 'first', value: {count: 1}},
    {source_event_id: 'second', value: {count: 2}},
  ]);
  const start = context('emitted');
  const preview = await read.preview(start);
  assert.equal(preview.source_event_id, 'first');
  preview.value.count = 99;
  const actual = await read(start);
  assert.equal(actual.value.count, 1);
  assert.deepEqual(await read.preview(start), actual);
  assert.deepEqual(start.checkpoint, {reads: 0, cursor: null});
  const next = context('emitted', Object.freeze({reads: 1, cursor: actual.cursor}));
  assert.equal((await read.preview(next)).source_event_id, 'second');
  assert.deepEqual(await read(next), await read.preview(next));
});

test('JSON preview samples the latest bounded file without a cached result', async (t) => {
  const path = join(await directory(t), 'input.json');
  const read = jsonSource(path);
  const ctx = context('until');
  assert.equal((await read.preview(ctx)).code, 'source_unavailable');
  await writeFile(path, '{"ready":false}');
  const waiting = await read.preview(ctx);
  assert.equal(waiting.value.ready, false);
  await writeFile(path, '{"ready":true}');
  const ready = await read.preview(ctx);
  assert.equal(ready.value.ready, true);
  assert.notEqual(ready.cursor, waiting.cursor);
  assert.deepEqual(await read(ctx), ready);
  await writeFile(path, Buffer.alloc(65_537));
  assert.equal((await read.preview(ctx)).code, 'capacity');
});

test('direct and configured web previews use fresh GETs and leave lazy configuration untouched', async (t) => {
  const watchId = randomUUID();
  let key = randomBytes(24).toString('hex');
  let text = 'waiting';
  let stamp = (now / 1_000) - 1;
  const requests = [];
  const prefix = '/api/v1/watch/' + watchId;
  const server = createServer((req, res) => {
    requests.push([req.method, req.url]);
    if (req.method !== 'GET' || req.headers['x-api-key'] !== key) return res.writeHead(403).end();
    if (req.url === prefix) {
      res.writeHead(200, {'content-type': 'application/json'}).end(JSON.stringify({
        uuid: watchId, processor: 'text_json_diff', last_checked: now / 1_000,
        last_changed: 0, history_n: 1, last_error: false, paused: false,
      }));
    } else if (req.url === prefix + '/history') {
      res.writeHead(200, {'content-type': 'application/json'}).end(JSON.stringify({[stamp]: 'fixture'}));
    } else if (req.url === prefix + '/history/latest') {
      res.writeHead(200, {'content-type': 'text/plain'}).end(text);
    } else {
      res.writeHead(404).end();
    }
  });
  await new Promise((resolve, reject) => server.once('error', reject).listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const options = {baseUrl: 'http://127.0.0.1:' + server.address().port, watchId, apiKey: key};
  const direct = changedetectionSource(options);
  const path = join(await directory(t), 'instance');
  initDevelopment(path);
  const config = join(path, 'web-source.json');
  await writeFile(config, JSON.stringify({apiKey: key}));
  const wrapped = openDevelopment(path).sources.get('web');
  assert.equal(requests.length, 0);
  const ctx = context('until');
  const invalid = await wrapped.preview(ctx);
  assert.equal(invalid.code, 'source_invalid');
  assert.ok(!JSON.stringify(invalid).includes(key));
  assert.equal(requests.length, 0);

  await writeFile(config, JSON.stringify(options));
  const directObservation = await direct.preview(ctx);
  assert.deepEqual(directObservation.value, {text: 'waiting'});
  assert.deepEqual(await wrapped.preview(ctx), directObservation);
  key = randomBytes(24).toString('hex');
  await writeFile(config, JSON.stringify({...options, apiKey: key}));
  text = 'available';
  stamp++;
  const actual = await wrapped(ctx);
  assert.deepEqual(actual.value, {text: 'available'});
  assert.deepEqual(await wrapped.preview(ctx), actual);
  assert.ok(!JSON.stringify(actual).includes(key));
  const expected = ['', '/history', '/history/latest', '/history', ''].map(suffix => ['GET', prefix + suffix]);
  assert.deepEqual(requests, [...expected, ...expected, ...expected, ...expected]);
});
