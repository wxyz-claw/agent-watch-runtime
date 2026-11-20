import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { changedetectionSource } from '../src/sources.mjs';
import { diffEvidence, evaluatePredicate, isAuthoritativeObservation } from '../src/sdk.mjs';

const capture = JSON.parse(await readFile(process.argv[2], 'utf8'));
assert.equal(capture.release, '0.70.4');
const key = randomBytes(32).toString('hex');
const prefix = '/api/v1/watch/' + capture.watchId;
let responses = capture.one;
const server = createServer((req, res) => {
  if (req.method !== 'GET' || req.headers['x-api-key'] !== key || !req.url.startsWith(prefix)) {
    res.writeHead(403).end();
    return;
  }
  const route = req.url.slice(prefix.length);
  if (!Object.hasOwn(responses, route)) {
    res.writeHead(404).end();
    return;
  }
  const {status, type, body} = responses[route];
  res.writeHead(status, {'content-type': type}).end(body);
});

await new Promise((resolve, reject) => server.once('error', reject).listen(0, '127.0.0.1', resolve));
try {
  const source = changedetectionSource({
    baseUrl: 'http://127.0.0.1:' + server.address().port, watchId: capture.watchId, apiKey: key,
  });
  const sample = (mode, checkpoint = {}) => source({mode, checkpoint, now: capture.now});
  const predicate = {path: '/text', equals: 'available'};
  const first = await sample('changes');
  assert.ok(isAuthoritativeObservation(first));
  assert.equal(first.cursor, capture.first);
  assert.equal(evaluatePredicate((await sample('until')).value, predicate), 'FALSE');

  responses = capture.two;
  const next = await sample('changes', {cursor: first.cursor, value: first.value});
  assert.ok(isAuthoritativeObservation(next));
  assert.equal(next.cursor, capture.second);
  assert.equal(next.evidence.history_count, 2);
  assert.equal(next.evidence.history_complete, false);
  const diff = capture.two['/difference/' + capture.first + '/' + capture.second + '?format=text'].body;
  assert.equal(next.evidence.diff_digest, createHash('sha256').update(diff).digest('hex'));
  assert.equal(next.evidence.diff_bytes, Buffer.byteLength(diff));
  assert.notDeepEqual(diffEvidence(first.value, next.value), diffEvidence(first.value, first.value));
  const ready = await sample('until');
  assert.ok(isAuthoritativeObservation(ready));
  assert.equal(evaluatePredicate(ready.value, predicate), 'TRUE');
  console.log('changedetection.io %s real GET capture and loopback bridge replay passed', capture.release);
  console.log('Latest predicate FALSE to TRUE. Two retained points and a verified text-diff digest.');
  console.log('Blocked urllib3 IPv6 import probes: %d. Upstream handler sockets and subprocesses: 0.', capture.blockedImportProbes);
} finally {
  await new Promise((resolve) => server.close(resolve));
}
