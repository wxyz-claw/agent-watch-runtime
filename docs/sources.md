# Source adapters

Register an adapter under a source ID in the operator's registry. Clients create watches against that ID. Every adapter returns the v1 observation described in the [watch contract](contract.md).

## Generated fixture

`syntheticSource(frames)` returns one frame per committed collection attempt. After the final frame, it repeats that frame. Reopening a watch resumes from its durable `checkpoint.reads`. This allows a fixture to recover from a failed read while the runtime preserves its last reliable snapshot.

```js
const sample = syntheticSource([
  {value: {phase: 'waiting'}},
  {coverage: 'failed', code: 'source_timeout'},
  {value: {phase: 'ready'}},
]);
```

Each frame can specify `coverage`, `authority` and an allowlisted diagnostic `code`. Emission fixtures supply an explicit `source_event_id`. Repeating an emission retains its identity. The adapter accepts 1–256 frames and up to 1 MiB of normalized fixture data.

## JSON file

`jsonSource(absolutePath, {timeoutMs})` reads one operator-selected regular UTF-8 JSON file. A document can contain any supported JSON value, including a scalar or null. Fields such as `authority` inside the document remain ordinary data. The evidence contains the byte count and SHA-256 of the file.

The raw input limit is 64 KiB. The SDK also enforces normalized size, depth and value-count bounds. The adapter rejects file links and directory links below the selected filesystem or system temporary root. A missing file, malformed document or file that changes size or metadata during the read produces an inconclusive observation. Writers can replace the fixture atomically by renaming a completed sibling file over it. The default read budget is two seconds.

## changedetection.io

`changedetectionSource({baseUrl, watchId, apiKey, maxAgeMs, timeoutMs})` samples a configured `text_json_diff` watch. Supply its UUID and the literal origin `http://127.0.0.1:<port>`. The optional API key travels in `x-api-key`. Feed the bridge an isolated fixture instance. Its requests use direct loopback GETs and never request a recheck or change an upstream setting.

For `watch_until`, select `/text` and use `contains` or `equals` on the latest **retained text snapshot**. A single retained snapshot can have `last_changed = 0`. The bridge obtains its timestamp from the history index, validates both its age and the last check's age, and compares health and index information before and after the snapshot read. The default maximum age is five minutes. Paused watches, failed upstream checks, stale snapshots, mismatched indexes and missing coverage yield `UNKNOWN`. A later healthy read can recover. More recent upstream checks alone do not refresh an older retained snapshot. [Watch API](https://changedetection.io/docs/api_v1/index.html) · [Upstream watch model](https://github.com/dgtlmoon/changedetection.io/blob/master/changedetectionio/model/Watch.py)

| GET path below `/api/v1/watch/{uuid}` | Use |
| --- | --- |
| Base path | Check watch identity, processor, health, last check and retained index metadata. |
| `/history` | Inspect available timestamp keys. The bridge ignores returned filesystem paths. |
| `/history/latest` | Read the current retained text for `watch_until`. |
| `/history/{timestamp}` | Read the pinned point for `watch_changes`. |
| `/difference/{from}/{to}?format=text` | Hash a supplementary diff when both observed timestamps remain available. |

The adapter caps metadata and index responses at 128 KiB, retained keys at 1,024 and snapshot/diff bodies at 64 KiB. Responses must match the expected media type. An escaped JSON representation can exceed the SDK's limit even when its original text fits. The read budget covers the whole sample. A supplementary diff gets up to one quarter of that budget, capped at 250 ms.

Snapshot evidence records the raw byte digest and length, retained count and `history_complete: false`. A successful upstream diff adds its digest and length. Pruned history or an unavailable diff produces `history_gap` while a verified current snapshot can still establish a change from the runtime's own baseline. A regressed timestamp keeps the old checkpoint and yields `UNKNOWN`. The bridge cannot count every intervening change or infer the state of a page between snapshots. [Upstream snapshot and diff handlers](https://github.com/dgtlmoon/changedetection.io/blob/master/changedetectionio/api/Watch.py)

## Verification

Run `node --test test/sources.test.mjs` from the repository root. The tests generate synthetic observations, temporary files and a loopback HTTP fixture with an ephemeral port and key. They exercise the v1 API paths, request authentication, text and JSON media types, coverage changes, stale data, retention, bounded responses and timeout handling. The paths and response semantics were checked against the official API v1 documentation (0.1.9) and its upstream implementation. Execution against an installed changedetection.io release is a separate compatibility check.
