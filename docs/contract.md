# Watch contract v1

This development interface connects a source, a persistent watch and a push consumer. Collection can poll a source or drain events supplied by an upstream system. A running worker schedules collection and webhook delivery without requiring a consumer to poll.

## Source SDK

Register a trusted source under an opaque `source_id`. Its async function receives `{checkpoint, signal, now, mode}`. Time is Unix milliseconds. The checkpoint includes the number of committed reads, `reads`, the last accepted source `cursor` or null, and the last reliable snapshot `value` when available. A source can honor the abort signal. It returns one versioned observation per call.

```json
{"version":1,"contract":"durable_ready","coverage":"complete","authority":"authoritative","cursor":"1","value":{"phase":"ready"}}
```

| Contract | Mode | Meaning |
| --- | --- | --- |
| `durable_ready` | `until` | Evaluate the current reliable snapshot immediately, including on creation. A match ends the watch with a `ready` event. |
| `observe_change` | `changes` | Baseline the first reliable snapshot silently. Emit `changed` for distinct later reliable snapshots, with bounded differences. |
| `source_emitted` | `emitted` | The source reports a discrete occurrence with a stable `source_event_id`. Deliver the first observed occurrence and deduplicate replays for this watch. |

Coverage is `complete`, `partial` or `failed` for the specific observation. Authority is `authoritative` or `advisory`. Partial, failed, advisory, timed-out and invalid observations yield `UNKNOWN` and preserve the last reliable snapshot and resume cursor. They can recover on a subsequent collection. Complete snapshot coverage applies to a point in time. History completeness requires a separate source guarantee. Polling can miss intermediate states.

A predicate has exactly one JSON Pointer `path` (empty selects the document) and one of `equals` (JSON) or `contains` (string). Missing paths and inapplicable operand types yield `UNKNOWN`. A known comparison can yield `FALSE`. An `until` watch ends on `TRUE`, explicit cancellation or its absolute deadline. The deadline has its own event kind. Status distinguishes collection health, observed truth, the watch lifecycle and delivery.

## Control interface

Use the MCP stdio tools or the equivalent JavaScript runtime methods. Create calls take `source_id`, `consumer_id` and `request_id`. The `watch_until` tool also requires `predicate`. Optional `deadline_ms` is an absolute Unix timestamp and `interval_ms` is the collection interval. Source addresses, file paths and webhook keys are operator configuration. Each watch has one source and one consumer.

| Tool | JavaScript call |
| --- | --- |
| `watch_until` | `await runtime.create('until', args)` |
| `watch_changes` | `await runtime.create('changes', args)` |
| `watch_emitted` | `await runtime.create('emitted', args)` |
| `create_watch` | `await runtime.createConfigured(config, request_id)` |
| `preview_watch` | `await runtime.preview(config, watch_id?)` |
| `list_watches` | `runtime.list({after, limit, state})` |
| `pause_watch` | `runtime.pause(watch_id, expected_revision)` |
| `resume_watch` | `runtime.resume(watch_id, expected_revision)` |
| `update_watch` | `runtime.update(watch_id, expected_revision, patch)` |
| `check_now` | `await runtime.checkNow(watch_id, expected_revision, operation_seq)` |
| `get_watch` | `runtime.get(watch_id)` |
| `cancel_watch` | `runtime.cancel(watch_id)` |
| `read_events` | `runtime.history(watch_id, {after_sequence, limit})` |

```json
{"source_id":"sample","consumer_id":"worker","request_id":"first-run","predicate":{"path":"/phase","equals":"ready"}}
```

The result includes `watch_id`, `state`, `sequence` and `truth`. Watch states are `active`, `paused`, `ready`, `canceled`, `deadline` and `error`. Request identity is scoped to the consumer. Repeating an identical create returns the original watch. Reusing that request identity with different semantics is a conflict. The stable consumer ID identifies a logical inbox across transport retries or changes in webhook configuration. Route and signing-key changes preserve the watch, baseline, event IDs and checkpoints.

The configuration SDK accepts v1 configs and patches and computes stable redacted summaries. Import `normalizeConfig`, `normalizePatch` and `configSummary` from `agent-watch-runtime/configuration`. A watch has a separate positive `revision`. A meaningful edit or lifecycle transition increments it. Ordinary collection, collection retries and delivery leave the revision stable. A repeated create matches the original creation configuration even after edits. [Control semantics and examples](controls.md)

IDs supplied by a control client use 1–128 ASCII letters, digits, dots, underscores or hyphens, starting with an alphanumeric character. The default interval is 1,000 ms and the supported interval range is 20 ms through 24 hours. Creation attempts the initial read immediately. If a source or collection slot is busy, the scheduler picks up the due watch. A deadline which has already passed ends the watch before another source read.

Register a `Map` of source IDs and a separate `Map` of consumer routes when constructing `new WatchRuntime({store,sources,routes})`. For an event-driven source, register a null reader, create an emitted watch, and pass each occurrence to `runtime.ingest(sourceId, watchId, observation)` from trusted adapter code. A polled occurrence reader can return a new continuation cursor for a previously seen matching occurrence. Conflicting content for an existing source occurrence yields `UNKNOWN` and preserves the checkpoint. Incoming source values have data authority only. Routing, code execution and handler configuration belong to the host.

Secondary history accepts an exclusive `after_sequence` (default 0) and `limit` (default 20, range 1–50). It returns `{events,next_sequence,has_more}` in per-watch order. Pages also have a 64 KiB size budget. Continue from the last returned sequence. Reading history leaves sender delivery state and receiver work state untouched. This stdio interface negotiates protocol revisions 2025-11-25 and 2025-06-18. [MCP stdio specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) · [MCP tools specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)

## Events and persistence

The SQLite runtime schema is v4. Opening a v1, v2 or v3 development store upgrades its configuration and retry metadata in one transaction. Each existing watch starts at revision 1 with delivery enabled. Its original creation signature, checkpoint, events and existing delivery deadlines retain their values. Use a separate development store when testing a newer runtime alongside an older one.

The runtime assigns a stable watch ID and a per-watch sequence starting at 1. Within one SQLite transaction it commits the accepted checkpoint, immutable event bytes and the pending delivery row. An event carries exactly `version`, `event_id`, `watch_id`, `sequence`, `kind`, `observed_at`, `evidence` and `digest`. Kinds are `ready`, `changed`, `emitted`, `deadline`, `canceled` and `error`.

The digest is lowercase SHA-256 of UTF-8 canonical JSON for the other seven fields. The v1 canonical encoder accepts finite JSON values, emits compact ECMAScript JSON scalars, preserves array order and sorts object keys by UTF-16 code-unit order. Retry sends the original body bytes. Evidence carries digests and bounded structural changes. Raw snapshots, endpoint addresses, authentication material and source exception strings stay out of event diagnostics.

For a cross-language vector, sealing `{"version":1,"event_id":"e","watch_id":"w","sequence":1,"kind":"ready","observed_at":123,"evidence":{}}` yields digest `e50978a483f11f7e559933719811536b70db4fe37838fdf42c3aefb131796314`. Validate the envelope with `validateEvent` from `agent-watch-runtime/sdk`.

The runtime uses `{path_digest,op}` in change evidence. A path digest is the digest of the JSON Pointer string. Operators using the SDK directly may expose exact known paths through `diffEvidence(before,after,{publicPaths})`. The summary holds up to 32 changes and 12 KiB of encoded entries. Snapshot values are limited to 64 KiB, 32 levels and 8,192 nodes. Envelopes are limited to 32 KiB.

## Webhook and inbox

The configured loopback callback receives `POST /inbox`. The sender emits these headers.

| Header | Value |
| --- | --- |
| `content-type` | `application/json` |
| `x-watch-consumer` | Stable consumer ID |
| `x-watch-key-id` | Operator-assigned lookup ID |
| `x-watch-timestamp` | Attempt time as decimal Unix milliseconds |
| `x-watch-signature` | Lowercase hex HMAC-SHA256 |

Sign the UTF-8 bytes of `agent-watch-webhook-v1\n<consumer>\n<keyId>\n<unixMs>\n` followed by the exact request body. Keys are generated independently for the configured receiver. Receiver authentication includes a freshness check and a constant-time signature comparison.

A 2xx response confirms that the receiver committed the inbox record and asynchronous work. Processing has its own state. An external effect requires its own idempotency or reconciliation protocol. A lost response causes redelivery with the same event ID and digest. The receiver deduplicates on `(consumer_id,event_id)` and checks the immutable digest. The sender persists attempts, backoff, leases and terminal delivery failures. The worker processes accepted events separately in per-watch sequence order. A failure on one watch leaves other watches eligible.

The receiver returns 202 for newly committed work, 200 for a matching duplicate and 409 for a conflicting ID, digest or sequence. It accepts a unique key ID header and signatures up to five minutes from its clock by default. To rotate keys, install the new receiver key, change the sender's route for the same consumer and retain the original inbox across callback changes. The consumer may accept both key IDs during the transition. A new, empty logical inbox needs its own consumer and watch.

Default sender retry policy is six attempts with exponential backoff from 250 ms to 60 seconds. The first sending attempt records the route's attempt and delay policy for the event. Restarts preserve that policy and its due time. For HTTP 429 or 503, a valid `Retry-After` in seconds or an HTTP date can increase the delay up to the configured ceiling. Invalid hints use the normal backoff. The optional `WatchStore` setting `retryJitterMs` accepts 0–60,000 ms and defaults to zero. On each retry it adds a reproducible offset between zero and that setting to the exponential delay, then applies the server hint as a floor and the configured ceiling. The offset is derived from the immutable event ID and attempt number. The first claim persists the jitter setting alongside the retry policy. `get_watch` exposes queue counts, the head event, attempt count, next retry, lease and first dead letter. An exhausted predecessor blocks delivery of later events on that watch.

HTTP 410 from the current destination pauses outstanding and future delivery to that consumer. Status exposes `delivery_paused` and a `paused` head with code `endpoint_gone`. The pause survives process restarts. Repair the registered route or, after repairing the same destination, call the operator SDK method `runtime.resumeDelivery(consumerId)`. Changed callback or signing configuration fences old attempts, clears the pause and gives pending events the new destination's attempt budget. A late 410 or 2xx from the retired destination cannot decide the new destination's state. Every worker for a consumer should resolve its current route from the same operator configuration. In-flight requests, retries, pauses and key changes preserve observation checkpoints and immutable event bytes.

Defaults bound a store to 512 watches and 1,024 events per watch, including a reserved terminal slot. The inbox retains up to 10,000 receipts and recognizes matching duplicates even at capacity. A fresh event at capacity receives 503. Completed jobs retain their deduplication receipts. An inbox worker defaults to five attempts and a 30-second lease. A handler receives the event and `{consumerId,attempt,signal}`.

## Integration gates

Source conformance covers version, authority, coverage, cursor, stable occurrence identity and bounded evidence. Runtime conformance exercises initially ready, initially false, unknown, partial collection, restarts, cancel, deadline, atomic rollback and retry. Delivery conformance exercises lost acknowledgement, deduplication, key changes and durable asynchronous work. A work ledger records the implemented gates and remaining coverage.
