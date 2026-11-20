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
| `get_watch` | `runtime.get(watch_id)` |
| `cancel_watch` | `runtime.cancel(watch_id)` |

```json
{"source_id":"sample","consumer_id":"worker","request_id":"first-run","predicate":{"path":"/phase","equals":"ready"}}
```

The result includes `watch_id`, `state`, `sequence` and `truth`. Watch states are `active`, `ready`, `canceled`, `deadline` and `error`. Request identity is scoped to the consumer. Repeating an identical create returns the original watch. Reusing that request identity with different semantics is a conflict. The stable consumer ID identifies a logical inbox across transport retries or changes in webhook configuration. Route and signing-key changes preserve the watch, baseline, event IDs and checkpoints.

## Events and persistence

The runtime assigns a stable watch ID and a per-watch sequence starting at 1. Within one SQLite transaction it commits the accepted checkpoint, immutable event bytes and the pending delivery row. An event carries exactly `version`, `event_id`, `watch_id`, `sequence`, `kind`, `observed_at`, `evidence` and `digest`. Kinds are `ready`, `changed`, `emitted`, `deadline`, `canceled` and `error`.

The digest is lowercase SHA-256 of UTF-8 canonical JSON for the other seven fields. The v1 canonical encoder accepts finite JSON values, emits compact ECMAScript JSON scalars, preserves array order and sorts object keys by UTF-16 code-unit order. Retry sends the original body bytes. Evidence carries digests and bounded structural changes. Raw snapshots, endpoint addresses, authentication material and source exception strings stay out of event diagnostics.

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

## Integration gates

Source conformance covers version, authority, coverage, cursor, stable occurrence identity and bounded evidence. Runtime conformance exercises initially ready, initially false, unknown, partial collection, restarts, cancel, deadline, atomic rollback and retry. Delivery conformance exercises lost acknowledgement, deduplication, key changes and durable asynchronous work. A work ledger records the implemented gates and remaining coverage.
