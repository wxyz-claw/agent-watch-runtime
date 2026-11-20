# Watch controls

MCP clients can preview, create and operate watches using registered source and consumer IDs. The push receiver remains responsible for accepting events. The controls return watch state, observation truth and delivery status.

## Create and preview

Pass a v1 configuration and a stable request ID to `create_watch`.

```json
{
  "request_id": "fixture-run",
  "config": {
    "version": 1,
    "mode": "until",
    "source_id": "json",
    "consumer_id": "worker",
    "interval_ms": 86400000,
    "deadline_ms": null,
    "predicate": {"path": "/phase", "equals": "ready"},
    "delivery": {"enabled": true}
  }
}
```

Modes are `until`, `changes` and `emitted`. Only `until` takes a predicate. The collection interval `interval_ms` accepts 20 through 86,400,000 milliseconds. Omitting it selects the runtime default, 1,000 ms in the CLI. `deadline_ms` is an absolute Unix timestamp in milliseconds, and null clears a deadline. Delivery is enabled by default. The source registry owns file locations, upstream addresses, callback routes and keys. The MCP configuration accepts IDs for those resources. The existing `watch_until`, `watch_changes` and `watch_emitted` tools remain available.

`preview_watch({config})` uses a source's explicitly registered, nonconsuming preview function. The result includes `basis`, `truth`, `coverage`, `authority`, a diagnostic `code` and a redacted configuration summary with `config_digest` and, for `until`, `predicate_digest`. A supported source returns a sample when a preview slot is available. Without a preview capability or an available slot, the basis is `unavailable` and the truth is `UNKNOWN`. Change and occurrence previews report observation coverage and `UNKNOWN` truth.

`preview_watch({config,watch_id})` evaluates the matching watch's last committed evidence. The proposed configuration must identify the same source, mode and consumer. A file can change after that evidence was collected. For example, an `until` preview of a committed waiting state can still yield `FALSE` while a fresh sample yields `TRUE`. A missing predicate path yields `UNKNOWN`. Both previews are read-only with respect to watch records, checkpoints, schedules and events. The summary redacts source bytes and expected predicate values.

## List and edit

`get_watch({watch_id})` includes the current `revision`, `config_version`, redacted `configuration`, `delivery_enabled` and the latest `manual_check`. Use `list_watches({limit,state,after})` to page through summaries in ascending watch ID order. Each field is optional. The default limit is 20 and the valid range is 1–50. Read `watches` from the result. When `has_more` is true, pass `next_cursor` back as the exclusive `after` cursor with the same filter.

All configuration edits, collection pauses and resumes take an `expected_revision` from an observed status. A `revision_conflict` asks the client to read current status before deciding whether its edit still applies.

| Tool | Input and effect |
| --- | --- |
| `pause_watch` | `{watch_id,expected_revision}` suspends collection. A paused watch retains its deadline and can expire. Already queued deliveries remain eligible. |
| `resume_watch` | `{watch_id,expected_revision}` makes the next polled observation due immediately, subject to collection slots. A push-only watch returns to active state and waits for an upstream occurrence. |
| `update_watch` | `{watch_id,expected_revision,patch}` applies a versioned patch. Mutable fields are `interval_ms`, `deadline_ms`, the predicate for an `until` watch and `delivery`. Mode, source ID and consumer ID identify the watch's observation contract. |

Pausing collection or changing its predicate, interval or deadline fences in-flight observation results and marks any queued or running manual check `superseded`. A predicate change resets truth to `UNKNOWN` until the new predicate is evaluated. A delivery-only edit preserves the observation in progress.

For example, `{"version":1,"delivery":{"enabled":false}}` holds new delivery claims for this watch. Collection continues. Existing and newly committed events retain their outbox rows, sequence and immutable bytes. An HTTP attempt that already held a lease can finish. Enabling delivery releases held work subject to retry deadlines, ordering and the consumer's route status. A delivery-only update can release a committed terminal event while the observation stays terminal. Other terminal edits, new manual checks, collection pauses and resumes return `watch_terminal`. Exhausted deliveries retain their dead-letter state.

## Check now

Call `check_now({watch_id,expected_revision,operation_seq})` to request one persistent collection operation. Start `operation_seq` at 1 and increase it by one for each new manual check on the watch. Keep the same sequence and revision when retrying that request. The response identifies the watch, operation, revision and current state. A recorded check can be `queued`, `running`, `completed` or `superseded`. The `manual_check` field of `get_watch({watch_id})` shows the latest operation.

The runtime keeps one durable manual-operation record per watch. Retrying the current operation returns its recorded status across a disconnect or process restart, including when its observation already ended the watch. On an active watch at the current revision, an older or skipped sequence yields `operation_conflict`. Requesting the next operation while one is queued or running yields `check_pending`. A fresh check on a paused watch returns `watch_paused`. Event-driven sources with no collection function return `unsupported_control` for a fresh check.

The public CLI test creates a generated JSON watch, distinguishes cached and sampled previews, pauses and edits with a revision check, and resumes collection. It reopens the MCP server, checks manual replay and releases a held terminal event to a fresh loopback receiver.

```sh
node --test test/control-cli.test.mjs
```
