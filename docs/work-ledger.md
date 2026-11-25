# Development and verification ledger

| Scope | Implemented in this candidate | Verification and boundaries |
| --- | --- | --- |
| P0 · Observation and identity | Three v1 observation contracts, authoritative coverage, three-valued predicates, consumer-scoped creation identity, per-watch sequencing and immutable event digests. | SDK vectors, independent digest vector, bounded evidence and source conformance tests. The typed SDK is available at `agent-watch-runtime/sdk`. |
| P0 · Independent failures | Separate budgets for source reads, delivery and job processing. Collection timeouts, advisory evidence and partial coverage preserve the accepted cursor. | Regression tests exercise a stalled source alongside a ready watch, transaction rollback and recovery at the next read. |
| P1 · Watches and store | MCP creation and initial evaluation, JSON fixtures, durable reads, SQLite transactions, cancel, deadline, capacity errors, stable event bytes and a transactional outbox. | Controlled clocks, restarts, leased claims, stale completions, source replay, changed retry defaults and the development database upgrade. |
| P1 · Configuration and watch controls | V1 SDK and MCP creation, sampled or checkpoint preview, paged list, revisioned pause/resume/update and durable manual checks. Absolute deadlines still apply during a pause. | Independent database connections, restart, stale revision and stale lease tests, plus an end-to-end stdio and loopback receiver check. See [watch controls](controls.md). |
| P1 · Push and receiver | Signed loopback HTTP, route-generation fencing, bounded backoff, persisted optional deterministic jitter and Retry-After, repairable 410 pause, visible dead letters, sender redrive and receiver retry or skip. Inbox receipt and worker job share an acceptance transaction. | Dropped 2xx after commit, current and retired 410 responses, callback and key rotation, failure before commit, restart, deduplication, independent workers and ordering. |
| P1 · Runnable application | Fresh isolated setup, stdio MCP process, detached sender and independent receiver with an asynchronous example handler. | The demo disconnects the MCP client before the source changes, then reopens both databases to verify acceptance and processing. |
| P2 · Changes and web source | JSON snapshot comparison, bounded diff evidence, retained text snapshot readiness, optional upstream diff. The CLI can register a web source through local operator configuration. | Generated loopback fixtures and actual changedetection.io 0.70.4 GET handlers using generated snapshots. Replay reproduced readiness and the upstream diff digest. See the [source verification notes](sources.md). |
| P3–P4 · Source-specific integrations | Host-defined source registration, occurrence ingestion and typed evidence form the public extension interface. | Provider-specific collectors, credential access and deployment-specific integrations belong in separate applications. Only generated sources run in this repository's tests. |
| P5 · Secondary history | Exclusive sequence cursors, bounded pages and read-only replay through MCP or JavaScript. | History tests verify ordering, byte bounds, repeatable pages and unchanged delivery bookkeeping. The signed webhook is the agent wake-up path. |

## Reproduce

```sh
npm ci --offline --ignore-scripts
npm run verify
npm run demo
```

`verify` runs syntax and privacy checks, probes native SQLite, runs the suite and installs the actual package tarball into a fresh local consumer. The package check exercises the installed executable, SDK exports and detached push demo. The CI workflow defines Linux jobs for Node 22.13.0, 24.x and 26.x and a tracked-file static check. Its `quality-gate` job checks each of the four named results for explicit success. The static checker scans tracked files or a source export, and verifies the installed package separately. Node 22.13.0 enables the built-in SQLite module without a command-line flag and can emit an experimental warning on stderr. [Node SQLite documentation](https://nodejs.org/api/sqlite.html)

Local verification on macOS ARM64 covered Node 22.13.0, 24.18.0 and 26.0.0. The optional upstream handler check uses a disposable Python environment and generated local watch history. Its dependencies are outside the Node runtime and package installation.

## Host and installation evidence

| Revision | Check | Observed result |
| --- | --- | --- |
| Public baseline `a0ca92dfb61d5e7c8637b853460923dcf082df92` | Anonymous Git fetch, then local packing and offline tarball installation | All 28 packaged files matched the pinned Git contents. |
| Same baseline | Codex CLI 0.162.1 app-server guest session, unrelated working directory, absolute Node, script and state paths | The host listed all six baseline tools through `mcpServerStatus/list`. It called `watch_until` and `get_watch` through `mcpServer/tool/call` and observed `FALSE` at sequence 0. The host and stdio server then exited. |
| Same baseline | Detached worker and loopback receiver after host exit | Updating the generated JSON fixture produced one signed acceptance and one completed job on its first attempt. |
| Lifecycle candidate | Installed package with a stdio protocol fixture in a separate working directory | The fixture discovered 13 tools and created a v1 watch. The native control suite also exercises pause, resume, update and manual checks across restart. |

## Remaining work

- Named-host coverage for the added lifecycle tools, authenticated model turns, automatic host wake and optional resource/subscription notifications remain unverified. Current delivery uses the configured webhook receiver.
- Multi-host transport, key provisioning across hosts, downstream external effects and operator-driven dead-letter reconciliation require separate deployment designs. The delivered transport is local HTTP with HMAC.
- Retention prunes settled sender history, finished watches and completed receipts, and keeps the hard limits as backpressure for undelivered work. See [dead letters and retention](contract.md#dead-letters-and-retention) and `test/retention.test.mjs`. Cross-watch ordering and global replay are outside the v1 contract.
- The aggregate workflow requires its first hosted run on the lifecycle revision. Its result checker has local tests covering successful, missing, skipped, canceled and failed jobs.
- Redrive keeps its deduplication guarantee without a shared replay horizon: the receiver never prunes the newest receipt of a watch, and the sender cannot advance past an unaccepted event. A sender-side skip is not offered, because the receiver would wait for the skipped sequence.
