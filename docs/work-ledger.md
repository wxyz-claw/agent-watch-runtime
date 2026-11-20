# Development and verification ledger

| Scope | Implemented in this candidate | Verification and boundaries |
| --- | --- | --- |
| P0 · Observation and identity | Three v1 observation contracts, authoritative coverage, three-valued predicates, consumer-scoped creation identity, per-watch sequencing and immutable event digests. | SDK vectors, independent digest vector, bounded evidence and source conformance tests. The typed SDK is available at `agent-watch-runtime/sdk`. |
| P0 · Independent failures | Separate budgets for source reads, delivery and job processing. Collection timeouts, advisory evidence and partial coverage preserve the accepted cursor. | Regression tests exercise a stalled source alongside a ready watch, transaction rollback and recovery at the next read. |
| P1 · Watches and store | MCP creation and initial evaluation, JSON fixtures, durable reads, SQLite transactions, cancel, deadline, capacity errors, stable event bytes and a transactional outbox. | Controlled clocks, restarts, leased claims, stale completions, source replay, changed retry defaults and the development database upgrade. |
| P1 · Push and receiver | Signed loopback HTTP, route-generation fencing, bounded backoff and Retry-After, repairable 410 pause and visible dead letters. Inbox receipt and worker job share an acceptance transaction. | Dropped 2xx after commit, current and retired 410 responses, callback and key rotation, failure before commit, restart, deduplication, independent workers and ordering. |
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

`verify` probes native SQLite, runs the suite and installs the actual package tarball into a fresh local consumer. The package check exercises the installed executable, SDK exports and detached push demo. The CI workflow runs these checks on Linux for Node 22.13.0, 24.x and 26.x. Node 22.13.0 enables the built-in SQLite module without a command-line flag and can emit an experimental warning on stderr. [Node SQLite documentation](https://nodejs.org/api/sqlite.html)

Local verification on macOS ARM64 covered Node 22.13.0, 24.18.0 and 26.0.0. The optional upstream handler check uses a disposable Python environment and generated local watch history. Its dependencies are outside the Node runtime and package installation.

## Remaining work

- An MCP client compatibility matrix for optional resource/subscription notifications remains open. Current clients need stdio tools and the configured webhook receiver.
- Multi-host transport, key provisioning across hosts, downstream external effects and operator-driven dead-letter reconciliation require separate deployment designs. The delivered transport is local HTTP with HMAC.
- Data retention currently uses visible hard limits. Automatic receipt pruning would require a replay horizon agreed by sender and receiver. Cross-watch ordering and global replay are outside the v1 contract.
- Hosted CI results require a published repository. Local package and protocol checks can be reproduced from the candidate alone.
