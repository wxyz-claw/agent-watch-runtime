# Agent Watch Runtime

Create a watch over MCP, close the client and let a worker deliver the result. A watch can wait for an existing condition, compare successive observations or forward a discrete source event. The worker commits each event and its pending delivery together in SQLite. A signed webhook wakes an agent's durable inbox.

## Try it

Use Node 22.13 or newer in the 22.x line, Node 24.x or Node 26.x. The runtime and SDK use the Node standard library. The installation has zero dependencies.

```sh
npm ci --offline --ignore-scripts
npm run check:node
npm run demo
```

The demo creates a watch over MCP stdio and closes the client while the condition is false. A separate worker observes a generated JSON fixture becoming ready. The loopback receiver commits the event, runs its queued job and reports `"accepted":1,"processed":1`. Each run uses fresh state, a generated key and an OS-assigned port. [Run the processes separately](docs/setup.md)

## Choose a watch

| Tool | Use it when | First reliable observation |
| --- | --- | --- |
| `watch_until` | An agent needs to wake when a durable condition is true | Evaluate immediately. A match emits `ready` even when the condition already holds. |
| `watch_changes` | An agent needs observable changes in a JSON value or retained text | Save a baseline. Emit a bounded structural diff on the next distinct reliable observation. |
| `watch_emitted` | A source supplies occurrences with stable identities | Emit the occurrence. A replay with the same identity and content is deduplicated. |

Collection can be scheduled or driven by a trusted in-process upstream source. Incomplete evidence produces `UNKNOWN`. Delivery retries carry the immutable event ID and digest across process restarts and webhook configuration changes. Receiver acceptance, asynchronous processing and any downstream effect are separate operations. [Public contract and wire format](docs/contract.md)

## Integrate

Import `WatchStore`, `WatchRuntime` and `Inbox` from `agent-watch-runtime`. The `agent-watch-runtime/sdk` entry point supplies the contract validators, canonical digest, JSON comparison and TypeScript declarations. The source registry accepts operator-configured adapters. The changedetection.io bridge reads retained snapshots through its documented v1 API. [Source interfaces and limits](docs/sources.md)

```sh
npm run test:contract
npm test
npm run check:package
```

The package check builds the tarball, compares every member with the manifest, installs it offline into a fresh consumer project and runs the installed CLI and demo. [Implementation and verification ledger](docs/work-ledger.md)

The project uses the [MIT license](LICENSE).
