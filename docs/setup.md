# Run the local development system

The package uses the Node.js standard library, including `node:sqlite`. From the repository root, run the end-to-end demo with a supported Node binary on your PATH.

```sh
npm ci --offline --ignore-scripts
npm run demo
```

The demo creates a fresh temporary directory, starts the loopback receiver and calls `watch_until` on a generated JSON fixture over MCP stdio. It closes the MCP connection and exits that server process. A headless worker reopens the watch database, observes the updated fixture and signs the pending event. The receiver commits the inbox item and a queued job. Its separate worker completes a local no-op handler. The demo reopens both databases, checks one accepted delivery and one completed job, prints their IDs and counts, and removes its temporary directory.

## Install a pinned public checkout

Set `WATCH_PUBLIC_REPO` to the public Git clone URL and `WATCH_PUBLIC_COMMIT` to a full, lowercase 40-hex commit ID you have independently verified. For the versioned controls, select a commit containing `docs/controls.md`. Use a new writable absolute directory in place of `/srv/watch-lab` throughout these examples. The checkout, tarball, consumer project and runtime state each have their own location.

```sh
(
  set -eu
  : "${WATCH_PUBLIC_REPO:?Set the public Git clone URL}"
  : "${WATCH_PUBLIC_COMMIT:?Set the verified full commit ID}"
  test "${#WATCH_PUBLIC_COMMIT}" -eq 40
  lab=/srv/watch-lab
  mkdir "$lab"
  git clone --no-checkout -- "$WATCH_PUBLIC_REPO" "$lab/source"
  pinned=$(git -C "$lab/source" rev-parse --verify "${WATCH_PUBLIC_COMMIT}^{commit}")
  test "$pinned" = "$WATCH_PUBLIC_COMMIT"
  git -C "$lab/source" checkout --detach "$pinned"
  test "$(git -C "$lab/source" rev-parse HEAD)" = "$WATCH_PUBLIC_COMMIT"

  mkdir "$lab/artifacts" "$lab/consumer"
  cd "$lab/source"
  archive=$(npm pack --ignore-scripts --silent --pack-destination "$lab/artifacts")
  cd "$lab/consumer"
  printf '%s\n' '{"name":"watch-consumer","version":"0.0.0","private":true}' > package.json
  npm install --offline --ignore-scripts "$lab/artifacts/$archive"
)
```

The package manifest keeps `"private": true`. npm builds and installs the local tarball directly. The package has no runtime dependencies.

## Launch the installed MCP server

Use absolute paths for the Node executable, the installed CLI and the isolated state directory. Substitute your supported Node binary for `/opt/node/bin/node`. From a different working directory, initialize state once and start the receiver.

```sh
cd /srv
WATCH_NODE=/opt/node/bin/node
WATCH_CLI=/srv/watch-lab/consumer/node_modules/agent-watch-runtime/bin/watch.mjs
WATCH_STATE=/srv/watch-lab/state
"$WATCH_NODE" "$WATCH_CLI" init "$WATCH_STATE"
"$WATCH_NODE" "$WATCH_CLI" receive "$WATCH_STATE"
```

Keep the receiver running. Supply this launch record to the MCP client's stdio process launcher, adapting the field names to its configuration format.

```json
{
  "transport": "stdio",
  "command": "/opt/node/bin/node",
  "args": [
    "/srv/watch-lab/consumer/node_modules/agent-watch-runtime/bin/watch.mjs",
    "serve",
    "/srv/watch-lab/state"
  ]
}
```

The client can start in a directory outside the checkout and consumer project. Protocol fixtures exercise `initialize`, `notifications/initialized`, `tools/list` and `tools/call` over newline-delimited JSON-RPC. The suite checks protocol revisions `2025-11-25` and `2025-06-18`. The [verification ledger](work-ledger.md) tracks named client compatibility.

## Run separate processes

Choose a fresh directory name whose parent exists. Supply the same directory to each command. Initialization creates the directory exclusively, a namespace marker, a generated signing key, the JSON fixture and separate SQLite files for the watch runtime and inbox.

```sh
node bin/watch.mjs init ./watch-dev
node bin/watch.mjs receive ./watch-dev
```

Keep the receiver running. It requests an ephemeral port from the OS on `127.0.0.1` and writes an authenticated registration inside the new directory. An OS-backed lock admits one receiver per directory and releases on process exit. The registration expires after five seconds without renewal. Sender attempts resolve the current registration and signing key ID while retaining the consumer and watch IDs. Receiver startup emits `{"state":"listening"}`. Completed asynchronous jobs emit a line with the watch ID, event ID and sequence. The local handler has no external effect.

From the repository root, start the same stdio server with the source checkout.

```sh
node bin/watch.mjs serve ./watch-dev
```

Call `watch_until` with this tool input.

```json
{"source_id":"sample","consumer_id":"worker","request_id":"first-run","interval_ms":200,"predicate":{"path":"/phase","equals":"ready"}}
```

The `sample` source supplies two generated frames with phases `waiting` and `ready`. The `json` source reads the generated `sample.json` in the selected directory. It starts with `{"phase":"waiting"}`. Publish a new fixture by writing a sibling file and renaming it over `sample.json`. Use `watch_emitted` with `source_id` set to `ticks` to observe two generated occurrences and a replay of the second occurrence on subsequent reads.

The MCP server runs its scheduler for the lifetime of the connection. To keep observation and delivery running across client disconnects, use a separate terminal.

```sh
node bin/watch.mjs worker ./watch-dev
```

Stop the processes with Ctrl-C. Restart them against the same directory to resume their persisted work. The SDK accepts operator-registered source and route maps for additional adapters and key transitions. When using the CLI, each process loads the directory's key on startup. A receiver restart may choose a new port. Pending deliveries retain their original event bytes.

## Watch a generated web snapshot

To register an isolated changedetection.io API fixture, create `web-source.json` in the initialized development directory before starting the producer. Substitute the ephemeral port and the watch UUID from that fixture.

```json
{"baseUrl":"http://127.0.0.1:<fixture-port>","watchId":"<generated-watch-uuid>","maxAgeMs":60000,"timeoutMs":2000}
```

`baseUrl` and `watchId` are required. The optional fields are `apiKey`, `maxAgeMs` and `timeoutMs`. If the fixture uses authentication, supply its generated API key in this local file. The producer loads the bounded regular file when it first samples `web`. Without the file, the source ID is unregistered. Invalid fields, links and malformed content yield `UNKNOWN` and the diagnostic `source_invalid` for a web watch. Restart the producer to reload corrected options.

An MCP client can create a `watch_until` against the latest reliable retained text snapshot using this input.

```json
{"source_id":"web","consumer_id":"worker","request_id":"web-ready","predicate":{"path":"/text","contains":"ready"}}
```

`watch_changes` accepts the same source and consumer IDs with its own request ID. It baselines the retained snapshot and compares subsequent observations. Retention gaps can limit diff evidence. See [source verification notes](sources.md).

Run the generated API fixture over MCP stdio from the repository root.

```sh
node --test --test-name-pattern='stdio MCP watches a generated web snapshot' test/setup.test.mjs
```
