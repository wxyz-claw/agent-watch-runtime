# Run the local development system

The package uses the Node.js standard library, including `node:sqlite`. From the repository root, run the end-to-end demo with a supported Node binary on your PATH.

```sh
npm ci --offline --ignore-scripts
npm run demo
```

The demo creates a fresh temporary directory, starts the loopback receiver and calls `watch_until` on a generated JSON fixture over MCP stdio. It closes the MCP connection and exits that server process. A headless worker reopens the watch database, observes the updated fixture and signs the pending event. The receiver commits the inbox item and a queued job. Its separate worker completes a local no-op handler. The demo reopens both databases, checks one accepted delivery and one completed job, prints their IDs and counts, and removes its temporary directory.

## Run separate processes

Choose a fresh directory name whose parent exists. Supply the same directory to each command. Initialization creates the directory exclusively, a namespace marker, a generated signing key, the JSON fixture and separate SQLite files for the watch runtime and inbox.

```sh
node bin/watch.mjs init ./watch-dev
node bin/watch.mjs receive ./watch-dev
```

Keep the receiver running. It requests an ephemeral port from the OS on `127.0.0.1` and writes an authenticated registration inside the new directory. An OS-backed lock admits one receiver per directory and releases on process exit. The registration expires after five seconds without renewal. Sender attempts resolve the current registration and signing key ID while retaining the consumer and watch IDs. Receiver startup emits `{"state":"listening"}`. Completed asynchronous jobs emit a line with the watch ID, event ID and sequence. The local handler has no external effect.

Configure an MCP client to launch the following command with an absolute path to the development directory. The server reads and writes newline-delimited JSON-RPC over stdin and stdout.

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
