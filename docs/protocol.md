# The local socket protocol

How a program talks to the kharka daemon of its own machine. The daemon answers from its replica, so every read is
local; writes are pushed to the hub by the daemon, not by the program.

## Connection

- **Socket:** `$XDG_RUNTIME_DIR/kharka/daemon.sock`, else `$XDG_STATE_HOME/kharka/daemon.sock` (default
  `~/.local/state/kharka/daemon.sock`). `KHARKA_SOCKET` (or `kharka daemon --socket`) moves it. Mode 0600 in a 0700
  folder: only the owner connects.
- **Framing:** JSON lines, UTF-8, one object per line, at most 2 MB per line.
- **Requests** carry an `op` and an `id` (any JSON value); every answer and every watch event repeats the `id`.
- A connection carries any number of requests, watches among them, interleaved. Answers to requests come in the order
  of the requests; watch events can come between them.

## Answers

```json
{"id": 1, "ok": true, …}
{"id": 1, "ok": false, "code": "invalid", "error": "not a key: \"a b\" (…)"}
```

| `code` | when |
|---|---|
| `invalid` | not JSON, unknown `op`, a missing field, a bad key or prefix, a write the collections do not allow, a value over 1 MB |
| `conflict` | `if_hlc` did not match; `current` holds the key's stamp now (null: the key has no value) |
| `ahead` | the key holds an entry stamped more than five minutes ahead of this machine's clock (not one of its own pending writes): a write would lose to it; it can be written once the clock catches up |
| `protected` | the collection comes from the hub's kharka.toml; only the file changes it |
| `not_synced` | the key (or everything under the prefix) is not synced to this machine: see [scope](#scope) |
| `io` | the daemon could not write to its disk |

## Entries

```json
{"key": "tasks/1", "value": {"title": "Price list"}, "hlc": "1791573241742-0-laptop", "seq": 41}
```

| field | |
|---|---|
| `key` | segments of `A-Z a-z 0-9 . _ -` joined by `/`, at most 256 bytes |
| `value` | any JSON document |
| `hlc` | the write's stamp, `<wall ms>-<counter>-<node>`; the node is the machine that wrote it |
| `seq` | the hub's sequence number; missing while the write has not reached the hub |
| `deleted` | `true` on a tombstone: a deletion, in watch events and in the answer to `del` |
| `stale` | `true` when the key belongs to a state collection and was not written within its TTL |

## Operations

### get

```json
{"id": 1, "op": "get", "key": "tasks/1"}
{"id": 1, "ok": true, "entry": {…}}            entry is null when the key has no value
```

### put

```json
{"id": 2, "op": "put", "key": "tasks/1", "value": {"title": "Price list"}, "if_hlc": "1791573241742-0-laptop"}
{"id": 2, "ok": true, "entry": {…}}
```

`if_hlc` is optional: the write happens only if the key's value still has this stamp. It protects against two programs
on one machine overwriting each other. Between machines the latest write wins.

Keys starting with `_` cannot be written (collections have their own operations). In a collection, a state key must be
`<collection>/<this node>/…` and the value must fit the collection's schema; outside every collection, a key is
refused only when the collections are strict.

### del

```json
{"id": 3, "op": "del", "key": "tasks/1", "if_hlc": "…"}
{"id": 3, "ok": true, "entry": {…, "deleted": true}}     entry is null when there was nothing to delete
```

`del` also clears a record of a refused write, `_rejected/<node>/<id>` (with `if_hlc` too).

### ls

```json
{"id": 4, "op": "ls", "prefix": "tasks/"}
{"id": 4, "ok": true, "entries": [{…}, {…}]}            in key order, no tombstones
```

### watch, unwatch

```json
{"id": "w", "op": "watch", "prefix": "status/", "initial": true}
{"id": "w", "ok": true, "watch": "status/"}
{"id": "w", "event": "change", "entry": {…}}             with initial: every current entry …
{"id": "w", "event": "ready"}                            … then ready
{"id": "w", "event": "change", "entry": {…}}             every change from now on, this machine's and the others'
{"id": "w", "event": "sync", "online": false}            the daemon lost the hub (or found it again)
{"id": "w", "event": "reset"}                            the watch fell behind: every current entry follows again, then ready
{"id": 9, "op": "unwatch", "watch": "w"}
{"id": 9, "ok": true}
```

The answer always comes before the first event. A watch is tied to its connection and ends with it; the `id` names it,
so two watches on one connection need two ids. The prefix is plain text: `status/` matches `status/a`, `status` also
matches `statusbar/x`.

### whoami, status

```json
{"id": 5, "op": "whoami"}
{"id": 5, "ok": true, "node": "laptop"}
{"id": 6, "op": "status"}
{"id": 6, "ok": true, "node": "laptop", "hub": "ssh:server", "online": true, "entries": 12, "pending": 0, "refused": 0, "seq": 41,
 "collections": ["status", "tasks"]}
```

`pending` counts the local writes the hub does not have yet; `refused` the ones the hub refused for good (they stay
here, kept on the hub under `_rejected/`, and are no longer pushed); `online` says whether the daemon is synced with the
hub;
`sync` lists the prefixes this machine syncs, null for everything.

### collections, define, undefine

```json
{"id": 7, "op": "collections"}
{"id": 7, "ok": true, "strict": false, "collections": {
  "status": {"kind": "state", "ttl": "30s", "protected": true, "by": "kharka.toml", "hlc": "…"},
  "notes": {"kind": "record", "schema": {"type": "object", "required": ["text"]}, "by": "laptop", "hlc": "…"}
}}
{"id": 8, "op": "define", "name": "notes", "definition": {"kind": "record", "schema": {…}}}
{"id": 8, "ok": true, "entry": {"key": "_collections/notes", …}}
{"id": 9, "op": "undefine", "name": "notes"}
{"id": 9, "ok": true, "entry": {…, "deleted": true}}     entry is null when there was no such collection
```

`collections` lists the collections in force on this machine, with their schemas as JSON, and who set each: `by` is
`kharka.toml` for a protected one (from the hub's file, or without a hub the local one), else the node that wrote it.
`define` sets a collection of this machine's own (`kind`, optional `ttl` and `schema`), `undefine` removes one; its data
stays. Both answer `protected` for a collection from kharka.toml, and `invalid` for a bad name (one key segment, not
starting with `_`) or definition. The change reaches every machine like any write.

## Scope

A daemon started with `--sync tasks/,status/` holds only the keys under those prefixes, and kharka's own keys (starting
with `_`). On such a machine:

- `get`, `put` and `del` of any other key answer `not_synced`; nothing is written.
- `ls` and `watch` answer `not_synced` for a prefix that cannot find anything synced here (`settings/`). A prefix that
  covers more than the synced part (the empty prefix, everything) lists and watches only what is here.

## Keys kharka writes

Keys starting with `_` belong to kharka; every machine holds them, whatever it syncs. They can be read and watched like
any key.

| Key | Value |
|---|---|
| `_nodes/<node>` | `{"online": true\|false, "since": <ms>}`, written by the hub when a machine connects or leaves |
| `_collections/<name>` | a collection's rules (see `collections`) |
| `_kharka/settings` | `{"strict": true\|false}`, from the hub's kharka.toml |
| `_rejected/<node>/<id>` | `{"key", "value", "deleted", "hlc", "reason"}`: a write of that node the hub refused, kept until someone deletes it |
