# kharka design

How kharka works, as built: the roles, the data model, the order of writes, the sync protocol, the storage and the local
socket. The last sections list what is deliberately left out and what is still open.

## Goals

- **Local-first.** Every machine holds a full replica. Reads are answered from memory on the same machine; writes land
  locally at once and travel when the hub can be reached. Nothing a tool does waits for the network.
- **Live.** A change reaches every watcher on every connected machine within milliseconds, as a stream, never by polling.
- **Small.** One binary, no runtime, no external database. A config file defines the collections.
- **Built by hand.** No database, broker or sync library underneath: storage, protocol and sync are the point of the
  project.

Non-goals for now: commands to another machine (kharka moves data, not actions), more than one hub, queries beyond key prefixes, large values (at most 1 MB of JSON per key).

## Roles

One binary, `kharka`:

| Role | Runs | Does |
|---|---|---|
| `kharka serve` | on one machine (the hub) | holds the authoritative log, numbers every entry it keeps, streams changes to every daemon, publishes who is connected |
| `kharka daemon` | on every machine, the hub's too | holds the replica, serves the local socket, syncs with the hub |
| `kharka bridge` | started by ssh on the hub's machine | connects stdin/stdout to the hub's socket |
| `kharka get/put/…` | anywhere | a thin client of the local daemon |

Tools never talk to the hub; they talk to their own daemon. The hub's machine runs a daemon as well (`--hub local`), so
every tool on every machine sees the same interface. The hub has its own node name in its stamps (`hub` by default),
different from every daemon's.

## Data model

A **key** is a path of segments `[A-Za-z0-9._-]+` joined by `/`, at most 256 bytes: `status/laptop`, `tasks/17913-10847`.
The first segment names the collection. Keys starting with `_` are written by kharka itself.

An **entry**:

| Field | |
|---|---|
| `key` | |
| `value` | a JSON document (null for a tombstone) |
| `deleted` | a tombstone: a deletion travels like a write, so it stays as an entry |
| `hlc` | the stamp of the write, `<wall ms>-<counter>-<node>`; the node is who wrote it |
| `seq` | the hub's sequence number; `0` (left out) while the write is only on its own machine |

### Collections

A collection gives the keys under one first segment their rules. Collections are entries themselves, so every replica
has them, also offline, and they travel like any data:

| Key | Value |
|---|---|
| `_collections/<name>` | `{ "kind": "state"\|"record", "ttl"?: "30s", "schema"?: {…}, "protected"?: true }` |
| `_kharka/settings` | `{ "strict": true\|false }` |

- **state:** what a node reports about itself. Keys `<collection>/<node>` and below are written only by that node, so
  there are no conflicts. An entry whose stamp is older than the TTL is marked `"stale": true` when a tool reads it.
- **record:** shared data any node writes. The latest write wins, per key.

**Two kinds of collections.**

- **Protected** ones come from `kharka.toml` on the hub. The hub writes each as `_collections/<name>` with
  `"protected": true`, and `strict` as `_kharka/settings`; only the file changes them. A name the file adds replaces a
  node's own definition of it (the data stays); a name the file drops is deleted, and any node may define it again.
- **Own** ones: any node defines, changes and removes them (`kharka collections set NAME …`, `rm NAME`), as long as
  the name is not protected. The latest definition wins, like any record.

The daemon refuses a change to a protected collection at once; the hub refuses it too (a daemon that was offline may
not know yet), restates its version, and keeps the refused write under `_rejected/`.

```toml
strict = false                  # true: every key needs a collection

[collections.status]
kind = "state"
ttl = "30s"

[collections.tasks]
kind = "record"
schema = "task.schema.json"     # a file next to kharka.toml, or an inline table
```

**Strict:** without it (the default), a key outside every collection is written unchecked; with it, refused. It is a
setting of the file, so only the hub's owner switches it.

**Reloading:** the hub looks at kharka.toml and its schema files every two seconds (by their contents), and reads them
again at once on `SIGHUP`. A config that loads is written as entries and reaches every connected daemon through the live
stream, with no reconnect; one that does not load is reported and changes nothing, and so does a file that is gone (it
may be relinked or rewritten in a moment): only a hub that starts without it has no protected collections. `kharka collections` shows what is in force on a
machine (protected, or which node set it), `kharka collections --check FILE` checks a config before it is saved.

**Without a hub,** a daemon lays its local `kharka.toml` over its own collections, as protected ones.

**Order:** a node that defines a collection offline and writes into it pushes both in stamp order, the definition
first, so the hub knows the rules before the data arrives. A changed schema applies to new writes; existing data is not
checked again.

Schemas are a subset of JSON Schema, checked by hand: `type`, `enum`, `const`, `properties`, `required`,
`additionalProperties`, `items`, `minimum`, `maximum`, `minLength`, `maxLength`, `minItems`, `maxItems`. Other keywords
are ignored, as JSON Schema ignores unknown ones. The daemon checks every write at once (also offline), the hub again.

## Order and conflicts

**Hybrid logical clock.** A node stamps a write with `max(its wall clock, the largest stamp it has seen)`, and counts up
when that does not move. Compared as `(wall ms, counter, node)`, stamps put every write in one total order that respects
causality (a write made after seeing another always wins over it) and does not depend on two machines' clocks agreeing.
A loaded replica observes every stamp it holds, so a write after a restart still sorts after everything before it.

**Last writer wins, per key.** An entry replaces the held one when its stamp is greater. Every replica and the hub apply
the same rule, so they converge on the same value whatever order the writes arrive in.

## Sync protocol

Daemon and hub speak in frames over one long-lived, bidirectional stream: a 4-byte big-endian length, then a JSON object
tagged by `type` (at most 8 MB; entries go in batches of about 1 MB).

```
daemon → hub   hello      { version, node, epoch, since, prefixes }
hub → daemon   welcome    { epoch, reset }                      or refused { reason }
hub → daemon   changes    { entries }  …                       everything after `since`, in sequence order
hub → daemon   caught_up  { seq }
daemon → hub   push       { entries, restore? }                 everything still pending, then every local write
hub → daemon   changes    { entries }                           live: every entry the hub keeps, to every daemon
hub → daemon   rejected   { key, hlc, reason, restated }        a pushed entry the hub did not take
both ways      ping, pong                                       every 15 s: the connection is alive
```

The protocol version is 1; a hub refuses a daemon with another one.

- **Pending writes** are simply the entries with `seq` 0; there is no separate outbox. The newest write per key is all
  that travels: a state entry overwritten ten times offline is pushed once.
- **Confirmation:** the hub sends every entry it keeps to every daemon, the sender included. The sender recognises its
  own stamp and takes the sequence number; the entry is no longer pending. The confirmation is kept in memory only:
  should it be lost (a crash before the next compaction), the write is pushed once more and confirmed again. A push
  that changes nothing (an older stamp, or the same push twice) is answered with the entry the hub holds, to the sender
  only, and only once the stream to that sender has passed its sequence number: an answer never overtakes the
  ordered stream, so a daemon never counts past changes still on their way to it.
- **Catch-up:** a daemon remembers its position in the hub's sequence: the highest sequence number it holds, or the
  last `caught_up` (kept in `position`), which also counts what a partial replica does not sync. After a reconnect it
  gets only what came after. The hub keeps one entry per key, so a catch-up is at most one entry per key, however long
  the daemon was away.
- **Epoch and reset:** a hub makes a random epoch with its log, and a new one when loading had to cut its log (a crash,
  a damaged disk): what the daemons counted on may be gone. A daemon that synced with another epoch (another hub, the
  same hub after losing its data or part of it) or that has a position the hub never reached gets `reset`: it drops
  what only the hub writes (`_nodes/`, the protected collections, the settings; the new hub sends its own in the
  catch-up), forgets every sequence number, so everything it holds becomes pending, and pushes it after the catch-up:
  its own writes as a plain push, every other node's entry as a *restoring* push. The hub takes restored entries as
  they are (the latest stamp still wins), without its checks for a sender's own writes, and leaves out what it decides
  itself. A hub that lost its data is filled again by its daemons, with every node's writes. After restoring a hub from
  a backup, delete its `epoch` file, so that it counts as a new hub.
- **Refusals:** the hub refuses a pushed entry with an invalid key, a `_` key other than an own collection or the
  clearing of a `_rejected/` record, a change to a protected collection, an invalid collection definition, a stamp by
  another node than the sender, a value over 1 MB, one the collections do not allow, or a stamp more than five minutes
  ahead of its clock. It keeps the refused write as `_rejected/<node>/<wall ms>-<counter>` = `{key, value, deleted, hlc,
  reason}` (without the value when that would not fit, then `value_bytes`), so offline work is not lost, and answers
  `rejected`. Except for a bad key or the clock, it then restates its own version (or a tombstone) with a later stamp
  (`restated`), so the refused write is replaced everywhere, also in the sender's replica. A write refused without a
  restatement stays in the sender's replica but is no longer pushed (`refused` in `kharka status`, kept in the replica
  folder as `refused`). A record is cleared with `kharka del _rejected/…` from any node.
- **Presence:** the hub writes `_nodes/<node>` = `{"online": true|false, "since": <ms>}` when a node connects and when
  it leaves, and marks everyone offline when it starts. A node has one connection: a new one closes the older (a laptop
  that slept and left it half open, or a second daemon under the same name, which the hub then reports).
- **Heartbeat:** each side pings every 15 s; a connection silent for 45 s is closed, and the daemon connects anew.
- **Scope:** a daemon started with `--sync tasks/,status/` sends its prefixes in the `hello`; the hub then sends it only
  entries under them (in the catch-up and live) and every `_` key (nodes, collections, settings, rejected writes),
  plus every entry the daemon itself wrote, as the confirmation. Sequence numbers stay global. The daemon refuses to
  read or write outside its scope (`not_synced`). When the scope differs from the last run, the daemon removes what it
  no longer holds (its pending writes stay until the hub has them) and catches up once from the beginning, for what it
  newly holds; it notes the scope in its data folder (`sync`) after that catch-up. The scope saves space and traffic;
  it is no access control: a daemon may ask for any prefixes.
- **Back pressure:** a daemon too slow for the stream is caught up from the log (the hub resubscribes and sends what it
  missed); the hub never buffers an unbounded backlog for it, and drops direct answers beyond 1024 (a dropped
  confirmation only means the write is pushed once more).
- **Reconnect:** the daemon retries after 1 s, doubling up to 30 s, and reports a hub that stays away once.

## Storage

The same in the hub and in every daemon:

- **Memory:** a `BTreeMap` of the entries by key, and one by sequence number for catch-ups. Every read is answered here.
- **Disk:** `data.log`, records of a 4-byte length, a CRC32 and the entry as JSON. On start the `snapshot` is loaded
  and the log replayed (the later stamp wins, so replaying is idempotent). A torn or damaged record ends the log: it
  and everything after it is cut off, with a warning (for the hub, also a new epoch). No record larger than the largest
  readable one is ever written.
- **Durability:** a tool's write is appended and synced before it is answered (on macOS a full sync, about 7 ms: some
  100 durable writes a second per daemon). What arrives from the hub, and what the hub takes from one push, is written
  as one batch with a single sync, and only published once it is durable: a catch-up of thousands of entries takes a
  fraction of a second, and local reads do not wait for it.
- **Compaction:** once the log holds more than 1000 records and twice as many as there are entries, or more than 16 MB
  and twice the last snapshot, the entries are written to `snapshot.tmp`, synced, renamed to `snapshot`, and the log is
  emptied. A crash in between loses nothing.
- **One process per folder:** a lock file (`flock`) keeps a second daemon or hub out.

The replica lives in `${XDG_DATA_HOME:-~/.local/share}/kharka/replica`, the hub's log in `…/kharka/hub`; folders 0700
(also when they were there before), files 0600, small files written atomically. The replica folder also holds
`hub-epoch`, `position`, `refused` and `sync` (the scope) from the last sync.

## The local socket

Programs talk to the daemon of their machine over a Unix socket (`$XDG_RUNTIME_DIR/kharka/daemon.sock`, else
`~/.local/state/kharka/daemon.sock`; not `$TMPDIR`, which a launchd agent and a shell do not reliably share), in JSON
lines: `get`, `put`, `del`, `ls`, `watch`, `unwatch`, `whoami`, `status`, `collections`. A watch is answered first, then streams every
change under its prefix, `sync` when the hub comes or goes, and `reset` when it fell behind and starts over. The
specification is [protocol.md](protocol.md).

## Transport and authentication

The daemon runs `ssh -T -o BatchMode=yes <hub> kharka bridge` (searching `~/.cargo/bin`, `~/.local/bin` and Homebrew on
the other side, since non-interactive shells often lack them in `PATH`) and speaks the frames over its stdin and stdout;
`kharka bridge` connects them to the hub's socket. The hub opens no port; ssh and its keys decide who may
connect. Anyone who can reach the hub's socket is trusted; the hub checks only that a node stamps its writes with its
own name.

## Left out on purpose

- **Tombstones are kept for good.** A catch-up must be able to deliver a deletion to a daemon that was away for months.
  Expiring them needs the hub to know the oldest sequence number every daemon has, or a reset for daemons behind the
  horizon.
- **No merge of two offline edits of one record.** Last writer wins per key; merging field by field (or a CRDT) could
  come per collection later.
- **The hub trusts its daemons** beyond the stamp check (see transport).

## Open questions

- A daemon that should follow two hubs, or a second hub as a standby.
- A binary frame encoding once the protocol settles.
- Packaging: a Homebrew tap and an AUR package.
- Client libraries (Rust, JavaScript) on top of [the socket protocol](protocol.md).
