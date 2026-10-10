# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the project follows [Semantic Versioning](https://semver.org/). Before 1.0, a minor release may still change the
protocol or the on-disk format; the notes say so when it does.

## [0.1.1] - 2026-10-10

### Changed

- Group commit for local writes: the writes that arrive while one is synced go to disk together, with one sync, and
  each is answered once it is durable. Several tools writing at once get about four times as many durable writes a
  second as before; nothing changes for a single tool or for the protocol.
- The hub sends the changes already queued for a daemon together, in frames of about 1 MB, instead of one frame per
  change; every entry is serialised once per frame. Daemons push the same way. With several daemons, a burst of
  writes reaches all of them many times faster.
- A reset (a new hub, or one that lost its data) compacts a replica once instead of twice.

### Fixed

- A push the hub already had later versions of could get an answer over the 8 MB frame limit (held values of up to
  1 MB each), which closed the connection again and again; the answer now comes in frames of the usual size.
- After a reset, a node's own definition of a collection the hub's kharka.toml protects, stamped later, replaced the
  protected one, schema included. The hub now keeps its definition and checks restored definitions like pushed ones.
- The hub no longer drops refusals when more than 1024 are waiting for a daemon (one that has 65536 waiting and does
  not read them is disconnected instead), and a daemon saves the writes it got refused once per burst instead of once
  each.

## [0.1.0] - 2026-10-10

The first release.

### Added

- A replica on every machine: reads are answered from memory, writes land at once and also offline.
- A hub that orders the changes and spreads them to every replica as a live stream over ssh; the hub opens no port.
- State and record collections with schemas, and watches on key prefixes.
- Protected collections from `kharka.toml` on the hub, reloaded while everything runs.
- Partial replicas with `--sync`.
- An append-only log with checksums and snapshots, written by hand.
- Service files for launchd and systemd (`kharka service`).
- A versioned on-disk format: `data.log` and `snapshot` start with a header (`KHRK`, format 1). Data written by
  earlier builds is rewritten on the first start, without loss and without a new hub epoch; a file of a newer format
  is refused instead of cut. Going back to 0.0.1 afterwards stops with an error instead of losing data: 0.1.0 always
  leaves a `snapshot`, on which 0.0.1 refuses to start.
- `kharka completions <shell>` and `kharka man`.
- Prebuilt binaries for macOS and Linux (arm64, x86_64), a shell installer and a Homebrew formula
  (`jakobbeyer/tap`). The minimum supported Rust version is 1.89.
- The library crate is documented as internal: the command line and the protocols are the contract.
- Fuzz targets for frames, stamps and keys, schemas and storage loading; property tests for convergence; tests for
  the heartbeat timeout, clock drift and back pressure.

### Fixed

- A stamp far in the future (written while a clock was wrong, or a damaged or forged log record) moved a clock there,
  so every later write of that node was stamped too far ahead and refused by the hub, or wrapped around to `0-0` and
  lost. A clock now ignores stamps more than five minutes ahead of its wall clock. A write to a key whose entry is
  stamped that far ahead replaces it when it is the node's own unconfirmed write, and is otherwise refused with the
  new error `ahead`.
- `kharka status` counts a refused write only while it is still held.
- `--socket` in a folder kharka did not create no longer changes that folder's permissions.

[0.1.1]: https://github.com/jakobbeyer/kharka/releases/tag/v0.1.1
[0.1.0]: https://github.com/jakobbeyer/kharka/releases/tag/v0.1.0
