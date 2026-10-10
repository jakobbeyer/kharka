# Changelog

All notable changes are listed here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the project follows [Semantic Versioning](https://semver.org/). Before 1.0, a minor release may still change the
protocol or the on-disk format; the notes say so when it does.

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

[0.1.0]: https://github.com/jakobbeyer/kharka/releases/tag/v0.1.0
