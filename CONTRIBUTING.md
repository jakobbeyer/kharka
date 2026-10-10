# Contributing

For anything larger than a small fix, please open an issue first, so we agree on the direction before you spend time
on it.

## Build and check

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

CI runs the same three on Linux and macOS. The minimum supported Rust version is the `rust-version` in `Cargo.toml`.

## What will not be taken

Storage, protocol and sync are built by hand on purpose. A pull request that replaces them with a database or a sync
library will not be taken.

## Protocol and format changes

A change to the sync protocol, the socket protocol or the on-disk format must update [docs/design.md](docs/design.md)
or [docs/protocol.md](docs/protocol.md). The on-disk format and the sync protocol carry version numbers; see the design
document for when they change. Say in the pull request how older daemons, hubs and data are affected.

## Docs

Write short, plain sentences. No marketing, no filler sections. Keep examples free of real machine names, hosts and
paths: use names like `server` and `laptop`.

## Fuzzing

Fuzz targets live in `fuzz/`. Run one with:

```sh
cargo +nightly fuzz run <target>
```

## License

kharka is MIT licensed. By contributing, you agree that your contribution is under the same license.
