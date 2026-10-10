# Releasing

A release is a version bump and a tag. The tag starts the release workflow (cargo-dist), which builds the binaries,
the shell installer and the GitHub release, and updates the Homebrew formula in `jakobbeyer/homebrew-tap`. crates.io
is published by hand.

## Version

kharka follows [Semantic Versioning](https://semver.org/). Before 1.0:

- **patch** (`0.1.1`): fixes, nothing incompatible;
- **minor** (`0.2.0`): a change to the sync protocol, the socket protocol, the command line or the on-disk format
  that older daemons, hubs, programs or data notice, or larger new features.

A change to the sync protocol raises its version (`docs/design.md`); a change to the on-disk format raises `FORMAT` in
`src/storage.rs` and comes with a migration of the older format. The changelog names both.

## Steps

1. On `main`, with CI green, raise `version` in `Cargo.toml` and run `cargo build`, so that `Cargo.lock` follows.
2. Add a section to `CHANGELOG.md`: `## [0.1.1] - YYYY-MM-DD` with *Added*, *Changed* and *Fixed* as needed, in
   plain sentences, and the link `[0.1.1]: https://github.com/jakobbeyer/kharka/releases/tag/v0.1.1` at the bottom.
   The heading must match the version: cargo-dist takes this section as the release notes.
3. Commit (`release 0.1.1`), push, and wait for CI.
4. Tag and push the tag:
   ```sh
   git tag -a v0.1.1 -m "kharka 0.1.1"
   git push origin v0.1.1
   ```
   A tag like `v0.2.0-rc.1` makes a prerelease, without the Homebrew formula.
5. Once the release workflow is green: `cargo publish`.

## When something goes wrong

- Tags `v*` cannot be moved or deleted, and crates.io takes a version only once. If a job of the release workflow
  fails for an outside reason (a token, the network), run it again: `gh run rerun <run id> --failed`. If the code is
  wrong, release a new patch version.
- Never edit `.github/workflows/release.yml` by hand. After updating cargo-dist, regenerate it with `dist generate`
  and check it with `dist plan`.
- The Homebrew job needs the secret `HOMEBREW_TAP_TOKEN`: a fine-grained token with *Contents: read and write* on
  `jakobbeyer/homebrew-tap` only.
- When `rust-version` in `Cargo.toml` rises, raise the toolchain of the `msrv` job in `.github/workflows/ci.yml` with
  it.
