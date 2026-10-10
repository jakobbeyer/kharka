//! kharka is a program, not a library: a small local-first JSON store. Every machine keeps a full replica; one hub
//! orders the changes and spreads them to every replica as a live stream.
//!
//! Its contract is the command line (`kharka --help`) and the socket protocol (docs/protocol.md). This crate exists for
//! the `kharka` binary, for the tests and for fuzzing, which reach the parsers. It has no stable API and may change in
//! any release, which is why its modules are hidden from the docs. See docs/design.md for how it works.

#[doc(hidden)]
pub mod bridge;
#[doc(hidden)]
pub mod client;
#[doc(hidden)]
pub mod daemon;
#[doc(hidden)]
pub mod entry;
#[doc(hidden)]
pub mod frame;
#[doc(hidden)]
pub mod hlc;
#[doc(hidden)]
pub mod hub;
#[doc(hidden)]
pub mod paths;
#[doc(hidden)]
pub mod schema;
#[doc(hidden)]
pub mod scope;
#[doc(hidden)]
pub mod service;
mod socket;
#[doc(hidden)]
pub mod storage;
#[doc(hidden)]
pub mod store;
#[doc(hidden)]
pub mod sync;
