//! kharka: a small local-first JSON store. Every machine keeps a full replica; one hub orders the changes and spreads
//! them to every replica as a live stream. See docs/design.md.

pub mod bridge;
pub mod client;
pub mod daemon;
pub mod entry;
pub mod frame;
pub mod hlc;
pub mod hub;
pub mod paths;
pub mod schema;
pub mod scope;
pub mod service;
mod socket;
pub mod storage;
pub mod store;
pub mod sync;
