//! Entries, keys and node names.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::hlc::Hlc;

/// The longest key, in bytes.
pub const MAX_KEY: usize = 256;
/// The longest node name, in bytes.
pub const MAX_NODE: usize = 64;
/// The largest value, as JSON text.
pub const MAX_VALUE: usize = 1 << 20;
/// Keys starting with `_` belong to kharka itself, and every replica holds them, whatever it syncs.
/// `_nodes/<node>`: whether a node is connected to the hub (written by the hub).
pub const NODES: &str = "_nodes/";
/// `_collections/<name>`: a collection's rules. Protected ones come from the hub's kharka.toml; any node defines others.
pub const COLLECTIONS: &str = "_collections/";
/// `_kharka/settings`: `{"strict": bool}`, from the hub's kharka.toml.
pub const SETTINGS: &str = "_kharka/settings";
/// `_rejected/<node>/<stamp>`: a write of that node the hub did not take, kept so that it is not lost.
pub const REJECTED: &str = "_rejected/";

/// One key's current value, or its tombstone: a deletion has to travel like a write, so it stays as an entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub key: String,
    #[serde(default)]
    pub value: Value,
    #[serde(default, skip_serializing_if = "is_false")]
    pub deleted: bool,
    pub hlc: Hlc,
    /// The hub's sequence number; 0 while the write is only on its own machine.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub seq: u64,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

impl Entry {
    /// A write of `value`, or a tombstone for `None`.
    pub fn new(key: impl Into<String>, value: Option<Value>, hlc: Hlc) -> Entry {
        let deleted = value.is_none();
        Entry {
            key: key.into(),
            value: value.unwrap_or(Value::Null),
            deleted,
            hlc,
            seq: 0,
        }
    }

    /// The value, unless this is a tombstone.
    pub fn live_value(&self) -> Option<&Value> {
        (!self.deleted).then_some(&self.value)
    }
}

fn segment_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

/// A key: segments of `[A-Za-z0-9._-]` joined by `/`, at most `MAX_KEY` bytes.
pub fn check_key(key: &str) -> Result<(), String> {
    if key.is_empty() || key.len() > MAX_KEY {
        return Err(format!("a key has 1 to {MAX_KEY} bytes"));
    }
    if key
        .split('/')
        .any(|segment| segment.is_empty() || !segment.bytes().all(segment_byte))
    {
        return Err(format!(
            "not a key: {key:?} (segments of A-Z a-z 0-9 . _ - joined by /)"
        ));
    }
    Ok(())
}

/// A key a tool may write: keys starting with `_` are kharka's own.
pub fn check_writable(key: &str) -> Result<(), String> {
    check_key(key)?;
    if key.starts_with('_') {
        return Err(format!("{key}: keys starting with _ are written by kharka itself"));
    }
    Ok(())
}

/// A prefix for listing and watching: any start of a key, also the empty one.
pub fn check_prefix(prefix: &str) -> Result<(), String> {
    if prefix.len() > MAX_KEY || !prefix.bytes().all(|byte| byte == b'/' || segment_byte(byte)) {
        return Err(format!("not a key prefix: {prefix:?}"));
    }
    Ok(())
}

/// A node name: one key segment, at most `MAX_NODE` bytes.
pub fn check_node(node: &str) -> Result<(), String> {
    if node.is_empty() || node.len() > MAX_NODE || !node.bytes().all(segment_byte) {
        return Err(format!(
            "not a node name: {node:?} (1 to {MAX_NODE} of A-Z a-z 0-9 . _ -)"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        for good in ["a", "status/mac", "topics/1791399233-10847", "a.b/c_d/E-9"] {
            assert!(check_key(good).is_ok(), "{good}");
        }
        for bad in ["", "/a", "a/", "a//b", "a b", "ä", "a/../b\n", &"x".repeat(MAX_KEY + 1)] {
            assert!(check_key(bad).is_err(), "{bad:?}");
        }
        assert!(check_writable("_nodes/mac").is_err());
        assert!(check_key("_nodes/mac").is_ok());
    }

    #[test]
    fn prefixes() {
        for good in ["", "status/", "status/m", "a/b/"] {
            assert!(check_prefix(good).is_ok(), "{good}");
        }
        assert!(check_prefix("a b").is_err());
    }

    #[test]
    fn tombstones_have_no_live_value() {
        let hlc: Hlc = "1-0-n".parse().unwrap();
        assert_eq!(Entry::new("a", None, hlc.clone()).live_value(), None);
        assert_eq!(Entry::new("a", Some(Value::Null), hlc).live_value(), Some(&Value::Null));
    }
}
