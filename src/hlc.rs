//! Hybrid logical clocks: the order of every write.

use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::entry::check_node;

/// A stamp: wall time in milliseconds, a counter for writes within the same millisecond (or while the wall clock lags
/// behind a stamp seen earlier), and the node that made it. Compared in that order, stamps put every write in one total
/// order that respects causality: a write made after seeing another one always sorts after it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hlc {
    pub wall_ms: u64,
    pub counter: u32,
    pub node: String,
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}-{}", self.wall_ms, self.counter, self.node)
    }
}

impl FromStr for Hlc {
    type Err = String;

    /// `<wall ms>-<counter>-<node>`; the node may contain `-` itself.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let bad = || format!("not a stamp: {text:?} (expected <wall ms>-<counter>-<node>)");
        let mut parts = text.splitn(3, '-');
        let mut number = || {
            parts
                .next()
                .filter(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
        };
        let wall_ms = number().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
        let counter = number().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
        let node = parts.next().ok_or_else(bad)?;
        check_node(node).map_err(|_| bad())?;
        Ok(Hlc {
            wall_ms,
            counter,
            node: node.to_string(),
        })
    }
}

impl Serialize for Hlc {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Hlc {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// The clock of one node.
#[derive(Debug)]
pub struct Clock {
    node: String,
    last: (u64, u32),
}

impl Clock {
    pub fn new(node: impl Into<String>) -> Clock {
        Clock {
            node: node.into(),
            last: (0, 0),
        }
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn tick(&mut self) -> Hlc {
        self.tick_at(wall_ms())
    }

    /// The next stamp, `now_ms` being the wall clock: never earlier than any stamp made or seen before.
    pub fn tick_at(&mut self, now_ms: u64) -> Hlc {
        let (wall, counter) = self.last;
        self.last = if now_ms > wall {
            (now_ms, 0)
        } else if counter < u32::MAX {
            (wall, counter + 1)
        } else {
            (wall + 1, 0)
        };
        Hlc {
            wall_ms: self.last.0,
            counter: self.last.1,
            node: self.node.clone(),
        }
    }

    /// Takes note of a stamp made elsewhere, so that every later tick sorts after it.
    pub fn observe(&mut self, seen: &Hlc) {
        self.last = self.last.max((seen.wall_ms, seen.counter));
    }
}

pub fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(wall_ms: u64, counter: u32, node: &str) -> Hlc {
        Hlc {
            wall_ms,
            counter,
            node: node.to_string(),
        }
    }

    #[test]
    fn ticks_stay_ahead_when_the_wall_clock_goes_back() {
        let mut clock = Clock::new("a");
        let first = clock.tick_at(1000);
        let second = clock.tick_at(900);
        assert!(second > first);
        assert_eq!((second.wall_ms, second.counter), (1000, 1));
        let third = clock.tick_at(1001);
        assert_eq!((third.wall_ms, third.counter), (1001, 0));
    }

    #[test]
    fn a_stamp_seen_elsewhere_moves_the_clock() {
        let mut clock = Clock::new("a");
        let seen = stamp(5000, 3, "b");
        clock.observe(&seen);
        let next = clock.tick_at(1000);
        assert!(next > seen);
        assert_eq!((next.wall_ms, next.counter), (5000, 4));
    }

    #[test]
    fn the_node_breaks_ties() {
        assert!(stamp(1, 0, "b") > stamp(1, 0, "a"));
        assert!(stamp(1, 1, "a") > stamp(1, 0, "b"));
        assert!(stamp(2, 0, "a") > stamp(1, 9, "z"));
    }

    #[test]
    fn stamps_print_and_parse() {
        let parsed: Hlc = "1791571517123-2-mac-mini".parse().unwrap();
        assert_eq!(parsed, stamp(1791571517123, 2, "mac-mini"));
        assert_eq!(parsed.to_string(), "1791571517123-2-mac-mini");
        for bad in ["", "x-1-a", "1-2", "1-2-", "1-2-a/b", "+1-2-a", "1--2-a"] {
            assert!(bad.parse::<Hlc>().is_err(), "{bad:?} parsed");
        }
    }

    #[test]
    fn stamps_are_json_strings() {
        let json = serde_json::to_string(&stamp(7, 1, "n")).unwrap();
        assert_eq!(json, "\"7-1-n\"");
        assert_eq!(serde_json::from_str::<Hlc>(&json).unwrap(), stamp(7, 1, "n"));
    }
}
