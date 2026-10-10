//! What a daemon syncs: everything, or only the keys under some prefixes (`kharka daemon --sync tasks/,status/`). The
//! hub sends such a daemon only those keys, and the daemon refuses to read or write any other. kharka's own keys
//! (starting with `_`: nodes, collections, settings, rejected writes) always travel.

use std::fmt;

use crate::entry::check_prefix;

/// The key prefixes a daemon syncs; no prefixes at all means everything.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Scope {
    /// Sorted, without duplicates; empty: everything.
    prefixes: Vec<String>,
}

impl Scope {
    /// The scope that holds every key.
    pub fn everything() -> Scope {
        Scope::default()
    }

    /// The scope of these prefixes; none at all is everything.
    pub fn of(prefixes: impl IntoIterator<Item = String>) -> Result<Scope, String> {
        let mut prefixes: Vec<String> = prefixes.into_iter().collect();
        for prefix in &prefixes {
            if prefix.is_empty() {
                return Err("an empty sync prefix: leave --sync out to sync everything".to_string());
            }
            check_prefix(prefix)?;
        }
        prefixes.sort();
        prefixes.dedup();
        Ok(Scope { prefixes })
    }

    /// The synced prefixes, sorted; empty for everything.
    pub fn prefixes(&self) -> &[String] {
        &self.prefixes
    }

    /// Whether nothing is left out.
    pub fn is_everything(&self) -> bool {
        self.prefixes.is_empty()
    }

    /// Whether a key is synced here.
    pub fn holds(&self, key: &str) -> bool {
        self.is_everything() || key.starts_with('_') || self.prefixes.iter().any(|prefix| key.starts_with(prefix))
    }

    /// Whether a listing or watch of this prefix can find anything synced here: it lies inside a synced prefix, or a
    /// synced prefix lies inside it (then only that part is here).
    pub fn overlaps(&self, prefix: &str) -> bool {
        let overlap = |synced: &str| prefix.starts_with(synced) || synced.starts_with(prefix);
        self.is_everything()
            || prefix.starts_with('_')
            || prefix.is_empty()
            || self.prefixes.iter().any(|synced| overlap(synced))
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_everything() {
            f.write_str("everything")
        } else {
            f.write_str(&self.prefixes.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(prefixes: &[&str]) -> Scope {
        Scope::of(prefixes.iter().map(|prefix| prefix.to_string())).unwrap()
    }

    #[test]
    fn keys_in_and_out() {
        let scope = scope(&["tasks/", "status/mac"]);
        assert!(scope.holds("tasks/1"));
        assert!(scope.holds("status/mac"));
        assert!(scope.holds("_nodes/server"));
        assert!(!scope.holds("status/server"));
        assert!(!scope.holds("settings/x"));
        assert!(Scope::everything().holds("anything/at/all"));
    }

    #[test]
    fn prefixes_that_find_something() {
        let scope = scope(&["tasks/"]);
        assert!(scope.overlaps("tasks/"));
        assert!(scope.overlaps("tasks/12"));
        assert!(scope.overlaps(""), "everything here");
        assert!(scope.overlaps("_nodes/"));
        assert!(!scope.overlaps("status/"));
    }

    #[test]
    fn prefixes_are_checked_and_sorted() {
        assert_eq!(scope(&["b/", "a/", "b/"]).prefixes(), ["a/", "b/"]);
        assert!(Scope::of(["".to_string()]).is_err());
        assert!(Scope::of(["a b".to_string()]).is_err());
        assert_eq!(scope(&["b/", "a/"]).to_string(), "a/,b/");
        assert_eq!(Scope::everything().to_string(), "everything");
    }
}
