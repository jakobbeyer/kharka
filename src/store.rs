//! The store: every entry in memory, every change on disk first.
//!
//! The same store serves a daemon's replica and the hub. A daemon writes with `put` and `delete` (sequence number 0:
//! pending until the hub has it) and takes the hub's entries with `from_hub`. The hub takes pushed entries with
//! `accept`, which gives every entry it keeps the next sequence number, and answers catch-ups with `since`.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::ops::Bound;
use std::path::Path;

use serde_json::Value;

use crate::entry::{Entry, MAX_VALUE, check_key, check_writable};
use crate::hlc::{Clock, Hlc};
use crate::storage::{LogStorage, Storage};

pub struct Options {
    /// Every write reaches the disk before it is answered.
    pub sync: bool,
    /// The log is compacted once it holds this many records, or twice as many as there are entries if that is more …
    pub compact_min: usize,
    /// … or once it holds this many bytes, or twice the size of the last snapshot if that is more.
    pub compact_min_bytes: u64,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            sync: true,
            compact_min: 1000,
            compact_min_bytes: 16 << 20,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    /// A conditional write found another stamp (or no entry at all).
    Conflict {
        current: Option<Hlc>,
    },
    Io(io::Error),
}

impl Error {
    /// The `code` of an error answer on the socket.
    pub fn code(&self) -> &'static str {
        match self {
            Error::Invalid(_) => "invalid",
            Error::Conflict { .. } => "conflict",
            Error::Io(_) => "io",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Invalid(message) => f.write_str(message),
            Error::Conflict { current: Some(hlc) } => write!(f, "changed meanwhile, now at {hlc}"),
            Error::Conflict { current: None } => f.write_str("changed meanwhile, the key does not exist now"),
            Error::Io(err) => write!(f, "storage: {err}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

pub struct Store {
    entries: BTreeMap<String, Entry>,
    /// The keys by their entry's sequence number, for `since`.
    by_seq: BTreeMap<u64, String>,
    /// The highest sequence number ever held.
    last_seq: u64,
    storage: Box<dyn Storage>,
    clock: Clock,
    options: Options,
    /// While a batch is open: its entries, each with the one it replaced in memory, written together at `commit`.
    batch: Option<Vec<(Entry, Option<Entry>)>>,
    /// The highest sequence number when the batch was opened, restored when it fails.
    batch_seq: u64,
}

impl Store {
    pub fn open(dir: &Path, node: &str, options: Options) -> io::Result<Store> {
        let storage = LogStorage::open(dir, options.sync)?;
        Store::with_storage(Box::new(storage), node, options)
    }

    pub fn with_storage(mut storage: Box<dyn Storage>, node: &str, options: Options) -> io::Result<Store> {
        let loaded = storage.load()?;
        let mut store = Store {
            entries: BTreeMap::new(),
            by_seq: BTreeMap::new(),
            last_seq: 0,
            storage,
            clock: Clock::new(node),
            options,
            batch: None,
            batch_seq: 0,
        };
        for entry in loaded {
            store.clock.observe(&entry.hlc);
            store.insert(entry);
        }
        store.maybe_compact();
        Ok(store)
    }

    pub fn node(&self) -> &str {
        self.clock.node()
    }

    /// Whether loading had to cut damaged records off the log: what was written after them is gone.
    pub fn damaged(&self) -> bool {
        self.storage.damaged()
    }

    /// Opens a batch: writes land in memory at once and on disk together at `commit`, with a single sync. Callers
    /// publish the entries only after the commit, so nothing is announced before it is durable.
    pub fn begin(&mut self) {
        if self.batch.is_none() {
            self.batch = Some(Vec::new());
            self.batch_seq = self.last_seq;
        }
    }

    /// Writes the open batch. When that fails, its entries leave memory again, as if they had never been written.
    pub fn commit(&mut self) -> io::Result<()> {
        let Some(batch) = self.batch.take() else { return Ok(()) };
        if batch.is_empty() {
            return Ok(());
        }
        let entries: Vec<Entry> = batch.iter().map(|(entry, _)| entry.clone()).collect();
        if let Err(err) = self.storage.append(&entries) {
            for (entry, replaced) in batch.into_iter().rev() {
                self.unset(&entry.key);
                if let Some(replaced) = replaced {
                    self.insert(replaced);
                }
            }
            self.last_seq = self.batch_seq;
            return Err(err);
        }
        self.maybe_compact();
        Ok(())
    }

    /// Entries with a value (tombstones not counted).
    pub fn live_count(&self) -> usize {
        self.entries.values().filter(|entry| !entry.deleted).count()
    }

    pub fn tick(&mut self) -> Hlc {
        self.clock.tick()
    }

    pub fn observe(&mut self, hlc: &Hlc) {
        self.clock.observe(hlc);
    }

    /// The live entry of a key.
    pub fn get(&self, key: &str) -> Option<&Entry> {
        self.entries.get(key).filter(|entry| !entry.deleted)
    }

    /// The entry of a key, a tombstone too.
    pub fn held(&self, key: &str) -> Option<&Entry> {
        self.entries.get(key)
    }

    /// The live entries under a prefix, in key order.
    pub fn list<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a Entry> + 'a {
        self.entries
            .range::<str, _>((Bound::Included(prefix), Bound::Unbounded))
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(_, entry)| entry)
            .filter(|entry| !entry.deleted)
    }

    /// A tool's write. `if_hlc`: only when the key's live entry still has this stamp.
    pub fn put(&mut self, key: &str, value: Value, if_hlc: Option<&Hlc>) -> Result<Entry, Error> {
        check_writable(key).map_err(Error::Invalid)?;
        check_size(&value)?;
        self.check_if(key, if_hlc)?;
        let entry = Entry::new(key, Some(value), self.clock.tick());
        self.write(entry.clone())?;
        Ok(entry)
    }

    /// A write of kharka's own keys (`_collections/…`, clearing `_rejected/…`), checked by the caller: a value, or a
    /// tombstone for `None`.
    pub fn write_own(&mut self, key: &str, value: Option<Value>) -> Result<Entry, Error> {
        check_key(key).map_err(Error::Invalid)?;
        if let Some(value) = &value {
            check_size(value)?;
        }
        let entry = Entry::new(key, value, self.clock.tick());
        self.write(entry.clone())?;
        Ok(entry)
    }

    /// A tool's deletion: a tombstone, or nothing when the key has no live entry.
    pub fn delete(&mut self, key: &str, if_hlc: Option<&Hlc>) -> Result<Option<Entry>, Error> {
        check_writable(key).map_err(Error::Invalid)?;
        self.check_if(key, if_hlc)?;
        if self.get(key).is_none() {
            return Ok(None);
        }
        let entry = Entry::new(key, None, self.clock.tick());
        self.write(entry.clone())?;
        Ok(Some(entry))
    }

    // ── the replica's side ──

    /// The local writes the hub does not have yet, oldest first.
    pub fn pending(&self) -> Vec<Entry> {
        let mut pending: Vec<Entry> = self.entries.values().filter(|entry| entry.seq == 0).cloned().collect();
        pending.sort_by(|a, b| a.hlc.cmp(&b.hlc));
        pending
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Takes an entry from the hub. The same stamp as held is the hub confirming a local write: only its sequence number
    /// is new, kept in memory (should it be lost, the write is pushed once more and confirmed again). A later stamp
    /// replaces what is held and is returned, for the watchers; an earlier one loses to a local write that is still on
    /// its way.
    pub fn from_hub(&mut self, entry: Entry) -> Result<Option<Entry>, Error> {
        check_key(&entry.key).map_err(Error::Invalid)?;
        self.clock.observe(&entry.hlc);
        self.last_seq = self.last_seq.max(entry.seq);
        match self.entries.get(&entry.key) {
            Some(held) if held.hlc == entry.hlc && (held.value != entry.value || held.deleted != entry.deleted) => {
                // the same stamp with another value: two daemons run under one node name
                eprintln!(
                    "kharka: {} came back from the hub with this node's stamp {} but another value: does another daemon \
                     run as {}? Taking the hub's.",
                    entry.key,
                    entry.hlc,
                    self.node()
                );
                self.write(entry.clone())?;
                Ok(Some(entry))
            }
            Some(held) if held.hlc == entry.hlc => {
                if held.seq != entry.seq {
                    self.insert(entry);
                }
                Ok(None)
            }
            Some(held) if held.hlc > entry.hlc => Ok(None),
            _ => {
                self.write(entry.clone())?;
                Ok(Some(entry))
            }
        }
    }

    /// Forgets every sequence number: the hub is a different one (or lost its data), so everything held is pending
    /// again until the new hub confirms it.
    pub fn forget_seqs(&mut self) -> io::Result<()> {
        for entry in self.entries.values_mut() {
            entry.seq = 0;
        }
        self.by_seq.clear();
        self.last_seq = 0;
        self.storage.compact(&mut self.entries.values())
    }

    /// Removes the entries `drop` says yes to, pending or not. Returns how many went.
    pub fn remove_where(&mut self, drop: impl Fn(&Entry) -> bool) -> io::Result<usize> {
        let before = self.entries.len();
        self.entries.retain(|_, entry| !drop(entry));
        let removed = before - self.entries.len();
        if removed > 0 {
            self.by_seq = self
                .entries
                .values()
                .filter(|entry| entry.seq > 0)
                .map(|entry| (entry.seq, entry.key.clone()))
                .collect();
            self.storage.compact(&mut self.entries.values())?;
        }
        Ok(removed)
    }

    /// Keeps only the entries `keep` says yes to, and those still pending (a local write the hub has not seen must not
    /// get lost). Returns how many went. Used when a daemon syncs less than before.
    pub fn retain(&mut self, mut keep: impl FnMut(&Entry) -> bool) -> io::Result<usize> {
        let before = self.entries.len();
        self.entries.retain(|_, entry| entry.seq == 0 || keep(entry));
        let removed = before - self.entries.len();
        if removed > 0 {
            self.by_seq = self
                .entries
                .values()
                .filter(|entry| entry.seq > 0)
                .map(|entry| (entry.seq, entry.key.clone()))
                .collect();
            self.last_seq = self.by_seq.keys().next_back().copied().unwrap_or(0);
            self.storage.compact(&mut self.entries.values())?;
        }
        Ok(removed)
    }

    // ── the hub's side ──

    /// Takes an entry pushed by a node: kept when its stamp beats the one held, with the next sequence number. Returns
    /// the entry now held for the key and whether it changed.
    pub fn accept(&mut self, mut entry: Entry) -> Result<(Entry, bool), Error> {
        check_key(&entry.key).map_err(Error::Invalid)?;
        self.clock.observe(&entry.hlc);
        if let Some(held) = self.entries.get(&entry.key).filter(|held| held.hlc >= entry.hlc) {
            return Ok((held.clone(), false));
        }
        entry.seq = self.last_seq + 1;
        self.write(entry.clone())?;
        Ok((entry, true))
    }

    /// The entries with a sequence number after `seq`, in that order: what a node missed.
    pub fn since(&self, seq: u64) -> Vec<Entry> {
        self.since_where(seq, |_| true)
    }

    /// `since`, only the entries `wanted` says yes to (cloned only then).
    pub fn since_where(&self, seq: u64, wanted: impl Fn(&Entry) -> bool) -> Vec<Entry> {
        self.by_seq
            .range(seq + 1..)
            .filter_map(|(_, key)| self.entries.get(key))
            .filter(|entry| wanted(entry))
            .cloned()
            .collect()
    }

    // ── inside ──

    fn check_if(&self, key: &str, if_hlc: Option<&Hlc>) -> Result<(), Error> {
        let Some(expected) = if_hlc else { return Ok(()) };
        let current = self.get(key).map(|entry| &entry.hlc);
        if current == Some(expected) {
            Ok(())
        } else {
            Err(Error::Conflict {
                current: current.cloned(),
            })
        }
    }

    fn write(&mut self, entry: Entry) -> io::Result<()> {
        if let Some(batch) = &mut self.batch {
            let replaced = self.entries.get(&entry.key).cloned();
            batch.push((entry.clone(), replaced));
            self.insert(entry);
            return Ok(());
        }
        self.storage.append(std::slice::from_ref(&entry))?;
        self.insert(entry);
        self.maybe_compact();
        Ok(())
    }

    /// Takes a key out of memory (undoing a batch).
    fn unset(&mut self, key: &str) {
        if let Some(held) = self.entries.remove(key)
            && held.seq > 0
        {
            self.by_seq.remove(&held.seq);
        }
    }

    /// Keeps an entry in memory unless the one held has a later stamp; the same stamp replaces it (a new sequence
    /// number).
    fn insert(&mut self, entry: Entry) {
        if let Some(held) = self.entries.get(&entry.key) {
            if held.hlc > entry.hlc {
                return;
            }
            if held.seq > 0 {
                self.by_seq.remove(&held.seq);
            }
        }
        if entry.seq > 0 {
            self.by_seq.insert(entry.seq, entry.key.clone());
            self.last_seq = self.last_seq.max(entry.seq);
        }
        self.entries.insert(entry.key.clone(), entry);
    }

    fn maybe_compact(&mut self) {
        let records = self.storage.appended() >= self.options.compact_min.max(2 * self.entries.len());
        let bytes =
            self.storage.appended_bytes() >= self.options.compact_min_bytes.max(2 * self.storage.snapshot_bytes());
        if self.batch.is_some() || !(records || bytes) {
            return;
        }
        if let Err(err) = self.storage.compact(&mut self.entries.values()) {
            eprintln!("kharka: compaction failed, the log keeps growing: {err}");
        }
    }
}

fn check_size(value: &Value) -> Result<(), Error> {
    let size = serde_json::to_vec(value).map_or(usize::MAX, |text| text.len());
    if size > MAX_VALUE {
        return Err(Error::Invalid(format!(
            "the value has {size} bytes, at most {MAX_VALUE} fit"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn open(dir: &Path, node: &str) -> Store {
        Store::open(dir, node, Options::default()).unwrap()
    }

    fn remote(key: &str, value: Value, hlc: &str, seq: u64) -> Entry {
        Entry {
            seq,
            ..Entry::new(key, Some(value), hlc.parse().unwrap())
        }
    }

    #[test]
    fn put_get_list_delete() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "n");
        store.put("topics/a", json!({ "title": "A" }), None).unwrap();
        store.put("topics/b", json!(2), None).unwrap();
        store.put("topicsx", json!(3), None).unwrap();
        assert_eq!(store.get("topics/a").unwrap().value, json!({ "title": "A" }));
        let keys: Vec<_> = store.list("topics/").map(|entry| entry.key.as_str()).collect();
        assert_eq!(keys, ["topics/a", "topics/b"]);
        assert!(store.delete("topics/a", None).unwrap().is_some());
        assert!(store.get("topics/a").is_none());
        assert!(store.held("topics/a").unwrap().deleted);
        assert!(store.delete("topics/a", None).unwrap().is_none());
        assert_eq!(store.list("topics/").count(), 1);
        assert_eq!(store.live_count(), 2);
    }

    #[test]
    fn conditional_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "n");
        let first = store.put("k", json!(1), None).unwrap();
        let second = store.put("k", json!(2), Some(&first.hlc)).unwrap();
        let refused = store.put("k", json!(3), Some(&first.hlc));
        assert!(matches!(refused, Err(Error::Conflict { current: Some(ref hlc) }) if *hlc == second.hlc));
        assert!(matches!(
            store.delete("missing", Some(&first.hlc)),
            Err(Error::Conflict { current: None })
        ));
        assert_eq!(store.get("k").unwrap().value, json!(2));
    }

    #[test]
    fn tools_cannot_write_kharkas_keys_or_huge_values() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "n");
        assert!(matches!(store.put("_nodes/x", json!(1), None), Err(Error::Invalid(_))));
        let huge = json!("x".repeat(MAX_VALUE));
        assert!(matches!(store.put("k", huge, None), Err(Error::Invalid(_))));
    }

    #[test]
    fn everything_survives_a_restart_and_new_stamps_stay_ahead() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "n");
        store.put("a", json!(1), None).unwrap();
        store.delete("a", None).unwrap();
        store.put("b", json!(2), None).unwrap();
        let future = remote("c", json!(3), "99999999999999-0-other", 7);
        store.from_hub(future.clone()).unwrap();
        drop(store);
        let mut store = open(dir.path(), "n");
        assert!(store.get("a").is_none());
        assert_eq!(store.get("b").unwrap().value, json!(2));
        assert_eq!(store.last_seq(), 7);
        assert!(store.put("d", json!(4), None).unwrap().hlc > future.hlc);
    }

    #[test]
    fn compaction_keeps_the_state() {
        let dir = tempfile::tempdir().unwrap();
        let options = Options {
            compact_min: 4,
            ..Options::default()
        };
        let mut store = Store::open(dir.path(), "n", options).unwrap();
        for n in 0..10 {
            store.put("k", json!(n), None).unwrap();
        }
        store.put("other", json!("x"), None).unwrap();
        drop(store);
        let store = open(dir.path(), "n");
        assert_eq!(store.get("k").unwrap().value, json!(9));
        assert_eq!(store.live_count(), 2);
    }

    #[test]
    fn the_replica_takes_newer_entries_and_confirmations() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "a");
        let mine = store.put("k", json!("mine"), None).unwrap();
        assert_eq!(store.pending(), vec![mine.clone()]);

        // the hub confirms the write: same stamp, a sequence number
        let confirmed = Entry { seq: 4, ..mine.clone() };
        assert_eq!(store.from_hub(confirmed).unwrap(), None);
        assert!(store.pending().is_empty());
        assert_eq!(store.get("k").unwrap().seq, 4);

        // an older entry loses, a newer one wins and goes to the watchers
        let older = remote("k", json!("older"), &format!("{}-0-b", mine.hlc.wall_ms - 1), 5);
        assert_eq!(store.from_hub(older).unwrap(), None);
        let newer = remote("k", json!("newer"), &format!("{}-0-b", mine.hlc.wall_ms + 1), 6);
        assert_eq!(store.from_hub(newer.clone()).unwrap(), Some(newer));
        assert_eq!(store.last_seq(), 6);
    }

    #[test]
    fn forgetting_sequence_numbers_makes_everything_pending() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "a");
        store.from_hub(remote("x", json!(1), "5-0-b", 3)).unwrap();
        store.forget_seqs().unwrap();
        assert_eq!(store.last_seq(), 0);
        assert_eq!(store.pending().len(), 1);
        drop(store);
        assert_eq!(open(dir.path(), "a").pending().len(), 1);
    }

    #[test]
    fn retain_drops_what_is_no_longer_synced_but_keeps_pending_writes() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "a");
        store.from_hub(remote("tasks/1", json!(1), "5-0-b", 3)).unwrap();
        store.from_hub(remote("status/b", json!(2), "6-0-b", 4)).unwrap();
        store.put("status/a", json!("mine"), None).unwrap();
        let removed = store.retain(|entry| entry.key.starts_with("tasks/")).unwrap();
        assert_eq!(removed, 1);
        assert!(store.get("status/b").is_none());
        assert!(store.get("status/a").is_some(), "pending: kept until the hub has it");
        assert_eq!(store.last_seq(), 3);
        drop(store);
        assert!(open(dir.path(), "a").get("status/b").is_none());
    }

    /// Storage that counts appends and fails on demand.
    struct Counting {
        appends: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        fail: std::sync::Arc<std::sync::atomic::AtomicBool>,
        records: Vec<Entry>,
    }

    impl Storage for Counting {
        fn load(&mut self) -> io::Result<Vec<Entry>> {
            Ok(self.records.clone())
        }
        fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(io::Error::other("disk full"));
            }
            self.appends.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.records.extend_from_slice(entries);
            Ok(())
        }
        fn compact(&mut self, _: &mut dyn Iterator<Item = &Entry>) -> io::Result<()> {
            Ok(())
        }
        fn appended(&self) -> usize {
            0
        }
        fn appended_bytes(&self) -> u64 {
            0
        }
        fn snapshot_bytes(&self) -> u64 {
            0
        }
        fn damaged(&self) -> bool {
            false
        }
    }

    fn counting(
        node: &str,
    ) -> (
        Store,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        let appends = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let storage = Counting {
            appends: appends.clone(),
            fail: fail.clone(),
            records: vec![],
        };
        (
            Store::with_storage(Box::new(storage), node, Options::default()).unwrap(),
            appends,
            fail,
        )
    }

    #[test]
    fn large_values_are_compacted_by_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let options = Options {
            compact_min: 1_000_000,
            compact_min_bytes: 50_000,
            ..Options::default()
        };
        let mut store = Store::open(dir.path(), "n", options).unwrap();
        for n in 0..40 {
            store
                .put("big", json!({ "n": n, "pad": "x".repeat(10_000) }), None)
                .unwrap();
        }
        let log = std::fs::metadata(dir.path().join("data.log")).unwrap().len();
        assert!(log < 60_000, "the log holds {log} bytes: it was never compacted");
        drop(store);
        assert_eq!(open(dir.path(), "n").get("big").unwrap().value["n"], json!(39));
    }

    #[test]
    fn a_batch_is_one_append() {
        let (mut hub, appends, _) = counting("hub");
        hub.begin();
        for n in 0..100 {
            hub.accept(remote(&format!("k{n}"), json!(n), &format!("{}-0-x", 10 + n), 0))
                .unwrap();
        }
        assert_eq!(
            appends.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "nothing on disk before the commit"
        );
        assert_eq!(hub.since(0).len(), 100, "but in memory at once");
        hub.commit().unwrap();
        assert_eq!(appends.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn a_failed_batch_leaves_no_trace() {
        let (mut hub, _, fail) = counting("hub");
        hub.accept(remote("a", json!(1), "10-0-x", 0)).unwrap();
        fail.store(true, std::sync::atomic::Ordering::SeqCst);
        hub.begin();
        hub.accept(remote("a", json!(2), "11-0-x", 0)).unwrap();
        hub.accept(remote("b", json!(3), "12-0-x", 0)).unwrap();
        assert!(hub.commit().is_err());
        assert_eq!(hub.get("a").unwrap().value, json!(1));
        assert!(hub.get("b").is_none());
        assert_eq!(hub.last_seq(), 1);
        fail.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            hub.accept(remote("b", json!(3), "12-0-x", 0)).unwrap().0.seq,
            2,
            "the numbers go on where they were"
        );
    }

    #[test]
    fn a_confirmation_is_not_written_again() {
        let (mut store, appends, _) = counting("a");
        let mine = store.put("k", json!(1), None).unwrap();
        store.from_hub(Entry { seq: 7, ..mine }).unwrap();
        assert_eq!(
            appends.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the write itself"
        );
        assert_eq!(store.get("k").unwrap().seq, 7);
        assert!(store.pending().is_empty());
    }

    #[test]
    fn the_same_stamp_with_another_value_takes_the_hubs() {
        let (mut store, _, _) = counting("a");
        let mine = store.put("k", json!("mine"), None).unwrap();
        let other = Entry {
            value: json!("the other daemon's"),
            seq: 3,
            ..mine
        };
        assert_eq!(store.from_hub(other.clone()).unwrap(), Some(other));
        assert_eq!(store.get("k").unwrap().value, json!("the other daemon's"));
    }

    #[test]
    fn since_where_filters_before_cloning() {
        let (mut hub, _, _) = counting("hub");
        hub.accept(remote("a/1", json!(1), "10-0-x", 0)).unwrap();
        hub.accept(remote("b/1", json!(2), "11-0-x", 0)).unwrap();
        let keys: Vec<String> = hub
            .since_where(0, |entry| entry.key.starts_with("b/"))
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(keys, ["b/1"]);
    }

    #[test]
    fn the_hub_numbers_what_it_keeps() {
        let dir = tempfile::tempdir().unwrap();
        let mut hub = open(dir.path(), "hub");
        let (first, changed) = hub.accept(remote("a", json!(1), "10-0-x", 0)).unwrap();
        assert!(changed);
        assert_eq!(first.seq, 1);
        hub.accept(remote("b", json!(2), "11-0-x", 0)).unwrap();
        // the same push again, and an older one: nothing changes, the held entry is the answer
        let (again, changed) = hub.accept(remote("a", json!(1), "10-0-x", 0)).unwrap();
        assert!(!changed);
        assert_eq!(again.seq, 1);
        let (held, changed) = hub.accept(remote("a", json!(0), "9-0-y", 0)).unwrap();
        assert!(!changed);
        assert_eq!(held.value, json!(1));
        // a newer one moves the key to the end of the feed
        hub.accept(remote("a", json!(3), "12-0-y", 0)).unwrap();
        let feed: Vec<_> = hub.since(0).into_iter().map(|entry| (entry.key, entry.seq)).collect();
        assert_eq!(feed, [("b".to_string(), 2), ("a".to_string(), 3)]);
        assert_eq!(hub.since(2).len(), 1);
        assert!(hub.since(3).is_empty());
    }
}
