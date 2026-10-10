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
use crate::hlc::{Clock, Hlc, wall_ms};
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
    /// A local write to a key whose entry is stamped too far ahead of this clock, and is not this node's own pending
    /// one: last writer wins would keep that entry everywhere.
    Ahead {
        key: String,
        hlc: Hlc,
    },
    Io(io::Error),
}

impl Error {
    /// The `code` of an error answer on the socket.
    pub fn code(&self) -> &'static str {
        match self {
            Error::Invalid(_) => "invalid",
            Error::Conflict { .. } => "conflict",
            Error::Ahead { .. } => "ahead",
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
            Error::Ahead { key, hlc } => write!(
                f,
                "{key} holds a write stamped {hlc}, more than five minutes ahead of this clock; it can be written \
                 again once the clock catches up"
            ),
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
    /// The open batch holds a write in place of a later pending entry: it is written by a compaction (see `replace`).
    batch_compacts: bool,
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
            batch_compacts: false,
        };
        let now = wall_ms();
        for entry in loaded {
            store.clock.observe_at(&entry.hlc, now);
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
            self.batch_compacts = false;
        }
    }

    /// Writes the open batch: appended, or, when it replaces a later pending entry, by a compaction. When that fails,
    /// its entries leave memory again, as if they had never been written.
    pub fn commit(&mut self) -> io::Result<()> {
        let Some(batch) = self.batch.take() else { return Ok(()) };
        if batch.is_empty() {
            return Ok(());
        }
        let written = if self.batch_compacts {
            self.storage.compact(&mut self.entries.values())
        } else {
            let entries: Vec<Entry> = batch.iter().map(|(entry, _)| entry.clone()).collect();
            self.storage.append(&entries)
        };
        if let Err(err) = written {
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

    /// The stamp of a write of this node to `key`: after every stamp seen, the held entry's included (unless it is too
    /// far ahead, which the clock does not follow).
    pub fn stamp(&mut self, key: &str) -> Hlc {
        self.stamp_at(key, wall_ms())
    }

    fn stamp_at(&mut self, key: &str, now: u64) -> Hlc {
        if let Some(held) = self.entries.get(key) {
            self.clock.observe_at(&held.hlc, now);
        }
        self.clock.tick_at(now)
    }

    /// The stamp of a write of the hub to a key it decides itself (`_nodes/`, protected collections, settings, a
    /// restatement): after the held entry, however far ahead that one is, so the write always wins. The clock does not
    /// follow it; the daemons take the entry from the hub, which checks no drift of its own writes.
    pub fn stamp_over(&mut self, key: &str) -> Hlc {
        let hlc = self.stamp(key);
        match self.entries.get(key) {
            Some(held) if held.hlc >= hlc => held.hlc.after(&hlc.node),
            _ => hlc,
        }
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
        self.write_local(key, Some(value))
    }

    /// A write of kharka's own keys (`_collections/…`, clearing `_rejected/…`), checked by the caller: a value, or a
    /// tombstone for `None`.
    pub fn write_own(&mut self, key: &str, value: Option<Value>) -> Result<Entry, Error> {
        check_key(key).map_err(Error::Invalid)?;
        if let Some(value) = &value {
            check_size(value)?;
        }
        self.write_local(key, value)
    }

    /// A tool's deletion: a tombstone, or nothing when the key has no live entry.
    pub fn delete(&mut self, key: &str, if_hlc: Option<&Hlc>) -> Result<Option<Entry>, Error> {
        check_writable(key).map_err(Error::Invalid)?;
        self.check_if(key, if_hlc)?;
        if self.get(key).is_none() {
            return Ok(None);
        }
        self.write_local(key, None).map(Some)
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

    /// Forgets every sequence number and removes the entries `drop` says yes to, with one compaction: the hub is a
    /// different one (or lost its data), so everything held is pending again until the new hub confirms it, and what
    /// only the old hub wrote goes. Returns how many entries went.
    pub fn forget_seqs(&mut self, drop: impl Fn(&Entry) -> bool) -> io::Result<usize> {
        let before = self.entries.len();
        self.entries.retain(|_, entry| !drop(entry));
        for entry in self.entries.values_mut() {
            entry.seq = 0;
        }
        self.by_seq.clear();
        self.last_seq = 0;
        self.storage.compact(&mut self.entries.values())?;
        Ok(before - self.entries.len())
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

    /// A write of this node. The clock does not follow a stamp too far ahead of it (see `Clock::observe_at`), so a held
    /// entry stamped like that would win over the write, by last writer wins, here and on the hub. When it is this
    /// node's own and the hub never confirmed it (a write made while this clock was wrong, pending or refused by the
    /// hub), the write replaces it all the same: the hub decides by last writer wins as always, takes this write if it
    /// never took the old one, and otherwise answers with the old one, which this replica takes back. Any other such
    /// entry refuses the write, until the wall clock catches up.
    fn write_local(&mut self, key: &str, value: Option<Value>) -> Result<Entry, Error> {
        // one reading of the clock: a clock stepping back between two would let the write sort before the held entry
        let now = wall_ms();
        let over_own = self.over_own(key, now)?;
        let entry = Entry::new(key, value, self.stamp_at(key, now));
        if over_own {
            self.replace(entry.clone())?;
        } else {
            self.write(entry.clone())?;
        }
        Ok(entry)
    }

    /// Whether a write to `key` at `now` goes in place of an own pending entry stamped too far ahead; `Ahead` when
    /// another entry stamped too far ahead refuses it.
    fn over_own(&self, key: &str, now: u64) -> Result<bool, Error> {
        match self.entries.get(key) {
            Some(held) if held.hlc.too_far_ahead(now) => {
                if held.hlc.node != self.clock.node() || held.seq > 0 {
                    return Err(Error::Ahead {
                        key: key.to_string(),
                        hlc: held.hlc.clone(),
                    });
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Puts a write in place of a later pending entry. Loading the log would keep the later one, so the store is
    /// compacted instead of appending: the snapshot holds the write, before it is answered. In a batch, the batch is
    /// written that way at `commit`.
    fn replace(&mut self, entry: Entry) -> io::Result<()> {
        let replaced = self.entries.insert(entry.key.clone(), entry.clone());
        if let Some(batch) = &mut self.batch {
            batch.push((entry, replaced));
            self.batch_compacts = true;
            return Ok(());
        }
        let compacted = self.storage.compact(&mut self.entries.values());
        if compacted.is_err()
            && let Some(replaced) = replaced
        {
            self.entries.insert(replaced.key.clone(), replaced);
        }
        compacted
    }

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
pub(crate) mod tests {
    use super::*;
    use crate::hlc::MAX_DRIFT_MS;
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
        let future = Entry {
            seq: 7,
            ..Entry::new("c", Some(json!(3)), ahead(60_000, "other"))
        };
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
        store.forget_seqs(|_| false).unwrap();
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

    /// Storage that counts appends and fails (appends and compactions) on demand.
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
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(io::Error::other("disk full"));
            }
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

    /// A store on `Counting` storage, with its count of appends and the switch that makes them fail (the daemon's tests
    /// use it too).
    pub(crate) fn counting(
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

    /// A stamp `ms` ahead of the wall clock.
    fn ahead(ms: u64, node: &str) -> Hlc {
        Hlc {
            wall_ms: wall_ms() + ms,
            counter: 0,
            node: node.into(),
        }
    }

    #[test]
    fn an_own_pending_write_stamped_too_far_ahead_is_replaced_by_the_next() {
        let dir = tempfile::tempdir().unwrap();
        let far = ahead(MAX_DRIFT_MS + 60_000, "n");
        let last: Hlc = format!("{}-{}-n", u64::MAX, u32::MAX).parse().unwrap();
        {
            // written while this machine's clock was ahead, and a damaged record with the largest stamp there is
            let mut store = open(dir.path(), "n");
            store
                .write(Entry::new("k/ahead", Some(json!("ahead")), far.clone()))
                .unwrap();
            store
                .write(Entry::new("k/last", Some(json!("last")), last.clone()))
                .unwrap();
        }
        let mut store = open(dir.path(), "n");
        // the clock did not follow them: its writes are ones the hub takes
        let other = store.put("k/other", json!(1), None).unwrap().hlc;
        assert!(!other.too_far_ahead(wall_ms()));
        // a write to their keys replaces them, stamped by this clock all the same
        let written = store.put("k/ahead", json!("now"), None).unwrap().hlc;
        assert!(written > other && written < far);
        let deleted = store.delete("k/last", None).unwrap().unwrap().hlc;
        assert!(deleted > written && deleted < last);
        assert_eq!(store.get("k/ahead").unwrap().value, json!("now"));
        assert!(store.held("k/last").unwrap().deleted);
        let pending: Vec<_> = store.pending().into_iter().map(|entry| entry.hlc).collect();
        assert_eq!(pending, [other, written, deleted.clone()]);
        // for good: loading again does not bring them back
        drop(store);
        let store = open(dir.path(), "n");
        assert_eq!(store.get("k/ahead").unwrap().value, json!("now"));
        assert_eq!(store.held("k/last").unwrap().hlc, deleted);
    }

    #[test]
    fn local_writes_in_a_batch_see_each_other_and_one_that_replaces_compacts() {
        let (mut store, appends, fail) = counting("n");
        let far = ahead(MAX_DRIFT_MS + 60_000, "n");
        store
            .write(Entry::new("k/ahead", Some(json!("ahead")), far.clone()))
            .unwrap();
        store
            .write(Entry::new("k/later", Some(json!("ahead")), far.clone()))
            .unwrap();
        store.begin();
        let first = store.put("k/other", json!(1), None).unwrap();
        let second = store.put("k/other", json!(2), Some(&first.hlc)).unwrap();
        assert!(second.hlc > first.hlc);
        assert!(matches!(
            store.put("k/other", json!(3), Some(&first.hlc)),
            Err(Error::Conflict { current: Some(ref hlc) }) if *hlc == second.hlc
        ));
        // decided with the write's own reading of the clock, and made in the batch
        let now = store.put("k/ahead", json!("now"), None).unwrap();
        assert!(now.hlc > second.hlc && now.hlc < far);
        store.commit().unwrap();
        assert_eq!(
            appends.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the entries ahead; the batch went in by a compaction"
        );
        assert_eq!(store.get("k/other").unwrap().value, json!(2));
        assert_eq!(store.get("k/ahead").unwrap().hlc, now.hlc);

        // a compaction that fails takes the batch back, the entry it replaced included
        fail.store(true, std::sync::atomic::Ordering::SeqCst);
        store.begin();
        store.put("k/new", json!(1), None).unwrap();
        store.put("k/later", json!("now"), None).unwrap();
        assert!(store.commit().is_err());
        assert!(store.get("k/new").is_none());
        assert_eq!(store.get("k/later").unwrap().hlc, far);
    }

    #[test]
    fn a_batch_that_replaces_is_on_disk_once_committed() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "n");
        let far = ahead(MAX_DRIFT_MS + 60_000, "n");
        store.write(Entry::new("k/ahead", Some(json!("ahead")), far)).unwrap();
        store.begin();
        store.put("k/before", json!(1), None).unwrap();
        let now = store.put("k/ahead", json!("now"), None).unwrap();
        store.put("k/after", json!(2), None).unwrap();
        store.commit().unwrap();
        drop(store);
        let store = open(dir.path(), "n");
        assert_eq!(store.get("k/ahead").unwrap().hlc, now.hlc);
        assert_eq!(store.get("k/before").unwrap().value, json!(1));
        assert_eq!(store.get("k/after").unwrap().value, json!(2));
    }

    #[test]
    fn a_write_over_another_entry_stamped_too_far_ahead_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "n");
        let theirs = ahead(MAX_DRIFT_MS + 60_000, "other");
        store
            .from_hub(remote("k/theirs", json!(1), &theirs.to_string(), 1))
            .unwrap();
        // this node's own, but confirmed: the hub has it, a write with an earlier stamp would lose there
        let confirmed = ahead(MAX_DRIFT_MS + 60_000, "n");
        store
            .from_hub(remote("k/confirmed", json!(2), &confirmed.to_string(), 2))
            .unwrap();
        for (key, hlc) in [("k/theirs", &theirs), ("k/confirmed", &confirmed)] {
            let refused = store.put(key, json!(3), None).unwrap_err();
            assert!(matches!(&refused, Error::Ahead { key: k, hlc: h } if k == key && h == hlc));
            assert_eq!(refused.code(), "ahead");
            assert!(refused.to_string().contains("once the clock catches up"), "{refused}");
            assert!(matches!(store.delete(key, None), Err(Error::Ahead { .. })));
        }
        assert!(store.pending().is_empty());
        // within the limit, the write stamps after it as always
        let near = ahead(60_000, "other");
        store
            .from_hub(remote("k/near", json!(4), &near.to_string(), 3))
            .unwrap();
        assert!(store.put("k/near", json!(5), None).unwrap().hlc > near);
    }

    #[test]
    fn the_hubs_own_write_wins_over_an_entry_stamped_too_far_ahead() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path(), "hub");
        let far = ahead(MAX_DRIFT_MS + 60_000, "hub");
        store
            .accept(remote("_nodes/a", json!({"online": true}), &far.to_string(), 0))
            .unwrap();
        let hlc = store.stamp_over("_nodes/a");
        assert_eq!(hlc, far.after("hub"));
        let (held, changed) = store
            .accept(Entry::new("_nodes/a", Some(json!({"online": false})), hlc))
            .unwrap();
        assert!(changed);
        assert_eq!(held.value, json!({"online": false}));
        // the clock did not follow: other keys are stamped by the wall clock
        assert!(!store.stamp_over("_nodes/b").too_far_ahead(wall_ms()));
    }
}
