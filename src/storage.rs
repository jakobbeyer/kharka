//! Persistence: an append-only log of entries after a snapshot of the live ones.
//!
//! A record is a 4-byte big-endian length, the CRC32 of the payload (4 bytes, big-endian) and the payload, an entry as
//! JSON. `snapshot` holds every entry at the last compaction, `data.log` every write since. Loading the snapshot and
//! replaying the log, the later stamp winning per key, gives the current state. Replaying is idempotent, so a crash
//! between writing a snapshot and emptying the log loses nothing.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crate::entry::{Entry, MAX_KEY, MAX_VALUE};

/// The largest record: an entry with a key and a value at their largest, and room for the rest.
pub const MAX_RECORD: usize = MAX_VALUE + MAX_KEY + 4096;

pub trait Storage: Send {
    /// Everything stored, oldest first.
    fn load(&mut self) -> io::Result<Vec<Entry>>;
    /// Appends entries, all or none, and (with sync) makes them durable with one sync.
    fn append(&mut self, entries: &[Entry]) -> io::Result<()>;
    /// Replaces everything stored by these entries.
    fn compact(&mut self, entries: &mut dyn Iterator<Item = &Entry>) -> io::Result<()>;
    /// Records appended since the last compaction.
    fn appended(&self) -> usize;
    /// Bytes appended since the last compaction, and the size of the last snapshot.
    fn appended_bytes(&self) -> u64;
    fn snapshot_bytes(&self) -> u64;
    /// Whether loading had to cut damaged records off: something written before may be lost.
    fn damaged(&self) -> bool;
}

/// The log and the snapshot in one folder, which a lock keeps to one process.
pub struct LogStorage {
    dir: PathBuf,
    log: File,
    /// The length of the log's valid records: a failed append is cut back to it.
    len: u64,
    appended: usize,
    snapshot_len: u64,
    damaged: bool,
    sync: bool,
    _lock: File,
}

impl LogStorage {
    /// `sync`: every append and compaction reaches the disk before it returns.
    pub fn open(dir: &Path, sync: bool) -> io::Result<LogStorage> {
        private_dir(dir)?;
        let lock = private_file(
            OpenOptions::new().write(true).create(true).truncate(false),
            &dir.join("lock"),
        )?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(io::Error::other(format!(
                    "another kharka process uses {}",
                    dir.display()
                )));
            }
            Err(fs::TryLockError::Error(err)) => return Err(err),
        }
        let log = private_file(
            OpenOptions::new().read(true).append(true).create(true),
            &dir.join("data.log"),
        )?;
        Ok(LogStorage {
            dir: dir.to_path_buf(),
            log,
            len: 0,
            appended: 0,
            snapshot_len: 0,
            damaged: false,
            sync,
            _lock: lock,
        })
    }
}

impl Storage for LogStorage {
    fn load(&mut self) -> io::Result<Vec<Entry>> {
        let mut entries = Vec::new();
        let snapshot = self.dir.join("snapshot");
        match fs::read(&snapshot) {
            Ok(bytes) => {
                self.snapshot_len = bytes.len() as u64;
                // a snapshot is renamed into place only after it was synced: damage there is real corruption
                if let (valid, Some(problem)) = decode(&bytes, &mut entries) {
                    let message = format!("{}: {problem} at byte {valid}", snapshot.display());
                    return Err(io::Error::new(io::ErrorKind::InvalidData, message));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        let mut bytes = Vec::new();
        (&self.log).read_to_end(&mut bytes)?;
        let before = entries.len();
        let (valid, problem) = decode(&bytes, &mut entries);
        if let Some(problem) = problem {
            // a torn last record: the process died in the middle of an append
            eprintln!(
                "kharka: {}: {problem} at byte {valid}, cut off the {} bytes from there",
                self.dir.join("data.log").display(),
                bytes.len() - valid
            );
            self.log.set_len(valid as u64)?;
            self.log.sync_all()?;
            self.damaged = true;
        }
        self.len = valid as u64;
        self.appended = entries.len() - before;
        Ok(entries)
    }

    fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        let mut buf = Vec::new();
        for entry in entries {
            encode(entry, &mut buf)?;
        }
        let written = self
            .log
            .write_all(&buf)
            .and_then(|()| if self.sync { self.log.sync_data() } else { Ok(()) });
        if let Err(err) = written {
            // half a record would hide every later one from the next load
            let _ = self.log.set_len(self.len);
            return Err(err);
        }
        self.len += buf.len() as u64;
        self.appended += entries.len();
        Ok(())
    }

    fn compact(&mut self, entries: &mut dyn Iterator<Item = &Entry>) -> io::Result<()> {
        let mut buf = Vec::new();
        for entry in entries {
            encode(entry, &mut buf)?;
        }
        let tmp = self.dir.join("snapshot.tmp");
        let mut file = private_file(OpenOptions::new().write(true).create(true).truncate(true), &tmp)?;
        file.write_all(&buf)?;
        file.sync_all()?;
        fs::rename(&tmp, self.dir.join("snapshot"))?;
        // the rename has to be durable before the log goes; not every system can sync a folder, hence best effort
        let _ = File::open(&self.dir).and_then(|dir| dir.sync_all());
        self.log.set_len(0)?;
        self.log.sync_all()?;
        self.len = 0;
        self.appended = 0;
        self.snapshot_len = buf.len() as u64;
        Ok(())
    }

    fn appended(&self) -> usize {
        self.appended
    }

    fn appended_bytes(&self) -> u64 {
        self.len
    }

    fn snapshot_bytes(&self) -> u64 {
        self.snapshot_len
    }

    fn damaged(&self) -> bool {
        self.damaged
    }
}

/// A folder only its owner can enter: made so, and set so when it was there already.
pub fn private_dir(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::DirBuilder::new().recursive(true).mode(0o700).create(dir)?;
    if fs::metadata(dir)?.permissions().mode() & 0o077 != 0 {
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Writes a small file atomically and durably, readable only by its owner: a temporary file, synced, renamed.
pub fn write_private(path: &Path, text: &str) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = private_file(OpenOptions::new().write(true).create(true).truncate(true), &tmp)?;
    file.write_all(text.as_bytes())?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        let _ = File::open(dir).and_then(|dir| dir.sync_all());
    }
    Ok(())
}

fn private_file(options: &mut OpenOptions, path: &Path) -> io::Result<File> {
    options.mode(0o600).open(path)
}

/// One record. A record larger than `MAX_RECORD` is refused: the next load would take it for damage and cut the log.
fn encode(entry: &Entry, buf: &mut Vec<u8>) -> io::Result<()> {
    let payload = serde_json::to_vec(entry).expect("an entry is always JSON");
    if payload.len() > MAX_RECORD {
        let message = format!(
            "{}: a record of {} bytes, at most {MAX_RECORD} fit",
            entry.key,
            payload.len()
        );
        return Err(io::Error::new(io::ErrorKind::InvalidInput, message));
    }
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&crc32fast::hash(&payload).to_be_bytes());
    buf.extend_from_slice(&payload);
    Ok(())
}

/// Decodes records into `out` until the bytes end or a record is damaged. Returns how many bytes were valid and, when it
/// stopped early, why.
fn decode(bytes: &[u8], out: &mut Vec<Entry>) -> (usize, Option<&'static str>) {
    let mut at = 0;
    while at < bytes.len() {
        let Some(head) = bytes.get(at..at + 8) else {
            return (at, Some("a torn record header"));
        };
        let len = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
        let crc = u32::from_be_bytes([head[4], head[5], head[6], head[7]]);
        if len > MAX_RECORD {
            return (at, Some("an impossible record length"));
        }
        let Some(payload) = bytes.get(at + 8..at + 8 + len) else {
            return (at, Some("a torn record"));
        };
        if crc32fast::hash(payload) != crc {
            return (at, Some("a checksum mismatch"));
        }
        match serde_json::from_slice(payload) {
            Ok(entry) => out.push(entry),
            Err(_) => return (at, Some("an unreadable record")),
        }
        at += 8 + len;
    }
    (at, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn entry(key: &str, wall_ms: u64) -> Entry {
        Entry::new(
            key,
            Some(json!({ "n": wall_ms })),
            format!("{wall_ms}-0-t").parse().unwrap(),
        )
    }

    fn reopen(dir: &Path) -> (LogStorage, Vec<Entry>) {
        let mut storage = LogStorage::open(dir, true).unwrap();
        let entries = storage.load().unwrap();
        (storage, entries)
    }

    #[test]
    fn appended_entries_come_back() {
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, entries) = reopen(dir.path());
        assert!(entries.is_empty());
        storage.append(&[entry("a", 1), entry("b", 2)]).unwrap();
        storage.append(&[entry("a", 3)]).unwrap();
        drop(storage);
        let (storage, entries) = reopen(dir.path());
        assert_eq!(entries, vec![entry("a", 1), entry("b", 2), entry("a", 3)]);
        assert_eq!(storage.appended(), 3);
    }

    #[test]
    fn a_torn_tail_is_cut_off_and_appends_go_on() {
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, _) = reopen(dir.path());
        storage.append(&[entry("a", 1), entry("b", 2)]).unwrap();
        drop(storage);
        let log = dir.path().join("data.log");
        let whole = fs::metadata(&log).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(&[0, 0, 0, 50, 1, 2, 3])
            .unwrap();
        let (mut storage, entries) = reopen(dir.path());
        assert_eq!(entries.len(), 2);
        assert_eq!(fs::metadata(&log).unwrap().len(), whole);
        storage.append(&[entry("c", 3)]).unwrap();
        drop(storage);
        assert_eq!(reopen(dir.path()).1.len(), 3);
    }

    #[test]
    fn a_damaged_record_ends_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, _) = reopen(dir.path());
        storage.append(&[entry("a", 1), entry("b", 2)]).unwrap();
        drop(storage);
        let log = dir.path().join("data.log");
        let mut bytes = fs::read(&log).unwrap();
        let last = bytes.len() - 3;
        bytes[last] ^= 0xff;
        fs::write(&log, bytes).unwrap();
        assert_eq!(reopen(dir.path()).1, vec![entry("a", 1)]);
    }

    #[test]
    fn compaction_replaces_the_log_by_a_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, _) = reopen(dir.path());
        storage.append(&[entry("a", 1), entry("a", 2), entry("b", 3)]).unwrap();
        storage.compact(&mut [entry("a", 2), entry("b", 3)].iter()).unwrap();
        assert_eq!(storage.appended(), 0);
        assert_eq!(fs::metadata(dir.path().join("data.log")).unwrap().len(), 0);
        storage.append(&[entry("c", 4)]).unwrap();
        drop(storage);
        let (storage, entries) = reopen(dir.path());
        assert_eq!(entries, vec![entry("a", 2), entry("b", 3), entry("c", 4)]);
        assert_eq!(storage.appended(), 1);
    }

    #[test]
    fn a_record_too_large_to_load_is_never_written() {
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, _) = reopen(dir.path());
        storage.append(&[entry("a", 1)]).unwrap();
        let huge = Entry::new("b", Some(json!("x".repeat(MAX_RECORD))), "2-0-t".parse().unwrap());
        let refused = storage.append(&[entry("c", 3), huge]);
        assert!(refused.is_err_and(|err| err.kind() == io::ErrorKind::InvalidInput));
        drop(storage);
        let (storage, entries) = reopen(dir.path());
        assert_eq!(entries, vec![entry("a", 1)], "all or none: c is not there either");
        assert!(!storage.damaged());
    }

    #[test]
    fn a_cut_log_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, _) = reopen(dir.path());
        storage.append(&[entry("a", 1)]).unwrap();
        drop(storage);
        OpenOptions::new()
            .append(true)
            .open(dir.path().join("data.log"))
            .unwrap()
            .write_all(&[0, 0, 9])
            .unwrap();
        assert!(reopen(dir.path()).0.damaged());
        assert!(!reopen(dir.path()).0.damaged(), "cut once, clean after");
    }

    #[test]
    fn private_files_and_folders() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let folder = dir.path().join("open");
        fs::create_dir(&folder).unwrap();
        fs::set_permissions(&folder, fs::Permissions::from_mode(0o755)).unwrap();
        private_dir(&folder).unwrap();
        assert_eq!(fs::metadata(&folder).unwrap().permissions().mode() & 0o777, 0o700);
        write_private(&folder.join("epoch"), "abc\n").unwrap();
        assert_eq!(fs::read_to_string(folder.join("epoch")).unwrap(), "abc\n");
        assert_eq!(
            fs::metadata(folder.join("epoch")).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn one_process_per_folder() {
        let dir = tempfile::tempdir().unwrap();
        let _first = LogStorage::open(dir.path(), true).unwrap();
        let second = LogStorage::open(dir.path(), true);
        assert!(second.is_err_and(|err| err.to_string().contains("another kharka process")));
    }
}
