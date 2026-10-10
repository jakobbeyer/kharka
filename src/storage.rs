//! Persistence: an append-only log of entries after a snapshot of the live ones.
//!
//! Both files start with a header, the magic `KHRK` and the format version (4 bytes, big-endian), then records. A record
//! is a 4-byte big-endian length, the CRC32 of the payload (4 bytes, big-endian) and the payload, an entry as JSON.
//! `snapshot` holds every entry at the last compaction, `data.log` every write since. Loading the snapshot and replaying
//! the log, the later stamp winning per key, gives the current state. Replaying is idempotent, so a crash between
//! writing a snapshot and emptying the log loses nothing.
//!
//! Files written before the header existed (format 0) are records only. They are read as such and rewritten with a
//! header. A file of a newer format, or one that starts with neither, is left alone: loading fails. After loading there
//! is always a snapshot, if only a header, so that an older kharka refuses these files instead of cutting the log.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crate::entry::{Entry, MAX_KEY, MAX_VALUE};

/// The largest record: an entry with a key and a value at their largest, and room for the rest.
pub const MAX_RECORD: usize = MAX_VALUE + MAX_KEY + 4096;

/// The format this binary writes, and the newest it reads.
pub const FORMAT: u32 = 1;
const MAGIC: &[u8; 4] = b"KHRK";
/// The magic and the format.
const HEADER: usize = 8;

// A format 0 file starts with a record's length, at most MAX_RECORD, so its first byte is 0. The magic starts with
// b'K': neither a header nor a part of one can pass for a format 0 file, and a file starting with any other byte is
// neither, so it is refused (a damaged magic would otherwise be read as records and the log cut), and so is one whose
// first length is larger than any record.
const _: () = assert!(MAX_RECORD < 1 << 24 && MAGIC[0] != 0);

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
        let snapshot_path = self.dir.join("snapshot");
        let snapshot = match fs::read(&snapshot_path) {
            Ok(bytes) => Some(bytes),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        };
        let log_path = self.dir.join("data.log");
        let mut log = Vec::new();
        (&self.log).read_to_end(&mut log)?;
        // both formats are known before anything is written: the files of a newer kharka stay as they are
        let snapshot_format = match &snapshot {
            Some(bytes) => Some(format(&snapshot_path, bytes)?),
            None => None,
        };
        let log_format = format(&log_path, &log)?;

        let mut entries = Vec::new();
        if snapshot.is_none() {
            // kharka 0.0.1 loads the snapshot before it reads the log and refuses to start on one it cannot decode, the
            // header being an impossible record length there; a log it cannot decode it cuts. So there is always a
            // snapshot, written before the log gets its header: going back refuses to start instead of losing the log.
            write_atomic(&snapshot_path, &header())?;
        }
        if let (Some(bytes), Some(format)) = (&snapshot, snapshot_format) {
            let at = format.records(bytes);
            // a snapshot is renamed into place only after it was synced: damage there is real corruption
            if let (valid, Some(problem)) = decode(&bytes[at..], &mut entries) {
                let message = format!("{}: {problem} at byte {}", snapshot_path.display(), at + valid);
                return Err(io::Error::new(io::ErrorKind::InvalidData, message));
            }
            if format != Format::Current {
                upgrade(&snapshot_path, format, &bytes[at..])?;
            }
            self.snapshot_len = (bytes.len() - at) as u64;
        }

        let before = entries.len();
        let at = log_format.records(&log);
        let (valid, problem) = decode(&log[at..], &mut entries);
        if let Some(problem) = problem {
            // a torn last record: the process died in the middle of an append
            eprintln!(
                "kharka: {}: {problem} at byte {}, cut off the {} bytes from there",
                log_path.display(),
                at + valid,
                log.len() - at - valid
            );
            self.damaged = true;
        }
        if log_format != Format::Current {
            upgrade(&log_path, log_format, &log[at..at + valid])?;
            self.log = private_file(OpenOptions::new().read(true).append(true), &log_path)?;
        } else if problem.is_some() {
            self.log.set_len((at + valid) as u64)?;
            self.log.sync_all()?;
        }
        self.len = (HEADER + valid) as u64;
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
        let mut buf = header();
        for entry in entries {
            encode(entry, &mut buf)?;
        }
        // the snapshot is in place for good before the log goes
        write_atomic(&self.dir.join("snapshot"), &buf)?;
        self.snapshot_len = (buf.len() - HEADER) as u64;
        // the log starts with the header since loading: only the records go, so it never lacks a whole header
        self.log.set_len(HEADER as u64)?;
        // that long now, synced or not: a failed append is cut back to here
        self.len = HEADER as u64;
        self.appended = 0;
        self.log.sync_all()
    }

    fn appended(&self) -> usize {
        self.appended
    }

    fn appended_bytes(&self) -> u64 {
        self.len - HEADER as u64
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
    write_atomic(path, text.as_bytes())
}

/// `write_private` for any bytes. A crash before the rename leaves the old file whole.
fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let mut file = private_file(OpenOptions::new().write(true).create(true).truncate(true), &tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&tmp, path)?;
    // the rename has to be durable before what comes next; not every system can sync a folder, hence best effort
    if let Some(dir) = path.parent() {
        let _ = File::open(dir).and_then(|dir| dir.sync_all());
    }
    Ok(())
}

fn private_file(options: &mut OpenOptions, path: &Path) -> io::Result<File> {
    options.mode(0o600).open(path)
}

/// What a file of records starts with.
#[derive(Clone, Copy, PartialEq)]
enum Format {
    /// Nothing, or a part of a header (a crash while it was written): no records.
    Empty,
    /// Format 0: records from the first byte, as written before the header existed.
    Headless,
    /// The header of `FORMAT`, then records.
    Current,
}

impl Format {
    /// Where the records start.
    fn records(self, bytes: &[u8]) -> usize {
        match self {
            Format::Empty => bytes.len(),
            Format::Headless => 0,
            Format::Current => HEADER,
        }
    }
}

/// The format of a file, or an error for one this binary cannot read.
fn format(path: &Path, bytes: &[u8]) -> io::Result<Format> {
    if !MAGIC.starts_with(&bytes[..bytes.len().min(MAGIC.len())]) {
        // a first length beyond MAX_RECORD is no torn write but damage (`\0HRK…`): refused, not cut
        let length = bytes
            .get(..4)
            .map(|len| u32::from_be_bytes([len[0], len[1], len[2], len[3]]));
        if bytes[0] == 0 && length.is_none_or(|len| len as usize <= MAX_RECORD) {
            return Ok(Format::Headless);
        }
        let message = format!(
            "{}: starts with neither a header nor a record: damaged, or not a kharka file",
            path.display()
        );
        return Err(io::Error::new(io::ErrorKind::InvalidData, message));
    }
    let Some(version) = bytes.get(MAGIC.len()..HEADER) else {
        return Ok(Format::Empty);
    };
    let version = u32::from_be_bytes([version[0], version[1], version[2], version[3]]);
    let problem = match version {
        FORMAT => return Ok(Format::Current),
        newer if newer > FORMAT => format!("written by a newer kharka, format {newer}; this one reads up to {FORMAT}"),
        _ => format!("an unknown format {version}"),
    };
    let message = format!("{}: {problem}", path.display());
    Err(io::Error::new(io::ErrorKind::InvalidData, message))
}

fn header() -> Vec<u8> {
    let mut header = MAGIC.to_vec();
    header.extend_from_slice(&FORMAT.to_be_bytes());
    header
}

/// Rewrites a file without a header as `FORMAT`, atomically: until the rename the old file stays, and the next start
/// rewrites it again. Nothing is lost, so this is no damage.
fn upgrade(path: &Path, format: Format, records: &[u8]) -> io::Result<()> {
    if format == Format::Headless {
        eprintln!("kharka: {}: format 0, rewritten as format {FORMAT}", path.display());
    }
    let mut bytes = header();
    bytes.extend_from_slice(records);
    write_atomic(path, &bytes)
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
        assert_eq!(
            fs::read(dir.path().join("data.log")).unwrap(),
            header(),
            "emptied down to its header"
        );
        assert!(fs::read(dir.path().join("snapshot")).unwrap().starts_with(&header()));
        storage.append(&[entry("c", 4)]).unwrap();
        drop(storage);
        let (storage, entries) = reopen(dir.path());
        assert_eq!(entries, vec![entry("a", 2), entry("b", 3), entry("c", 4)]);
        assert_eq!(storage.appended(), 1);
    }

    #[test]
    fn compaction_only_cuts_the_log_back_to_its_header() {
        use std::io::{Seek, SeekFrom};
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, _) = reopen(dir.path());
        storage.append(&[entry("a", 1), entry("b", 2)]).unwrap();
        // a handle that writes where it stands, not at the end, shows any write: a failed one would leave the log
        // without a whole header, and every append after it would be cut on the next load
        let log = dir.path().join("data.log");
        storage.log = OpenOptions::new().read(true).write(true).open(&log).unwrap();
        storage.log.seek(SeekFrom::End(0)).unwrap();
        storage.compact(&mut [entry("a", 1), entry("b", 2)].iter()).unwrap();
        assert_eq!(fs::read(&log).unwrap(), header());
        assert_eq!(storage.appended_bytes(), 0);
        drop(storage);
        let (storage, entries) = reopen(dir.path());
        assert_eq!(entries, vec![entry("a", 1), entry("b", 2)]);
        assert!(!storage.damaged());
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

    /// Records without a header, as before formats had versions.
    fn format_0(entries: &[Entry]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for entry in entries {
            encode(entry, &mut bytes).unwrap();
        }
        bytes
    }

    #[test]
    fn a_new_log_starts_with_the_header() {
        let dir = tempfile::tempdir().unwrap();
        let (storage, _) = reopen(dir.path());
        assert_eq!(fs::read(dir.path().join("data.log")).unwrap(), b"KHRK\0\0\0\x01");
        assert_eq!(storage.appended_bytes(), 0);
    }

    #[test]
    fn format_0_files_are_rewritten_with_a_header_and_nothing_lost() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("snapshot"), format_0(&[entry("a", 1), entry("b", 2)])).unwrap();
        fs::write(dir.path().join("data.log"), format_0(&[entry("a", 3)])).unwrap();
        let (mut storage, entries) = reopen(dir.path());
        assert_eq!(entries, vec![entry("a", 1), entry("b", 2), entry("a", 3)]);
        assert!(!storage.damaged(), "a rewrite is no damage");
        assert_eq!(storage.appended(), 1);
        for file in ["snapshot", "data.log"] {
            assert!(
                fs::read(dir.path().join(file)).unwrap().starts_with(&header()),
                "{file}"
            );
        }
        storage.append(&[entry("c", 4)]).unwrap();
        drop(storage);
        let (storage, entries) = reopen(dir.path());
        assert_eq!(entries.len(), 4);
        assert!(!storage.damaged());
    }

    #[test]
    fn a_torn_format_0_log_is_cut_while_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = format_0(&[entry("a", 1)]);
        bytes.extend_from_slice(&[0, 0, 0, 50, 1, 2, 3]);
        fs::write(dir.path().join("data.log"), bytes).unwrap();
        let (storage, entries) = reopen(dir.path());
        assert_eq!(entries, vec![entry("a", 1)]);
        assert!(storage.damaged(), "the torn record is lost, as without the rewrite");
        assert_eq!(
            fs::read(dir.path().join("data.log")).unwrap(),
            [header(), format_0(&[entry("a", 1)])].concat()
        );
    }

    #[test]
    fn a_newer_format_is_refused_and_left_alone() {
        for file in ["snapshot", "data.log"] {
            let dir = tempfile::tempdir().unwrap();
            let mut bytes = b"KHRK\0\0\0\x02".to_vec();
            bytes.extend_from_slice(b"whatever format 2 holds");
            fs::write(dir.path().join(file), &bytes).unwrap();
            let other = if file == "snapshot" { "data.log" } else { "snapshot" };
            fs::write(dir.path().join(other), format_0(&[entry("a", 1)])).unwrap();
            let refused = LogStorage::open(dir.path(), true).unwrap().load();
            let message = refused.unwrap_err().to_string();
            assert!(
                message.ends_with("written by a newer kharka, format 2; this one reads up to 1"),
                "{message}"
            );
            assert_eq!(fs::read(dir.path().join(file)).unwrap(), bytes, "{file} untouched");
            assert_eq!(
                fs::read(dir.path().join(other)).unwrap(),
                format_0(&[entry("a", 1)]),
                "nothing written before the refusal"
            );
        }
    }

    #[test]
    fn a_file_starting_with_neither_a_header_nor_a_record_is_refused_and_left_alone() {
        for file in ["snapshot", "data.log"] {
            for damaged in [&b"KHRL\0\0\0\x01"[..], b"kHRK\0\0\0\x01", b"X", b"\0HRK\0\0\0\x01"] {
                let dir = tempfile::tempdir().unwrap();
                let bytes = [damaged, &format_0(&[entry("a", 1)])].concat();
                fs::write(dir.path().join(file), &bytes).unwrap();
                let refused = LogStorage::open(dir.path(), true).unwrap().load();
                let message = refused.unwrap_err().to_string();
                assert!(
                    message.ends_with("starts with neither a header nor a record: damaged, or not a kharka file"),
                    "{message}"
                );
                assert_eq!(fs::read(dir.path().join(file)).unwrap(), bytes, "{file} untouched");
                if file == "data.log" {
                    assert!(
                        !dir.path().join("snapshot").exists(),
                        "nothing written before the refusal"
                    );
                }
            }
        }
    }

    #[test]
    fn there_is_always_a_snapshot_so_an_older_kharka_refuses_instead_of_cutting_the_log() {
        let logs = [
            Vec::new(),
            format_0(&[entry("a", 1)]),
            [header(), format_0(&[entry("a", 1)])].concat(),
        ];
        for log in logs {
            let dir = tempfile::tempdir().unwrap();
            fs::write(dir.path().join("data.log"), &log).unwrap();
            let (storage, entries) = reopen(dir.path());
            assert_eq!(entries.len(), storage.appended());
            assert_eq!(storage.snapshot_bytes(), 0);
            let snapshot = fs::read(dir.path().join("snapshot")).unwrap();
            assert_eq!(snapshot, header());
            // kharka 0.0.1 decodes the snapshot first, as records from byte 0, and stops at an error there
            let mut entries = Vec::new();
            assert_eq!(
                decode(&snapshot, &mut entries),
                (0, Some("an impossible record length"))
            );
        }
    }

    #[test]
    fn a_torn_header_counts_as_empty() {
        for torn in [&b"K"[..], b"KHRK", b"KHRK\0\0\0"] {
            let dir = tempfile::tempdir().unwrap();
            fs::write(dir.path().join("data.log"), torn).unwrap();
            let (mut storage, entries) = reopen(dir.path());
            assert!(entries.is_empty());
            assert!(!storage.damaged());
            storage.append(&[entry("a", 1)]).unwrap();
            drop(storage);
            assert_eq!(reopen(dir.path()).1, vec![entry("a", 1)]);
        }
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
