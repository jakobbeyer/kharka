//! Loading a replica from damaged files, through the public store: never a panic. The first byte chooses how the files
//! come about: the input itself as `data.log` or `snapshot`, or a store written with the input's writes, its file then
//! cut, flipped and lengthened as the input says. What loads once loads again, the same, and clean (a damaged log is cut
//! only once); a write after loading survives the next load.

#![no_main]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use kharka::entry::Entry;
use kharka::hlc::Hlc;
use kharka::store::{Options, Store};
use libfuzzer_sys::arbitrary::{Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

fn folder() -> &'static Path {
    static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
    DIR.get_or_init(|| tempfile::tempdir().unwrap()).path()
}

fn options(compact_min: usize) -> Options {
    Options {
        sync: false,
        compact_min,
        ..Options::default()
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&mode, rest)) = data.split_first() else {
        return;
    };
    let dir: PathBuf = folder().join("replica");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    let file = dir.join(if mode & 2 == 0 { "data.log" } else { "snapshot" });
    if mode & 1 == 0 {
        fs::write(&file, rest).unwrap();
    } else if write_and_damage(&dir, &file, Unstructured::new(rest)).is_err() {
        return;
    }

    let Ok(mut store) = Store::open(&dir, "fuzz", options(1000)) else {
        // a damaged snapshot, or a file starting with neither a header nor a record, is refused, not cut: it was synced before it was put in place
        return;
    };
    let loaded = state(&store);
    let after = store.put("fuzz/after", json!("after"), None).ok();
    drop(store);

    let store = Store::open(&dir, "fuzz", options(1000)).expect("what loaded once loads again");
    assert!(!store.damaged(), "a damaged log is cut once, and clean after");
    let mut reloaded = state(&store);
    if let Some(after) = after {
        assert_eq!(reloaded.remove("fuzz/after").map(|entry| entry.hlc), Some(after.hlc));
    }
    assert_eq!(
        reloaded,
        loaded.into_iter().filter(|(key, _)| key != "fuzz/after").collect()
    );
});

/// Every entry held, tombstones too, by key.
fn state(store: &Store) -> BTreeMap<String, Entry> {
    let keys: BTreeSet<String> = store
        .pending()
        .into_iter()
        .chain(store.since(0))
        .map(|entry| entry.key)
        .chain(store.list("").map(|entry| entry.key.clone()))
        .collect();
    keys.into_iter()
        .filter_map(|key| store.held(&key).cloned().map(|entry| (key, entry)))
        .collect()
}

/// A store written with the input's writes (its own and the hub's), its file then damaged as the input says.
fn write_and_damage(dir: &Path, file: &Path, mut u: Unstructured) -> Result<()> {
    let compact_min = if u.arbitrary()? { 4 } else { 1000 };
    let mut store = Store::open(dir, "fuzz", options(compact_min)).unwrap();
    let mut seq = 0;
    for _ in 0..u.int_in_range(0..=24)? {
        let key = format!("k/{}", u.int_in_range(0..=5)?);
        let value = match u.int_in_range(0..=2)? {
            0 => Value::Null,
            1 => json!(u.arbitrary::<i64>()?),
            _ => json!(u.arbitrary::<String>()?),
        };
        match u.int_in_range(0..=2)? {
            0 => drop(store.put(&key, value, None)),
            1 => drop(store.delete(&key, None)),
            _ => {
                seq += u.int_in_range(1..=3)?;
                let hlc = Hlc {
                    wall_ms: u.arbitrary()?,
                    counter: u.arbitrary()?,
                    node: "other".into(),
                };
                let deleted: bool = u.arbitrary()?;
                let entry = Entry {
                    seq,
                    ..Entry::new(key, (!deleted).then_some(value), hlc)
                };
                let _ = store.from_hub(entry);
            }
        }
    }
    drop(store);
    let mut bytes = fs::read(file).unwrap_or_default();
    for _ in 0..u.int_in_range(0..=4)? {
        if bytes.is_empty() {
            break;
        }
        let at = u.choose_index(bytes.len())?;
        match u.int_in_range(0..=2)? {
            0 => bytes.truncate(at),
            1 => bytes[at] ^= u.int_in_range(1..=255)?,
            _ => bytes[at] = u.arbitrary()?,
        }
    }
    bytes.extend_from_slice(u.take_rest());
    OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file)
        .unwrap()
        .write_all(&bytes)
        .unwrap();
    Ok(())
}
