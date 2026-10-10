//! Collections as data: protected ones from the hub's kharka.toml (also against a restoring daemon), own ones from any
//! machine, strict mode, and writes the hub refused, kept under `_rejected/`.

mod common;

use std::fs;
use std::path::Path;

use common::*;
use kharka::entry::Entry;
use kharka::hlc::{Hlc, wall_ms};
use kharka::store::{Options, Store};
use serde_json::{Value, json};

fn hub_target(dir: &Path) -> String {
    hub_socket(dir).display().to_string()
}

fn synced(dir: &Path, node: &str) -> bool {
    status(dir, node)["online"] == true
}

fn collections(dir: &Path, node: &str) -> Value {
    serde_json::from_str(&stdout(&run(dir, node, &["collections", "--json"]))).unwrap()
}

fn write_schema(dir: &Path, name: &str, schema: Value) -> String {
    let path = dir.join(name);
    fs::write(&path, schema.to_string()).unwrap();
    path.display().to_string()
}

#[test]
fn a_machine_defines_a_collection_and_every_machine_follows_it() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    let _b = start_daemon(dir, "b", Some(&target));
    let schema = write_schema(dir, "note.json", json!({ "type": "object", "required": ["text"] }));

    let set = run(dir, "a", &["collections", "set", "notes", "--schema", &schema]);
    assert!(set.status.success(), "{}", stderr(&set));
    wait_until("b has the collection", || {
        collections(dir, "b")["collections"]["notes"]["by"] == "a"
    });
    let refused = run(dir, "b", &["put", "notes/1", "{}"]);
    assert!(stderr(&refused).contains("text is missing"), "{}", stderr(&refused));
    put(dir, "b", "notes/1", r#"{"text":"hi"}"#);

    // any machine changes it
    assert!(
        run(
            dir,
            "b",
            &["collections", "set", "notes", "--kind", "state", "--ttl", "1m"]
        )
        .status
        .success()
    );
    wait_until("a has b's version", || {
        collections(dir, "a")["collections"]["notes"]["kind"] == "state"
    });
    assert!(stdout(&run(dir, "a", &["collections"])).contains("notes  state   ttl 1m     no schema  by b"));

    // removing it leaves the data
    assert!(run(dir, "a", &["collections", "rm", "notes"]).status.success());
    wait_until("b sees it gone", || {
        collections(dir, "b")["collections"].get("notes").is_none()
    });
    assert!(get(dir, "b", "notes/1").is_some());
    assert_eq!(run(dir, "a", &["collections", "rm", "notes"]).status.code(), Some(3));
    let bad = run(dir, "a", &["collections", "set", "_x"]);
    assert!(stderr(&bad).contains("not a collection name"), "{}", stderr(&bad));
}

#[test]
fn the_hubs_file_protects_its_collections() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(dir.join("kharka.toml"), "[collections.tasks]\nkind = \"record\"\n").unwrap();
    let hub = start_hub(dir);
    let _a = start_daemon(dir, "a", Some(&hub_target(dir)));
    wait_until("a has tasks", || {
        collections(dir, "a")["collections"]["tasks"]["protected"] == true
    });
    let refused = run(dir, "a", &["collections", "set", "tasks", "--kind", "state"]);
    assert!(stderr(&refused).contains("tasks is protected"), "{}", stderr(&refused));
    assert!(!run(dir, "a", &["collections", "rm", "tasks"]).status.success());

    // a name of a machine's own becomes protected once the file has it, and free again when the file drops it
    assert!(
        run(dir, "a", &["collections", "set", "notes", "--kind", "state"])
            .status
            .success()
    );
    wait_until("the hub has a's notes", || status(dir, "a")["pending"] == 0);
    fs::write(
        dir.join("kharka.toml"),
        "[collections.tasks]\n\n[collections.notes]\nkind = \"record\"\n",
    )
    .unwrap();
    wait_until("notes is protected now", || {
        let notes = &collections(dir, "a")["collections"]["notes"];
        notes["protected"] == true && notes["kind"] == "record"
    });
    assert!(!run(dir, "a", &["collections", "set", "notes"]).status.success());
    fs::write(dir.join("kharka.toml"), "[collections.tasks]\n").unwrap();
    wait_until("notes is gone", || {
        collections(dir, "a")["collections"].get("notes").is_none()
    });
    assert!(run(dir, "a", &["collections", "set", "notes"]).status.success());
    assert!(hub.log().contains("collections reloaded"));
}

#[test]
fn strict_is_switched_in_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _hub = start_hub(dir);
    let _a = start_daemon(dir, "a", Some(&hub_target(dir)));
    wait_until("a synced", || synced(dir, "a"));
    put(dir, "a", "anything/1", "1");

    fs::write(dir.join("kharka.toml"), "strict = true\n").unwrap();
    wait_until("a is strict", || collections(dir, "a")["strict"] == true);
    let refused = run(dir, "a", &["put", "anything/2", "2"]);
    assert!(stderr(&refused).contains("(strict)"), "{}", stderr(&refused));
    assert!(run(dir, "a", &["collections", "set", "anything"]).status.success());
    put(dir, "a", "anything/2", "2");
    assert!(stdout(&run(dir, "a", &["collections"])).ends_with("(strict: every key needs a collection)"));
}

#[test]
fn a_write_refused_by_the_hub_is_kept_under_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(dir.join("kharka.toml"), "[collections.tasks]\n").unwrap();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    let _b = start_daemon(dir, "b", Some(&target));
    wait_until("a has tasks", || {
        collections(dir, "a")["collections"].get("tasks").is_some()
    });

    // a writes while the hub is away, and meanwhile the file gets a schema a's write does not fit
    drop(hub);
    wait_until("a sees the hub gone", || !synced(dir, "a"));
    put(dir, "a", "tasks/1", r#"{"note":"no title"}"#);
    fs::write(dir.join("task.json"), r#"{"type":"object","required":["title"]}"#).unwrap();
    fs::write(dir.join("kharka.toml"), "[collections.tasks]\nschema = \"task.json\"\n").unwrap();
    let _hub = start_hub(dir);

    wait_until("the refusal is kept", || {
        !stdout(&run(dir, "b", &["ls", "_rejected/a/"])).is_empty()
    });
    let record_key = stdout(&run(dir, "b", &["ls", "_rejected/a/"]));
    let record: Value = serde_json::from_str(&get(dir, "b", &record_key).unwrap()).unwrap();
    assert_eq!(record["key"], "tasks/1");
    assert_eq!(record["value"], json!({ "note": "no title" }));
    assert!(record["reason"].as_str().unwrap().contains("title is missing"));
    // the hub's version (nothing) replaced a's write everywhere
    wait_until("a's write is replaced", || get(dir, "a", "tasks/1").is_none());
    assert!(get(dir, "b", "tasks/1").is_none());

    // a record is cleared by deleting it
    assert!(run(dir, "a", &["del", &record_key]).status.success());
    wait_until("b sees it cleared", || get(dir, "b", &record_key).is_none());
}

#[test]
fn a_restored_definition_does_not_replace_a_protected_collection() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(
        dir.join("kharka.toml"),
        "[collections.tasks]\nschema = { type = \"object\", required = [\"title\"] }\n",
    )
    .unwrap();
    // a's replica, made beforehand: b's own definition of tasks, stamped after the one the hub is about to write (b's
    // clock runs a minute ahead). a never synced with this hub, so it restores what it holds.
    {
        let mut store = Store::open(&dir.join("a"), "a", Options::default()).unwrap();
        let later = Hlc {
            wall_ms: wall_ms() + 60_000,
            counter: 0,
            node: "b".into(),
        };
        let own = Entry::new("_collections/tasks", Some(json!({ "kind": "state" })), later);
        store.from_hub(own).unwrap();
        assert_eq!(store.pending().len(), 1);
    }
    let hub = start_hub(dir);
    let _a = start_daemon(dir, "a", Some(&hub_target(dir)));
    wait_until("a restored the hub", || {
        let status = status(dir, "a");
        status["online"] == true && status["pending"] == 0
    });

    // the file's definition stays, restated over b's on a as well
    let tasks = &collections(dir, "a")["collections"]["tasks"];
    assert_eq!((&tasks["protected"], &tasks["kind"]), (&json!(true), &json!("record")));
    let refused = run(dir, "a", &["put", "tasks/1", "{}"]);
    assert!(stderr(&refused).contains("title is missing"), "{}", stderr(&refused));
    assert!(hub.log().contains("kept the hub's _collections/tasks"), "{}", hub.log());
}

#[test]
fn a_collection_made_offline_arrives_before_its_data() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(dir.join("kharka.toml"), "strict = true\n").unwrap();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    let _b = start_daemon(dir, "b", Some(&target));
    wait_until("a is strict", || collections(dir, "a")["strict"] == true);
    drop(hub);
    wait_until("a sees the hub gone", || !synced(dir, "a"));

    let schema = write_schema(dir, "note.json", json!({ "type": "object", "required": ["text"] }));
    assert!(
        run(dir, "a", &["collections", "set", "notes", "--schema", &schema])
            .status
            .success()
    );
    put(dir, "a", "notes/1", r#"{"text":"written offline"}"#);
    let _hub = start_hub(dir);
    wait_until("b has the data", || get(dir, "b", "notes/1").is_some());
    assert!(collections(dir, "b")["collections"].get("notes").is_some());
    assert!(
        stdout(&run(dir, "b", &["ls", "_rejected/"])).is_empty(),
        "nothing was refused"
    );
}
