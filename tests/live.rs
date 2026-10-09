//! Changes in running operation: collections reloaded by the hub, and daemons that sync only part of the keys.

mod common;

use std::fs;
use std::path::Path;
use std::process::Command;

use common::*;
use serde_json::Value;

fn hub_target(dir: &Path) -> String {
    hub_socket(dir).display().to_string()
}

fn synced(dir: &Path, node: &str) -> bool {
    status(dir, node)["online"] == true
}

fn collections_json(dir: &Path, node: &str) -> Value {
    let all: Value = serde_json::from_str(&stdout(&run(dir, node, &["collections", "--json"]))).unwrap();
    all["collections"].clone()
}

#[test]
fn the_hub_reloads_its_collections_while_daemons_stay_connected() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(
        dir.join("kharka.toml"),
        "[collections.status]\nkind = \"state\"\nttl = \"30s\"\n",
    )
    .unwrap();
    let hub = start_hub(dir);
    let a = start_daemon(dir, "a", Some(&hub_target(dir)));
    wait_until("a synced", || synced(dir, "a"));
    let shown = stdout(&run(dir, "a", &["collections"]));
    assert!(
        shown.contains("status  state   ttl 30s    no schema  protected"),
        "{shown}"
    );
    assert!(
        shown.ends_with("(open: keys without a collection are not checked)"),
        "{shown}"
    );

    // a new collection with a schema, written while everything runs
    fs::write(dir.join("task.json"), r#"{"type":"object","required":["title"]}"#).unwrap();
    fs::write(
        dir.join("kharka.toml"),
        "[collections.status]\nkind = \"state\"\nttl = \"30s\"\n\n[collections.tasks]\nschema = \"task.json\"\n",
    )
    .unwrap();
    wait_until("a has the new collection", || {
        collections_json(dir, "a").get("tasks").is_some()
    });
    let refused = run(dir, "a", &["put", "tasks/1", "{}"]);
    assert!(stderr(&refused).contains("title is missing"), "{}", stderr(&refused));
    put(dir, "a", "tasks/1", r#"{"title":"x"}"#);
    assert_eq!(a.log().matches("synced with").count(), 1, "no reconnect: {}", a.log());
    assert!(hub.log().contains("collections reloaded: status, tasks"));

    // a schema file changes on its own
    fs::write(
        dir.join("task.json"),
        r#"{"type":"object","required":["title","status"]}"#,
    )
    .unwrap();
    wait_until("a has the stricter schema", || {
        collections_json(dir, "a")["tasks"]["schema"]["required"]
            .as_array()
            .map(Vec::len)
            == Some(2)
    });

    // a broken config changes nothing
    fs::write(dir.join("kharka.toml"), "[collections.tasks\n").unwrap();
    wait_until("the hub noticed", || {
        hub.log().contains("the collections stay as they were")
    });
    assert!(collections_json(dir, "a").get("tasks").is_some());

    // SIGHUP reads it again on request
    fs::write(dir.join("kharka.toml"), "[collections.tasks]\nschema = \"task.json\"\n").unwrap();
    wait_until("reloaded", || collections_json(dir, "a").get("status").is_none());
    Command::new("kill")
        .args(["-HUP", &hub.pid().to_string()])
        .status()
        .unwrap();
    wait_until("the hub answered the signal", || {
        hub.log().contains("collections unchanged: tasks")
    });
}

#[test]
fn a_config_is_checked_without_a_daemon() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(
        dir.join("ok.toml"),
        "[collections.tasks]\nschema = { type = \"object\", required = [\"title\"] }\n",
    )
    .unwrap();
    let ok = kharka(dir)
        .args(["collections", "--check"])
        .arg(dir.join("ok.toml"))
        .output()
        .unwrap();
    assert!(ok.status.success());
    assert!(
        stdout(&ok).contains("tasks  record             schema: object, required: title"),
        "{}",
        stdout(&ok)
    );
    fs::write(
        dir.join("bad.toml"),
        "[collections.tasks]\nschema = { type = \"thing\" }\n",
    )
    .unwrap();
    let bad = kharka(dir)
        .args(["collections", "--check"])
        .arg(dir.join("bad.toml"))
        .output()
        .unwrap();
    assert_eq!(bad.status.code(), Some(1));
    assert!(stderr(&bad).contains("unknown type"), "{}", stderr(&bad));
}

#[test]
fn a_daemon_syncs_only_its_prefixes() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    put(dir, "a", "tasks/1", r#""task""#);
    put(dir, "a", "status/a", r#""status""#);

    let b = start_daemon_with(dir, "b", Some(&target), &["--sync", "tasks/"]);
    wait_until("b has the task", || get(dir, "b", "tasks/1").is_some());
    let outside = run(dir, "b", &["get", "status/a"]);
    assert_eq!(outside.status.code(), Some(1));
    assert!(
        stderr(&outside).contains("not synced to this machine, which syncs tasks/"),
        "{}",
        stderr(&outside)
    );
    assert!(
        !run(dir, "b", &["put", "status/b", "1"]).status.success(),
        "writing outside is refused"
    );
    assert!(!run(dir, "b", &["ls", "status/"]).status.success());
    assert!(!run(dir, "b", &["watch", "status/"]).status.success());
    let everything = stdout(&run(dir, "b", &["ls"]));
    assert!(everything.contains("tasks/1") && everything.contains("_nodes/a") && !everything.contains("status/a"));
    assert_eq!(status(dir, "b")["sync"], serde_json::json!(["tasks/"]));

    // live changes respect the scope too
    put(dir, "a", "tasks/2", "2");
    put(dir, "a", "status/a", r#""changed""#);
    wait_until("b has the new task", || get(dir, "b", "tasks/2").is_some());
    assert_eq!(
        status(dir, "b")["entries"].as_u64(),
        Some(4),
        "tasks/1, tasks/2, _nodes/a, _nodes/b"
    );

    // a wider scope fetches what it did not have
    drop(b);
    let b = start_daemon_with(dir, "b", Some(&target), &["--sync", "tasks/,status/"]);
    wait_until("b has the status now", || {
        get(dir, "b", "status/a").as_deref() == Some(r#""changed""#)
    });

    // a narrower one removes what it no longer holds
    drop(b);
    let _b = start_daemon_with(dir, "b", Some(&target), &["--sync", "status/"]);
    wait_until("b synced", || synced(dir, "b"));
    assert!(
        stdout(&run(dir, "b", &["ls"]))
            .lines()
            .all(|key| !key.starts_with("tasks/"))
    );
    assert!(
        fs::read_to_string(dir.join("b.log"))
            .unwrap()
            .contains("removed 2 entries")
    );
}

#[test]
fn a_write_made_before_the_scope_shrank_still_reaches_the_hub() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    let b = start_daemon_with(dir, "b", Some(&target), &["--sync", "tasks/"]);
    wait_until("b synced", || synced(dir, "b"));
    drop(hub);
    wait_until("b sees the hub gone", || !synced(dir, "b"));
    put(dir, "b", "tasks/9", r#""offline""#);
    drop(b);
    // b now syncs something else; its pending write must still travel, and then go
    let _b = start_daemon_with(dir, "b", Some(&target), &["--sync", "status/"]);
    let _hub = start_hub(dir);
    wait_until("a got b's write", || {
        get(dir, "a", "tasks/9").as_deref() == Some(r#""offline""#)
    });
    wait_until("b has nothing pending", || status(dir, "b")["pending"] == 0);
    assert_eq!(
        status(dir, "b")["entries"].as_u64(),
        Some(2),
        "only _nodes/a and _nodes/b stay"
    );
}
