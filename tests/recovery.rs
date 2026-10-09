//! Recovery: a hub that lost its data or had its log cut is filled again without losing anyone's writes, a refused
//! write too large to keep whole leaves the log readable, and a config file that is away for a moment changes nothing.

mod common;

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::Stdio;

use common::*;
use serde_json::Value;

fn hub_target(dir: &Path) -> String {
    hub_socket(dir).display().to_string()
}

fn synced(dir: &Path, node: &str) -> bool {
    let status = status(dir, node);
    status["online"] == true && status["pending"] == 0
}

#[test]
fn a_hub_that_lost_its_data_gets_every_nodes_writes_back() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let a = start_daemon(dir, "a", Some(&target));
    let _b = start_daemon(dir, "b", Some(&target));
    put(dir, "a", "topics/from-a", r#""a's""#);
    put(dir, "b", "topics/from-b", r#""b's""#);
    wait_until("both have both", || {
        get(dir, "a", "topics/from-b").is_some() && get(dir, "b", "topics/from-a").is_some()
    });

    // the hub loses everything; b reconnects first and restores what it holds, a's writes included
    drop(hub);
    drop(a);
    fs::remove_dir_all(dir.join("hub")).unwrap();
    let _hub = start_hub(dir);
    wait_until("b restored the new hub", || synced(dir, "b"));
    assert_eq!(
        get(dir, "b", "topics/from-a").as_deref(),
        Some(r#""a's""#),
        "a's write survived on b"
    );

    let _a = start_daemon(dir, "a", Some(&target));
    wait_until("a synced", || synced(dir, "a"));
    assert_eq!(get(dir, "a", "topics/from-a").as_deref(), Some(r#""a's""#));
    assert_eq!(get(dir, "a", "topics/from-b").as_deref(), Some(r#""b's""#));
    assert_eq!(stdout(&run(dir, "a", &["ls", "_rejected/"])), "", "nothing was refused");
    wait_until("both are online again", || {
        get(dir, "a", "_nodes/a").is_some_and(|v| v.contains(r#""online":true"#))
            && get(dir, "a", "_nodes/b").is_some_and(|v| v.contains(r#""online":true"#))
    });
}

#[test]
fn a_cut_hub_log_starts_a_new_epoch_and_the_daemons_restore_it() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    put(dir, "a", "topics/kept", r#""kept""#);
    wait_until("the hub has it", || synced(dir, "a"));
    drop(hub);
    // a torn last record, as after a crash in the middle of a write
    fs::OpenOptions::new()
        .append(true)
        .open(dir.join("hub/data.log"))
        .unwrap()
        .write_all(&[0, 0, 0, 40, 1, 2])
        .unwrap();
    let hub = start_hub(dir);
    wait_until("the hub took a new epoch", || hub.log().contains("a new epoch"));
    wait_until("a restored it", || synced(dir, "a"));
    let _b = start_daemon(dir, "b", Some(&target));
    wait_until("b gets what a still had", || get(dir, "b", "topics/kept").is_some());
}

#[test]
fn a_refused_write_too_large_to_keep_whole_leaves_the_log_readable() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(dir.join("kharka.toml"), "[collections.tasks]\n").unwrap();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    wait_until("a synced", || synced(dir, "a"));

    // while the hub is away, a writes a big value; meanwhile the hub's schema stops allowing it
    drop(hub);
    wait_until("a sees the hub gone", || status(dir, "a")["online"] == false);
    let big = format!("\"{}\"", "x".repeat(700_000));
    let mut writer = kharka(dir)
        .arg("--socket")
        .arg(socket(dir, "a"))
        .args(["put", "tasks/big", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    writer.stdin.take().unwrap().write_all(big.as_bytes()).unwrap();
    assert!(writer.wait().unwrap().success());
    fs::write(
        dir.join("kharka.toml"),
        "[collections.tasks]\nschema = { enum = [\"small\"] }\n",
    )
    .unwrap();
    let hub = start_hub(dir);
    wait_until("the refusal is kept", || {
        !stdout(&run(dir, "a", &["ls", "_rejected/a/"])).is_empty()
    });
    let record_key = stdout(&run(dir, "a", &["ls", "_rejected/a/"]));
    let record: Value = serde_json::from_str(&get(dir, "a", &record_key).unwrap()).unwrap();
    assert_eq!(record["value"], Value::Null, "too large to keep whole");
    assert_eq!(record["value_bytes"], 700_002);
    assert!(record["reason"].as_str().unwrap().len() < 400);

    // after a restart the hub's log is whole: a later write is still there
    put(dir, "a", "plain/after", "1");
    wait_until("the hub has it", || synced(dir, "a"));
    drop(hub);
    let hub = start_hub(dir);
    let _b = start_daemon(dir, "b", Some(&target));
    wait_until("b gets the later write", || get(dir, "b", "plain/after").is_some());
    assert!(!hub.log().contains("cut off"), "{}", hub.log());
}

#[test]
fn a_config_file_away_for_a_moment_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(dir.join("kharka.toml"), "strict = true\n[collections.tasks]\n").unwrap();
    let hub = start_hub(dir);
    let _a = start_daemon(dir, "a", Some(&hub_target(dir)));
    wait_until("a is strict", || {
        stdout(&run(dir, "a", &["collections"])).contains("(strict")
    });
    fs::remove_file(dir.join("kharka.toml")).unwrap();
    wait_until("the hub noticed", || hub.log().contains("is gone"));
    std::thread::sleep(std::time::Duration::from_millis(300));
    let shown = stdout(&run(dir, "a", &["collections"]));
    assert!(shown.contains("tasks") && shown.contains("(strict"), "{shown}");
    assert!(
        !run(dir, "a", &["put", "other/1", "1"]).status.success(),
        "still strict"
    );
}

#[test]
fn a_second_daemon_under_the_same_name_is_noticed() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _first = start_daemon(dir, "a", Some(&target));
    wait_until("a synced", || synced(dir, "a"));
    let mut twin = kharka(dir);
    twin.arg("daemon")
        .arg("--socket")
        .arg(dir.join("twin.sock"))
        .arg("--data")
        .arg(dir.join("twin"))
        .args(["--node", "a", "--hub", &target, "--config"])
        .arg(dir.join("none.toml"));
    let _twin = Process::start(dir, "twin", &mut twin);
    wait_until("the hub says so", || hub.log().contains("connected again"));
}

#[test]
fn deleting_a_missing_key_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _a = start_daemon(dir, "a", None);
    assert_eq!(run(dir, "a", &["del", "nothing/here"]).status.code(), Some(3));
    let refused = run(dir, "a", &["daemon", "--hub=-oProxyCommand=touch x"]);
    assert!(stderr(&refused).contains("not a hub"), "{}", stderr(&refused));
}
