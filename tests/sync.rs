//! A hub and daemons: changes travel, offline writes arrive later, the latest write wins, a hub that lost its data is
//! filled again, and the ssh transport works through `kharka bridge`.

mod common;

use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::process::Stdio;

use common::*;
use serde_json::Value;

fn hub_target(dir: &std::path::Path) -> String {
    hub_socket(dir).display().to_string()
}

fn synced(dir: &std::path::Path, node: &str) -> bool {
    status(dir, node)["online"] == true
}

#[test]
fn changes_travel_through_the_hub() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _hub = start_hub(dir);
    let hub = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&hub));
    let _b = start_daemon(dir, "b", Some(&hub));

    put(dir, "a", "topics/1", r#"{"title":"from a"}"#);
    wait_until("b has a's write", || {
        get(dir, "b", "topics/1").as_deref() == Some(r#"{"title":"from a"}"#)
    });
    put(dir, "b", "topics/1", r#"{"title":"from b"}"#);
    wait_until("a has b's write", || {
        get(dir, "a", "topics/1").as_deref() == Some(r#"{"title":"from b"}"#)
    });
    assert!(run(dir, "a", &["del", "topics/1"]).status.success());
    wait_until("b has the deletion", || get(dir, "b", "topics/1").is_none());

    // the hub says who is connected
    wait_until("b knows a is online", || {
        get(dir, "b", "_nodes/a").is_some_and(|value| value.contains(r#""online":true"#))
    });
    wait_until("nothing pending", || {
        status(dir, "a")["pending"] == 0 && status(dir, "b")["pending"] == 0
    });
}

#[test]
fn a_watch_sees_other_machines() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _hub = start_hub(dir);
    let hub = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&hub));
    let _b = start_daemon(dir, "b", Some(&hub));
    wait_until("b synced", || synced(dir, "b"));

    let mut watch = kharka(dir)
        .arg("--socket")
        .arg(socket(dir, "b"))
        .args(["watch", "status/"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(watch.stdout.take().unwrap()).lines();
    put(dir, "a", "status/a", r#"{"agents":[]}"#);
    let event: Value = serde_json::from_str(&lines.next().unwrap().unwrap()).unwrap();
    assert_eq!(event["entry"]["key"], "status/a");
    assert!(event["entry"]["seq"].as_u64().unwrap() > 0);
    watch.kill().unwrap();
    watch.wait().unwrap();
}

#[test]
fn offline_writes_arrive_later_and_the_latest_wins() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    let _b = start_daemon(dir, "b", Some(&target));
    wait_until("both synced", || synced(dir, "a") && synced(dir, "b"));

    drop(hub);
    wait_until("both see the hub gone", || !synced(dir, "a") && !synced(dir, "b"));
    // both change the same key while the hub is away; b writes later and wins everywhere
    put(dir, "a", "shared", r#""a""#);
    put(dir, "a", "only-a", "1");
    std::thread::sleep(std::time::Duration::from_millis(5));
    put(dir, "b", "shared", r#""b""#);
    assert_eq!(status(dir, "a")["pending"], 2);

    let _hub = start_hub(dir);
    wait_until("a has only-a and b's version", || {
        get(dir, "b", "only-a").is_some() && get(dir, "a", "shared").as_deref() == Some(r#""b""#)
    });
    wait_until("b keeps its own", || {
        get(dir, "b", "shared").as_deref() == Some(r#""b""#)
    });
    wait_until("nothing pending", || {
        status(dir, "a")["pending"] == 0 && status(dir, "b")["pending"] == 0
    });
}

#[test]
fn a_hub_that_lost_its_data_is_filled_again() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let a = start_daemon(dir, "a", Some(&target));
    put(dir, "a", "topics/kept", r#""kept""#);
    wait_until("the hub has it", || status(dir, "a")["pending"] == 0);
    drop(hub);
    drop(a);
    fs::remove_dir_all(dir.join("hub")).unwrap();

    let _hub = start_hub(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    let _b = start_daemon(dir, "b", Some(&target));
    wait_until("b gets what only a still had", || {
        get(dir, "b", "topics/kept").as_deref() == Some(r#""kept""#)
    });
    assert!(
        fs::read_to_string(dir.join("a.log"))
            .unwrap()
            .contains("another one or lost its data")
    );
}

#[test]
fn the_hub_hands_out_its_collections() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    fs::write(
        dir.join("kharka.toml"),
        "strict = true\n[collections.status]\nkind = \"state\"\nttl = \"30s\"\n",
    )
    .unwrap();
    let _hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    wait_until("a has the collections", || {
        stdout(&run(dir, "a", &["collections"])).contains("status")
    });
    let refused = run(dir, "a", &["put", "topics/1", "1"]);
    assert!(stderr(&refused).contains("(strict)"), "{}", stderr(&refused));
    put(dir, "a", "status/a", "{}");
}

#[test]
fn through_ssh_and_the_bridge() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    // an `ssh` that runs the bridge here, whatever host it is given
    let bin = dir.join("bin");
    fs::create_dir(&bin).unwrap();
    fs::write(
        bin.join("ssh"),
        format!(
            "#!/bin/sh\nexec '{BIN}' bridge --hub-socket '{}'\n",
            hub_socket(dir).display()
        ),
    )
    .unwrap();
    fs::set_permissions(bin.join("ssh"), fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap_or_default());

    let mut command = kharka(dir);
    command
        .env("PATH", path)
        .args(["daemon", "--node", "remote", "--hub", "server", "--data"])
        .arg(dir.join("remote"))
        .arg("--socket")
        .arg(socket(dir, "remote"))
        .arg("--config")
        .arg(dir.join("none.toml"));
    let _remote = Process::start(dir, "remote", &mut command);
    wait_for_socket(&socket(dir, "remote"));
    let _local = start_daemon(dir, "local", Some(&hub_target(dir)));

    put(dir, "remote", "status/remote", r#"{"via":"ssh"}"#);
    wait_until("local has the write sent through the bridge", || {
        get(dir, "local", "status/remote").is_some()
    });
    put(dir, "local", "back", "1");
    wait_until("and the way back", || get(dir, "remote", "back").is_some());
    assert_eq!(status(dir, "remote")["hub"], "ssh:server");

    // the hub goes: the bridge ends at once, and with it the connection, without waiting for a write or the heartbeat
    drop(hub);
    let started = std::time::Instant::now();
    wait_until("remote sees the hub gone", || status(dir, "remote")["online"] == false);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
}
