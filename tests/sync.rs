//! A hub and daemons: changes travel, offline writes arrive later, the latest write wins, a hub that lost its data is
//! filled again, the ssh transport works through `kharka bridge`, a stamp from the future is refused (and does not hold
//! up later writes), stale writes are answered in frames that fit, and a daemon too slow for the stream is caught up
//! from the log.

mod common;

use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::process::Stdio;

use common::*;
use kharka::entry::Entry;
use kharka::hlc::{Hlc, wall_ms};
use kharka::store::{Options, Store};
use serde_json::{Value, json};

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

#[test]
fn a_write_stamped_far_ahead_of_the_hub_is_refused_and_kept() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _b = start_daemon(dir, "b", Some(&target));
    // a's replica, made beforehand: it holds a write a made while its clock was ten minutes ahead (put into the log as
    // a write lands there, pending)
    let ahead = Hlc {
        wall_ms: wall_ms() + 10 * 60 * 1000,
        counter: 0,
        node: "a".into(),
    };
    {
        let mut store = Store::open(&dir.join("a"), "a", Options::default()).unwrap();
        let written = Entry::new("tasks/ahead", Some(json!("ahead")), ahead.clone());
        store.from_hub(written).unwrap();
        assert_eq!(store.pending().len(), 1);
    }
    let a = start_daemon(dir, "a", Some(&target));

    // kept on the hub under _rejected/, answered with `rejected`
    let record_key = format!("_rejected/a/{}-{}", ahead.wall_ms, ahead.counter);
    wait_until("the record of the refused write reached a", || {
        get(dir, "a", &record_key).is_some()
    });
    let record: Value = serde_json::from_str(&get(dir, "b", &record_key).unwrap()).unwrap();
    assert_eq!(record["key"], "tasks/ahead");
    assert_eq!(record["value"], "ahead");
    assert_eq!(record["hlc"], ahead.to_string());
    assert!(
        record["reason"].as_str().unwrap().contains("five minutes ahead"),
        "{record}"
    );
    wait_until("a got the answer", || {
        a.log().contains("the hub did not take tasks/ahead")
    });
    // not restated: a restatement comes with the record and would replace a's version; a keeps it, but no longer
    // pushes it
    assert_eq!(get(dir, "a", "tasks/ahead").as_deref(), Some(r#""ahead""#));
    assert_eq!(get(dir, "b", "tasks/ahead"), None);
    let before = status(dir, "a");
    assert_eq!(
        (before["refused"].as_u64(), before["pending"].as_u64()),
        (Some(1), Some(0))
    );

    // nor after a restart; and a's clock did not follow that stamp: its next write is one the hub takes
    drop(a);
    let a = start_daemon(dir, "a", Some(&target));
    wait_until("a synced", || synced(dir, "a"));
    put(dir, "a", "tasks/next", "1");
    wait_until("b has a's next write", || {
        get(dir, "b", "tasks/next").as_deref() == Some("1")
    });
    assert_eq!(
        hub.log().matches("refused tasks/ahead from a").count(),
        1,
        "{}",
        hub.log()
    );

    // a write to the key replaces the version too far ahead, on a and everywhere, also after a restart
    put(dir, "a", "tasks/ahead", r#""again""#);
    assert_eq!(get(dir, "a", "tasks/ahead").as_deref(), Some(r#""again""#));
    wait_until("b has it", || {
        get(dir, "b", "tasks/ahead").as_deref() == Some(r#""again""#)
    });
    wait_until("a counts nothing refused or pending", || {
        let now = status(dir, "a");
        (now["refused"].as_u64(), now["pending"].as_u64()) == (Some(0), Some(0))
    });
    drop(a);
    let _a = start_daemon(dir, "a", Some(&target));
    wait_until("a synced", || synced(dir, "a"));
    assert_eq!(get(dir, "a", "tasks/ahead").as_deref(), Some(r#""again""#));
    let after = status(dir, "a");
    assert_eq!(
        (after["refused"].as_u64(), after["pending"].as_u64()),
        (Some(0), Some(0))
    );
    assert_eq!(hub.log().matches("refused").count(), 1, "{}", hub.log());
}

#[test]
fn every_refusal_reaches_the_daemon() {
    // more refusals at once than the 1024 direct answers a connection may have waiting (ANSWERS in src/hub.rs)
    const WRITES: u32 = 1500;
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    {
        let options = Options {
            sync: false,
            ..Options::default()
        };
        let mut store = Store::open(&dir.join("a"), "a", options).unwrap();
        let ahead = wall_ms() + 10 * 60 * 1000;
        for n in 0..WRITES {
            let hlc = Hlc {
                wall_ms: ahead,
                counter: n,
                node: "a".into(),
            };
            store
                .from_hub(Entry::new(format!("tasks/{n}"), Some(json!(n)), hlc))
                .unwrap();
        }
    }
    let _hub = start_hub(dir);
    let _a = start_daemon(dir, "a", Some(&hub_target(dir)));
    wait_until("a has every refusal", || {
        let status = status(dir, "a");
        (status["refused"].as_u64(), status["pending"].as_u64()) == (Some(WRITES.into()), Some(0))
    });
}

#[test]
fn stale_writes_are_answered_in_frames_that_fit() {
    // the hub holds more than a frame's 8 MB of large values; a pushes an older, small write to each key
    const KEYS: u64 = 12;
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let options = || Options {
        sync: false,
        ..Options::default()
    };
    {
        let mut store = Store::open(&dir.join("a"), "a", options()).unwrap();
        for n in 0..KEYS {
            store.put(&format!("big/{n}"), json!(n), None).unwrap();
        }
    }
    {
        let mut store = Store::open(&dir.join("hub"), "hub", options()).unwrap();
        let large = json!("x".repeat(900_000));
        for n in 0..KEYS {
            let later = Hlc {
                wall_ms: wall_ms() + 1000,
                counter: 0,
                node: "b".into(),
            };
            store
                .accept(Entry::new(format!("big/{n}"), Some(large.clone()), later))
                .unwrap();
        }
    }
    let hub = start_hub(dir);
    // a syncs something else: the catch-up leaves big/ out, so a's writes go to the hub, which already has later ones
    let _a = start_daemon_with(dir, "a", Some(&hub_target(dir)), &["--sync", "other/"]);
    wait_until("a's writes are answered", || {
        let status = status(dir, "a");
        status["online"] == true && status["pending"] == 0
    });
    // answered on the first connection: the answer did not break it
    assert_eq!(hub.log().matches("a connected").count(), 1, "{}", hub.log());
    assert_eq!(get(dir, "a", "big/0"), None);
}

#[test]
fn a_daemon_behind_the_stream_is_caught_up_from_the_log() {
    // more than the 4096 changes a connection may fall behind (BACKLOG in src/hub.rs), pushed at once
    const WRITES: u64 = 6000;
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    {
        let options = Options {
            sync: false,
            ..Options::default()
        };
        let mut store = Store::open(&dir.join("b"), "b", options).unwrap();
        for n in 0..WRITES {
            store.put(&format!("bulk/{n}"), json!(n), None).unwrap();
        }
    }
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let _a = start_daemon(dir, "a", Some(&target));
    wait_until("a synced", || synced(dir, "a"));
    let _b = start_daemon(dir, "b", Some(&target));

    wait_until("a has every entry", || {
        status(dir, "a")["entries"].as_u64() == Some(WRITES + 2)
    });
    assert!(hub.log().contains("a fell"), "{}", hub.log());
    wait_until("b's writes are all confirmed", || status(dir, "b")["pending"] == 0);
    assert_eq!(get(dir, "a", "bulk/0").as_deref(), Some("0"));
    assert_eq!(
        get(dir, "a", &format!("bulk/{}", WRITES - 1)),
        Some((WRITES - 1).to_string())
    );
}
