//! The heartbeat, shortened for tests: a quiet connection stays up, a silent one is closed (by the hub, and by the
//! daemon), and the daemon connects anew.

mod common;

use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

use common::*;

/// A ping every 300 ms, so a connection is silent after 900 ms.
const PING_MS: &str = "300";
const SILENCE: Duration = Duration::from_millis(900);

fn hub_target(dir: &Path) -> String {
    hub_socket(dir).display().to_string()
}

fn synced(dir: &Path, node: &str) -> bool {
    status(dir, node)["online"] == true
}

fn online(dir: &Path, observer: &str, node: &str) -> bool {
    get(dir, observer, &format!("_nodes/{node}")).is_some_and(|value| value.contains(r#""online":true"#))
}

fn start(dir: &Path) -> (Process, Process, Process) {
    let hub = start_hub_from(dir, hub_command(dir).args(["--heartbeat-ms", PING_MS]));
    let target = hub_target(dir);
    let daemon = |node| {
        let mut command = daemon_command(dir, node, Some(&target));
        start_daemon_from(dir, node, command.args(["--heartbeat-ms", PING_MS]))
    };
    let (a, b) = (daemon("a"), daemon("b"));
    wait_until("both synced", || synced(dir, "a") && synced(dir, "b"));
    (hub, a, b)
}

/// Stops (`STOP`) or resumes (`CONT`) a process: it stays connected, but says nothing.
fn signal(process: &Process, name: &str) {
    let sent = Command::new("kill")
        .arg(format!("-{name}"))
        .arg(process.pid().to_string())
        .status()
        .unwrap();
    assert!(sent.success());
}

#[test]
fn the_hub_closes_a_silent_daemon_which_then_connects_anew() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let (hub, a, _b) = start(dir);
    assert!(hub.log().contains("--heartbeat-ms is set"));

    // nothing to say for twice the silence: the pings keep the connection up
    thread::sleep(2 * SILENCE);
    assert_eq!(a.log().matches("synced with").count(), 1, "{}", a.log());
    assert!(online(dir, "b", "a"));

    // a stops answering: the hub closes its connection and says so
    signal(&a, "STOP");
    wait_until("the hub found a silent", || hub.log().contains("a went silent"));
    wait_until("b sees a offline", || !online(dir, "b", "a"));
    signal(&a, "CONT");

    wait_until("a connected anew", || a.log().matches("synced with").count() == 2);
    wait_until("b sees a online", || online(dir, "b", "a"));
    put(dir, "a", "after", "1");
    wait_until("a's write travels again", || get(dir, "b", "after").is_some());
}

#[test]
fn a_daemon_leaves_a_silent_hub_and_connects_anew() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let (hub, a, _b) = start(dir);

    // the hub stops: its connections stay open, but no ping comes
    signal(&hub, "STOP");
    wait_until("a found the hub silent", || a.log().contains("the hub went silent"));
    assert!(!synced(dir, "a"));
    signal(&hub, "CONT");

    wait_until("a connected anew", || a.log().matches("synced with").count() == 2);
    put(dir, "a", "after", "1");
    wait_until("a's write travels again", || get(dir, "b", "after").is_some());
}
