//! Real kharka processes in a throw-away folder.

#![allow(dead_code)]

use std::fs::File;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

pub const BIN: &str = env!("CARGO_BIN_EXE_kharka");
const PATIENCE: Duration = Duration::from_secs(30);

/// A kharka command that sees nothing of the user's own setup.
pub fn kharka(dir: &Path) -> Command {
    let mut command = Command::new(BIN);
    for var in [
        "KHARKA_SOCKET",
        "KHARKA_DATA",
        "KHARKA_NODE",
        "KHARKA_HUB",
        "KHARKA_CONFIG",
        "KHARKA_HUB_DATA",
        "KHARKA_HUB_SOCKET",
    ] {
        command.env_remove(var);
    }
    command
        .env("XDG_CONFIG_HOME", dir.join("config"))
        .env("XDG_DATA_HOME", dir.join("share"));
    command
}

/// A running process, killed (SIGKILL) when dropped; its stderr goes to `<dir>/<name>.log`.
pub struct Process {
    child: Child,
    pub log: PathBuf,
}

impl Process {
    pub fn start(dir: &Path, name: &str, command: &mut Command) -> Process {
        let log = dir.join(format!("{name}.log"));
        let stderr = File::options().create(true).append(true).open(&log).unwrap();
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .unwrap();
        Process { child, log }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Waits for the process to end on its own; its exit status.
    pub fn wait(mut self) -> std::process::ExitStatus {
        self.child.wait().unwrap()
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    while !done() {
        assert!(Instant::now() < deadline, "gave up waiting: {what}");
        thread::sleep(Duration::from_millis(25));
    }
}

pub fn wait_for_socket(path: &Path) {
    wait_until(&format!("{} to answer", path.display()), || {
        UnixStream::connect(path).is_ok()
    });
}

pub fn hub_socket(dir: &Path) -> PathBuf {
    dir.join("hub.sock")
}

pub fn socket(dir: &Path, node: &str) -> PathBuf {
    dir.join(format!("{node}.sock"))
}

/// `kharka serve` with its data in `<dir>/hub` and `<dir>/kharka.toml` as config (when there is one).
pub fn start_hub(dir: &Path) -> Process {
    let hub = Process::start(
        dir,
        "hub",
        kharka(dir)
            .arg("serve")
            .arg("--data")
            .arg(dir.join("hub"))
            .arg("--hub-socket")
            .arg(hub_socket(dir))
            .arg("--config")
            .arg(dir.join("kharka.toml")),
    );
    wait_for_socket(&hub_socket(dir));
    hub
}

/// `kharka daemon` named `node`, with its data in `<dir>/<node>`, syncing with `hub` (a hub target) when given.
pub fn start_daemon(dir: &Path, node: &str, hub: Option<&str>) -> Process {
    start_daemon_with(dir, node, hub, &[])
}

/// `start_daemon` with more arguments, e.g. `--sync`.
pub fn start_daemon_with(dir: &Path, node: &str, hub: Option<&str>, extra: &[&str]) -> Process {
    let mut command = kharka(dir);
    command
        .arg("daemon")
        .arg("--socket")
        .arg(socket(dir, node))
        .arg("--data")
        .arg(dir.join(node))
        .arg("--node")
        .arg(node)
        .arg("--config")
        .arg(dir.join(format!("{node}.toml")));
    if let Some(hub) = hub {
        command.arg("--hub").arg(hub);
    }
    command.args(extra);
    let daemon = Process::start(dir, node, &mut command);
    wait_for_socket(&socket(dir, node));
    daemon
}

/// A client command against the daemon `node`.
pub fn run(dir: &Path, node: &str, args: &[&str]) -> Output {
    kharka(dir)
        .arg("--socket")
        .arg(socket(dir, node))
        .args(args)
        .output()
        .unwrap()
}

pub fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).trim_end().to_string()
}

pub fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).trim_end().to_string()
}

/// The value at `key` on daemon `node`, if there is one.
pub fn get(dir: &Path, node: &str, key: &str) -> Option<String> {
    let output = run(dir, node, &["get", key]);
    output.status.success().then(|| stdout(&output))
}

pub fn put(dir: &Path, node: &str, key: &str, value: &str) {
    let output = run(dir, node, &["put", key, value]);
    assert!(output.status.success(), "put {key} on {node}: {}", stderr(&output));
}

/// The daemon's status as JSON.
pub fn status(dir: &Path, node: &str) -> serde_json::Value {
    serde_json::from_str(&stdout(&run(dir, node, &["status"]))).unwrap()
}
