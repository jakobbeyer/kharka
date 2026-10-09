//! Default places: the sockets, the data, the config, the node name.

use std::env;
use std::path::PathBuf;

use crate::entry::MAX_NODE;

fn var(name: &str) -> Option<PathBuf> {
    env::var_os(name).filter(|value| !value.is_empty()).map(PathBuf::from)
}

fn home() -> PathBuf {
    var("HOME").unwrap_or_else(|| PathBuf::from("/"))
}

/// Where the sockets live: `$XDG_RUNTIME_DIR/kharka` (Linux sessions and user services have it), else
/// `~/.local/state/kharka`. Not `$TMPDIR`: a launchd agent on macOS does not reliably get the same one as a shell, and
/// the service and the command line have to find the same socket.
pub fn runtime_dir() -> PathBuf {
    match var("XDG_RUNTIME_DIR") {
        Some(dir) => dir.join("kharka"),
        None => var("XDG_STATE_HOME")
            .unwrap_or_else(|| home().join(".local/state"))
            .join("kharka"),
    }
}

/// The daemon's socket, where tools connect.
pub fn socket() -> PathBuf {
    runtime_dir().join("daemon.sock")
}

/// The hub's socket, where daemons (through `kharka bridge`) connect.
pub fn hub_socket() -> PathBuf {
    runtime_dir().join("hub.sock")
}

/// Where a client looks for the daemon when no socket is given: the first of the candidates where a socket exists, else
/// the default place.
pub fn find_socket() -> PathBuf {
    candidates("daemon.sock")
        .into_iter()
        .find(|path| path.exists())
        .unwrap_or_else(socket)
}

/// Where a bridge looks for the hub, in this order.
pub fn hub_socket_candidates() -> Vec<PathBuf> {
    candidates("hub.sock")
}

/// The places a socket may be: the default one, the runtime folder systemd gives the user (`/run/user/<uid>`, which an
/// ssh session does not always announce in `$XDG_RUNTIME_DIR`), and the place without a runtime folder. A service and a
/// shell of the same user thus find each other's sockets.
fn candidates(name: &str) -> Vec<PathBuf> {
    let mut candidates = vec![runtime_dir().join(name)];
    if let Ok(meta) = std::fs::metadata(home()) {
        use std::os::unix::fs::MetadataExt;
        candidates.push(PathBuf::from(format!("/run/user/{}/kharka/{name}", meta.uid())));
    }
    candidates.push(
        var("XDG_STATE_HOME")
            .unwrap_or_else(|| home().join(".local/state"))
            .join("kharka")
            .join(name),
    );
    let mut seen = Vec::new();
    candidates.retain(|candidate| {
        let new = !seen.contains(candidate);
        seen.push(candidate.clone());
        new
    });
    candidates
}

fn data_home() -> PathBuf {
    var("XDG_DATA_HOME")
        .unwrap_or_else(|| home().join(".local/share"))
        .join("kharka")
}

/// The daemon's replica.
pub fn data() -> PathBuf {
    data_home().join("replica")
}

/// The hub's log.
pub fn hub_data() -> PathBuf {
    data_home().join("hub")
}

/// `kharka.toml`, the collections.
pub fn config() -> PathBuf {
    var("XDG_CONFIG_HOME")
        .unwrap_or_else(|| home().join(".config"))
        .join("kharka")
        .join("kharka.toml")
}

/// The host name as a node name: lower case, up to the first dot, only the characters a key segment may hold.
pub fn node() -> String {
    let host = gethostname::gethostname().to_string_lossy().to_lowercase();
    let mut name: String = host
        .split('.')
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
        .collect();
    name.truncate(MAX_NODE);
    if name.is_empty() { "node".to_string() } else { name }
}
