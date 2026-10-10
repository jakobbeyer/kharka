//! The daemon's side of the sync: one long-lived connection to the hub, re-established for as long as the daemon runs.
//!
//! A session: `hello` with the position in the hub's sequence, the hub's `welcome`, the changes missed meanwhile up to
//! `caught_up`, then everything still pending here, then live in both directions, with a ping every 15 s each way.

use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::daemon::Shared;
use crate::entry::{COLLECTIONS, Entry, NODES, SETTINGS};
use crate::frame::{self, Frame, PROTOCOL};
use crate::hub::Heartbeat;
use crate::paths;
use crate::scope::Scope;
use crate::storage::write_private;

const FIRST_RETRY: Duration = Duration::from_secs(1);
const LAST_RETRY: Duration = Duration::from_secs(30);
const HANDSHAKE: Duration = Duration::from_secs(30);

/// The command ssh runs on the hub's machine. Non-interactive shells often lack the folders kharka is installed in, hence
/// the search.
const REMOTE_BRIDGE: &str = r#"sh -c 'k=$(command -v kharka || ls "$HOME/.cargo/bin/kharka" "$HOME/.local/bin/kharka" /opt/homebrew/bin/kharka /usr/local/bin/kharka 2>/dev/null | head -n 1); exec "${k:-kharka}" bridge'"#;

/// How a daemon reaches the hub.
#[derive(Clone, Debug, PartialEq)]
pub enum HubTarget {
    /// The hub's socket on this machine.
    Unix(PathBuf),
    /// Through `ssh <destination> kharka bridge`.
    Ssh(String),
}

impl HubTarget {
    /// `local` (the hub's default socket here), a socket path (`/…` or `unix:…`), or an ssh destination (`server`,
    /// `user@host`, `ssh:host`). A destination that starts with `-` would be read as an ssh option, and is refused.
    pub fn parse(text: &str) -> Result<HubTarget, String> {
        if text == "local" {
            Ok(HubTarget::Unix(paths::hub_socket()))
        } else if let Some(path) = text.strip_prefix("unix:") {
            Ok(HubTarget::Unix(PathBuf::from(path)))
        } else if text.starts_with('/') {
            Ok(HubTarget::Unix(PathBuf::from(text)))
        } else {
            let destination = text.strip_prefix("ssh:").unwrap_or(text);
            if destination.is_empty() || destination.starts_with('-') || destination.contains(char::is_whitespace) {
                return Err(format!(
                    "not a hub: {text:?} (an ssh destination like server or user@host, local, or a socket path)"
                ));
            }
            Ok(HubTarget::Ssh(destination.to_string()))
        }
    }
}

impl fmt::Display for HubTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HubTarget::Unix(path) => write!(f, "unix:{}", path.display()),
            HubTarget::Ssh(destination) => write!(f, "ssh:{destination}"),
        }
    }
}

/// Keeps a session with the hub up, retrying with a growing pause (1 s up to 30 s).
pub(crate) async fn run(shared: Arc<Shared>, hub: HubTarget, data: PathBuf, heartbeat: Heartbeat) {
    let mut pause = FIRST_RETRY;
    let mut last_problem = String::new();
    loop {
        let started = Instant::now();
        let problem = match session(&shared, &hub, &data, heartbeat).await {
            Ok(()) => "the hub closed the connection".to_string(),
            Err(err) => format!("{err:#}"),
        };
        let was_online = shared.online.send_replace(false);
        // a hub that stays away is reported once, not every 30 seconds
        if was_online || problem != last_problem {
            eprintln!("kharka: hub {hub}: {problem}; retrying");
        }
        last_problem = problem;
        if started.elapsed() > LAST_RETRY {
            pause = FIRST_RETRY;
        }
        tokio::time::sleep(pause).await;
        pause = (pause * 2).min(LAST_RETRY);
    }
}

struct Connection {
    reader: FramedRead<Box<dyn AsyncRead + Unpin + Send>, LengthDelimitedCodec>,
    writer: FramedWrite<Box<dyn AsyncWrite + Unpin + Send>, LengthDelimitedCodec>,
    /// ssh, ended when the session ends.
    _child: Option<Child>,
}

async fn connect(hub: &HubTarget) -> anyhow::Result<Connection> {
    let (reader, writer, child): (Box<dyn AsyncRead + Unpin + Send>, Box<dyn AsyncWrite + Unpin + Send>, _) = match hub
    {
        HubTarget::Unix(path) => {
            let stream = UnixStream::connect(path)
                .await
                .with_context(|| format!("no hub listens on {}", path.display()))?;
            let (reader, writer) = stream.into_split();
            (Box::new(reader), Box::new(writer), None)
        }
        HubTarget::Ssh(destination) => {
            let mut child = Command::new("ssh")
                .args([
                    "-T",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "ConnectTimeout=10",
                    "-o",
                    "ServerAliveInterval=15",
                    "-o",
                    "ServerAliveCountMax=3",
                    "--",
                ])
                .arg(destination)
                .arg(REMOTE_BRIDGE)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .context("starting ssh")?;
            if let Some(stderr) = child.stderr.take() {
                let destination = destination.clone();
                tokio::spawn(async move {
                    let mut lines = BufReader::new(stderr).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        eprintln!("kharka: ssh {destination}: {line}");
                    }
                });
            }
            let stdout = child.stdout.take().context("ssh without stdout")?;
            let stdin = child.stdin.take().context("ssh without stdin")?;
            (Box::new(stdout), Box::new(stdin), Some(child))
        }
    };
    Ok(Connection {
        reader: FramedRead::new(reader, frame::codec()),
        writer: FramedWrite::new(writer, frame::codec()),
        _child: child,
    })
}

async fn session(shared: &Arc<Shared>, hub: &HubTarget, data: &Path, heartbeat: Heartbeat) -> anyhow::Result<()> {
    let Connection {
        mut reader,
        mut writer,
        _child,
    } = connect(hub).await?;
    // subscribed before the hello: a local write made during the catch-up is pushed with the pending ones or after them
    let mut local = shared.changes.subscribe();
    let full_catch_up = shared.full_catch_up.load(Ordering::SeqCst);
    let (node, since) = {
        let store = shared.store();
        // the position: the highest sequence number held, or the last caught_up (which also counts the entries this
        // replica does not sync), whichever is higher
        let since = if full_catch_up {
            0
        } else {
            store.last_seq().max(saved_position(data))
        };
        (store.node().to_string(), since)
    };
    let epoch = saved_epoch(data);
    frame::send(
        &mut writer,
        &Frame::Hello {
            version: PROTOCOL,
            node: node.clone(),
            epoch: epoch.clone(),
            since,
            prefixes: shared.scope.prefixes().to_vec(),
        },
    )
    .await?;
    let welcome = timeout(HANDSHAKE, frame::recv(&mut reader))
        .await
        .context("no answer from the hub")??;
    let (hub_epoch, reset) = match welcome {
        Some(Frame::Welcome { epoch, reset }) => (epoch, reset),
        Some(Frame::Refused { reason }) => bail!("the hub refused: {reason}"),
        Some(other) => bail!("the hub sent {} instead of welcome", other.name()),
        None => bail!("the hub closed the connection"),
    };
    if reset {
        if epoch.is_some() {
            eprintln!("kharka: the hub is another one or lost its data: everything here goes to it again");
        }
        {
            let mut store = shared.store();
            // what only the hub writes comes from the new hub, in the catch-up that follows
            store
                .forget_seqs(hub_owned)
                .context("forgetting the old hub's sequence numbers and entries")?;
        }
        let _ = fs::remove_file(data.join("position"));
    }
    let caught_up = loop {
        match timeout(HANDSHAKE, frame::recv(&mut reader))
            .await
            .context("the catch-up stalled")??
        {
            Some(Frame::Changes { entries }) => apply(shared, entries)?,
            Some(Frame::CaughtUp { seq }) => break seq,
            Some(Frame::Rejected {
                key,
                hlc,
                reason,
                restated,
            }) => {
                if rejected(shared, key, hlc, &reason, restated) {
                    save_refused(data, &shared.refused());
                }
            }
            Some(Frame::Ping) => frame::send(&mut writer, &Frame::Pong).await?,
            Some(other) => bail!("the hub sent {} during the catch-up", other.name()),
            None => bail!("the hub closed the connection during the catch-up"),
        }
    };
    save_epoch(data, &hub_epoch)?;
    save_position(data, caught_up)?;
    if full_catch_up {
        save_scope(data, &shared.scope)?;
        shared.full_catch_up.store(false, Ordering::SeqCst);
    }
    shared.online.send_replace(true);
    eprintln!("kharka: synced with {hub} up to {caught_up}");
    push_pending(shared, data, &node, &mut writer).await?;
    let mut heard = Instant::now();
    let mut unsaved = Unsaved {
        shared,
        data,
        noted: false,
    };
    loop {
        let silence = tokio::time::sleep_until((heard + heartbeat.silence).into());
        tokio::select! {
            incoming = frame::recv(&mut reader) => {
                heard = Instant::now();
                match incoming? {
                    Some(Frame::Changes { entries }) => apply(shared, entries)?,
                    Some(Frame::Rejected { key, hlc, reason, restated }) => {
                        unsaved.noted |= rejected(shared, key, hlc, &reason, restated);
                    }
                    Some(Frame::Ping) => frame::send(&mut writer, &Frame::Pong).await?,
                    Some(Frame::Pong) => {}
                    Some(other) => bail!("the hub sent {}", other.name()),
                    None => return Ok(()),
                }
                if reader.read_buffer().is_empty() {
                    unsaved.save();
                }
            }
            () = silence => bail!("the hub went silent"),
            change = local.recv() => {
                let mut batch = match change {
                    Ok(entry) => {
                        let mut batch = vec![entry];
                        let mut lagged = false;
                        loop {
                            match local.try_recv() {
                                Ok(entry) => batch.push(entry),
                                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
                                Err(TryRecvError::Lagged(_)) => {
                                    lagged = true;
                                    break;
                                }
                            }
                        }
                        if lagged {
                            push_pending(shared, data, &node, &mut writer).await?;
                            continue;
                        }
                        batch
                    }
                    Err(RecvError::Lagged(_)) => {
                        push_pending(shared, data, &node, &mut writer).await?;
                        continue;
                    }
                    Err(RecvError::Closed) => return Ok(()),
                };
                // only local writes travel up; the hub's entries carry a sequence number
                batch.retain(|entry| entry.seq == 0);
                push(&mut writer, &batch, false).await?;
            }
        }
    }
}

/// Refusals noted in a session but not yet in the replica folder: a burst of them is saved once, when no more of it is
/// read already, and at the latest when the session ends, however it ends (one lost in a crash is only pushed and
/// refused once more).
struct Unsaved<'a> {
    shared: &'a Shared,
    data: &'a Path,
    noted: bool,
}

impl Unsaved<'_> {
    fn save(&mut self) {
        if self.noted {
            save_refused(self.data, &self.shared.refused());
            self.noted = false;
        }
    }
}

impl Drop for Unsaved<'_> {
    fn drop(&mut self) {
        self.save();
    }
}

/// The hub's entries into the replica, written with a single sync; the ones that change something go to the watches
/// once they are durable. A confirmed write of this machine outside its scope (made before the scope shrank) is not kept
/// once the hub has it.
fn apply(shared: &Shared, entries: Vec<Entry>) -> anyhow::Result<()> {
    let mut store = shared.store();
    let mut outside = false;
    let mut changed = Vec::new();
    store.begin();
    for entry in entries {
        outside |= !shared.scope.holds(&entry.key);
        if let Some(entry) = store.from_hub(entry)? {
            changed.push(entry);
        }
    }
    store.commit().context("writing the replica")?;
    for entry in &changed {
        shared.publish(&store, entry);
    }
    if outside {
        store.retain(|entry| shared.scope.holds(&entry.key))?;
    }
    Ok(())
}

/// An entry only the hub writes: who is connected, the protected collections and the settings from its file.
fn hub_owned(entry: &Entry) -> bool {
    entry.key.starts_with(NODES)
        || entry.key == SETTINGS
        || (entry.key.starts_with(COLLECTIONS) && entry.value["protected"] == true)
}

/// Everything pending: this node's own writes as a plain push; other nodes' entries (pending after a reset) as a
/// restoring push, which the hub takes as they are. `_nodes/` is the hub's to write, and writes the hub refused for
/// good stay here.
async fn push_pending<W: AsyncWrite + Unpin>(
    shared: &Shared,
    data: &Path,
    node: &str,
    writer: &mut FramedWrite<W, LengthDelimitedCodec>,
) -> anyhow::Result<()> {
    let (own, others) = {
        let store = shared.store();
        let mut refused = shared.refused();
        let before = refused.len();
        // a key written anew is no longer the refused write
        refused.retain(|(key, hlc)| {
            store
                .held(key)
                .is_some_and(|held| held.seq == 0 && held.hlc.to_string() == *hlc)
        });
        if refused.len() != before {
            save_refused(data, &refused);
        }
        let pending: Vec<Entry> = store
            .pending()
            .into_iter()
            .filter(|entry| !entry.key.starts_with(NODES))
            .filter(|entry| !refused.contains(&(entry.key.clone(), entry.hlc.to_string())))
            .collect();
        pending
            .into_iter()
            .partition::<Vec<_>, _>(|entry| entry.hlc.node == node)
    };
    push(writer, &others, true).await?;
    push(writer, &own, false).await
}

/// A write the hub did not take. It is kept on the hub under `_rejected/<node>/…`. When the hub restated its own
/// version, that replaces it here as well; otherwise (a stamp from the future, a bad key) it stays here, and is no
/// longer pushed. Returns whether it was noted as refused; the caller saves that.
fn rejected(shared: &Shared, key: String, hlc: crate::hlc::Hlc, reason: &str, restated: bool) -> bool {
    let node = shared.store().node().to_string();
    eprintln!("kharka: the hub did not take {key}: {reason} (kept under _rejected/{node}/)");
    !restated && shared.refused().insert((key, hlc.to_string()))
}

async fn push<W: AsyncWrite + Unpin, E: Borrow<Entry>>(
    writer: &mut FramedWrite<W, LengthDelimitedCodec>,
    entries: &[E],
    restore: bool,
) -> anyhow::Result<()> {
    frame::send_entries(writer, frame::Entries::push(restore), entries).await
}

/// The hub this replica synced with last: a hub with another epoch numbers its changes differently.
fn saved_epoch(data: &Path) -> Option<String> {
    fs::read_to_string(data.join("hub-epoch"))
        .ok()
        .map(|text| text.trim().to_string())
        .filter(|epoch| !epoch.is_empty())
}

fn save_epoch(data: &Path, epoch: &str) -> anyhow::Result<()> {
    write_private(&data.join("hub-epoch"), &format!("{epoch}\n")).context("saving the hub's epoch")
}

/// The last `caught_up`: every change up to there has reached this replica (or was outside its scope).
fn saved_position(data: &Path) -> u64 {
    fs::read_to_string(data.join("position"))
        .ok()
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

fn save_position(data: &Path, seq: u64) -> anyhow::Result<()> {
    write_private(&data.join("position"), &format!("{seq}\n")).context("saving the position in the hub's sequence")
}

/// Writes the hub refused for good: `key<TAB>stamp` per line.
pub(crate) fn saved_refused(data: &Path) -> BTreeSet<(String, String)> {
    fs::read_to_string(data.join("refused"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .map(|(key, hlc)| (key.to_string(), hlc.to_string()))
        .collect()
}

fn save_refused(data: &Path, refused: &BTreeSet<(String, String)>) {
    let text: String = refused.iter().map(|(key, hlc)| format!("{key}\t{hlc}\n")).collect();
    if let Err(err) = write_private(&data.join("refused"), &text) {
        eprintln!("kharka: could not note the refused writes: {err}");
    }
}

/// The scope of the last sync: one prefix per line, none for everything; `None` before the first sync.
pub(crate) fn saved_scope(data: &Path) -> Option<Scope> {
    let text = fs::read_to_string(data.join("sync")).ok()?;
    Scope::of(text.lines().filter(|line| !line.is_empty()).map(str::to_string)).ok()
}

pub(crate) fn save_scope(data: &Path, scope: &Scope) -> anyhow::Result<()> {
    let text: String = scope.prefixes().iter().map(|prefix| format!("{prefix}\n")).collect();
    write_private(&data.join("sync"), &text).context("noting what this machine syncs")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::daemon::tests::shared;
    use crate::store::tests::counting;
    use bytes::Bytes;
    use futures_util::SinkExt;
    use tokio::net::UnixListener;

    #[tokio::test]
    async fn a_refusal_is_saved_however_the_session_ends() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("hub.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let hub = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (reader, writer) = stream.into_split();
            let (mut reader, mut writer) = (
                FramedRead::new(reader, frame::codec()),
                FramedWrite::new(writer, frame::codec()),
            );
            assert!(matches!(
                frame::recv(&mut reader).await.unwrap(),
                Some(Frame::Hello { .. })
            ));
            // in one write: the refusal, then a frame the daemon does not take, which ends the session with the
            // refusal noted and more of the stream read already
            let frames = [
                Frame::Welcome {
                    epoch: "e".into(),
                    reset: false,
                },
                Frame::CaughtUp { seq: 0 },
                Frame::Rejected {
                    key: "k".into(),
                    hlc: "1-0-a".parse().unwrap(),
                    reason: "no".into(),
                    restated: false,
                },
                Frame::Welcome {
                    epoch: "e".into(),
                    reset: false,
                },
            ];
            for frame in &frames {
                writer
                    .feed(Bytes::from(serde_json::to_vec(frame).unwrap()))
                    .await
                    .unwrap();
            }
            SinkExt::<Bytes>::flush(&mut writer).await.unwrap();
            // open until the daemon is done
            frame::recv(&mut reader).await.ok();
        });
        let shared = Arc::new(shared(counting("a").0));
        let data = dir.path().join("replica");
        fs::create_dir(&data).unwrap();
        let ended = session(&shared, &HubTarget::Unix(socket), &data, Heartbeat::default()).await;
        assert!(ended.unwrap_err().to_string().contains("the hub sent welcome"));
        assert!(saved_refused(&data).contains(&("k".to_string(), "1-0-a".to_string())));
        hub.abort();
    }

    #[test]
    fn hub_targets() {
        let parse = |text: &str| HubTarget::parse(text).unwrap();
        assert_eq!(parse("/run/k/hub.sock"), HubTarget::Unix("/run/k/hub.sock".into()));
        assert_eq!(parse("unix:rel.sock"), HubTarget::Unix("rel.sock".into()));
        assert_eq!(parse("server"), HubTarget::Ssh("server".into()));
        assert_eq!(parse("ssh:me@server"), HubTarget::Ssh("me@server".into()));
        assert_eq!(parse("local"), HubTarget::Unix(paths::hub_socket()));
        assert_eq!(parse("server").to_string(), "ssh:server");
        for bad in ["-oProxyCommand=touch x", "ssh:-F", "", "two words"] {
            assert!(HubTarget::parse(bad).is_err(), "{bad:?}");
        }
    }
}
