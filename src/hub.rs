//! The hub: the one place every change passes through. It numbers what it keeps, streams it to every connected daemon,
//! tells them who is connected (`_nodes/<node>`), and writes the protected collections of its kharka.toml.

use std::collections::HashMap;
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::fs;
use std::future::Future;
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use serde_json::{Value, json};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{Notify, broadcast, mpsc};
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::entry::{COLLECTIONS, Entry, MAX_VALUE, NODES, REJECTED, SETTINGS, check_key, check_node};
use crate::frame::{self, Frame, PROTOCOL};
use crate::hlc::wall_ms;
use crate::schema::{Collection, Collections, check_definition};
use crate::scope::Scope;
use crate::socket;
use crate::storage::write_private;
use crate::store::{self, Store};

/// Changes a connection may fall behind before it is caught up from the log.
const BACKLOG: usize = 4096;
/// Direct answers (confirmations, refusals) waiting for one connection; more are dropped: a dropped confirmation only
/// means the write is pushed once more later.
const ANSWERS: usize = 1024;
/// A stamp further ahead of the hub's clock than this is refused: the sender's clock is wrong.
const MAX_DRIFT_MS: u64 = 5 * 60 * 1000;
const HANDSHAKE: Duration = Duration::from_secs(10);
/// How often each side shows it is alive, and how long a silent connection is kept.
pub const PING: Duration = Duration::from_secs(15);
pub const SILENCE: Duration = Duration::from_secs(45);
/// How often the hub looks whether kharka.toml or a schema file changed.
const CONFIG_CHECK: Duration = Duration::from_secs(2);
/// The longest reason kept with a refused write.
const MAX_REASON: usize = 300;

pub struct Options {
    pub socket: PathBuf,
    pub data: PathBuf,
    /// The hub's own name in its stamps; must differ from every daemon's.
    pub node: String,
    pub config: Option<PathBuf>,
}

struct Hub {
    store: Mutex<Store>,
    changes: broadcast::Sender<Arc<Entry>>,
    /// The collections in force, as the entries under `_collections/` and `_kharka/settings` say.
    rules: RwLock<Collections>,
    epoch: String,
    /// The open connection of each node: its number, and how to close it when the node connects anew.
    connected: Mutex<HashMap<String, (u64, Arc<Notify>)>>,
    next_connection: AtomicU64,
}

/// Whether a key holds rules: a collection or the settings.
fn is_rule(key: &str) -> bool {
    key.starts_with(COLLECTIONS) || key == SETTINGS
}

/// What becomes of one pushed entry.
enum Verdict {
    Take,
    /// Left out without a word (a restored entry the hub decides itself, like `_nodes/`).
    Skip,
    /// Refused; with `restate`, the hub then states its own version with a later stamp.
    Refuse {
        reason: String,
        restate: bool,
    },
}

impl Hub {
    fn store(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn rules(&self) -> RwLockReadGuard<'_, Collections> {
        self.rules.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Puts the rules the store holds in force (also in the middle of a batch: a definition pushed together with data
    /// for it applies to that data).
    fn refresh_rules(&self, store: &Store) {
        let rules = Collections::from_entries(store.list(COLLECTIONS), store.get(SETTINGS));
        *self.rules.write().unwrap_or_else(PoisonError::into_inner) = rules;
    }

    /// Writes the open batch, then hands what it kept to every connection; entries holding rules put them in force.
    fn commit(&self, store: &mut Store, kept: Vec<Entry>) -> anyhow::Result<()> {
        let committed = store.commit().context("writing the hub's log");
        if kept.iter().any(|entry| is_rule(&entry.key)) {
            self.refresh_rules(store);
        }
        committed?;
        for entry in kept {
            let _ = self.changes.send(Arc::new(entry));
        }
        Ok(())
    }

    /// A write of the hub itself (`_nodes/…`, the protected collections, refused writes), numbered; `None` deletes.
    /// Goes into the open batch; the caller commits.
    fn write_own(store: &mut Store, key: String, value: Option<Value>) -> anyhow::Result<Entry> {
        let entry = Entry::new(key, value, store.tick());
        Ok(store.accept(entry)?.0)
    }

    /// A connection of `node` begins. A node has one connection: an older one (left half open by a laptop that slept,
    /// or a second daemon under the same name) is closed.
    fn arrive(&self, node: &str, id: u64, closer: Arc<Notify>) -> anyhow::Result<()> {
        let mut connected = self.connected.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((_, older)) = connected.insert(node.to_string(), (id, closer)) {
            older.notify_one();
            eprintln!("kharka: {node} connected again, the older connection is closed (two daemons named {node}?)");
            return Ok(());
        }
        let mut store = self.store();
        store.begin();
        let entry = Hub::write_own(
            &mut store,
            format!("{NODES}{node}"),
            Some(json!({ "online": true, "since": wall_ms() })),
        )?;
        self.commit(&mut store, vec![entry])
    }

    /// A connection of `node` ends; the node is offline unless a newer connection took its place.
    fn leave(&self, node: &str, id: u64) -> anyhow::Result<()> {
        let mut connected = self.connected.lock().unwrap_or_else(PoisonError::into_inner);
        if connected.get(node).is_none_or(|(current, _)| *current != id) {
            return Ok(());
        }
        connected.remove(node);
        let mut store = self.store();
        store.begin();
        let entry = Hub::write_own(
            &mut store,
            format!("{NODES}{node}"),
            Some(json!({ "online": false, "since": wall_ms() })),
        )?;
        self.commit(&mut store, vec![entry])
    }

    /// At start nobody is connected, whatever the log says.
    fn everyone_offline(&self) -> anyhow::Result<()> {
        let mut store = self.store();
        let online: Vec<String> = store
            .list(NODES)
            .filter(|entry| entry.value["online"] == json!(true))
            .map(|entry| entry.key.clone())
            .collect();
        store.begin();
        let mut kept = Vec::new();
        for key in online {
            kept.push(Hub::write_own(
                &mut store,
                key,
                Some(json!({ "online": false, "since": wall_ms() })),
            )?);
        }
        self.commit(&mut store, kept)
    }

    /// Makes the entries say what kharka.toml says: its collections, protected (replacing a node's own definition of
    /// the same name), no protected collection the file no longer has, and `strict`. Returns whether anything changed.
    fn apply_file(&self, file: &Collections) -> anyhow::Result<bool> {
        let mut store = self.store();
        store.begin();
        let mut kept = Vec::new();
        for (name, collection) in &file.collections {
            let protected = Collection {
                protected: true,
                ..collection.clone()
            };
            let value = serde_json::to_value(&protected)?;
            let key = format!("{COLLECTIONS}{name}");
            if store.get(&key).map(|entry| &entry.value) != Some(&value) {
                kept.push(Hub::write_own(&mut store, key, Some(value))?);
            }
        }
        let dropped: Vec<String> = store
            .list(COLLECTIONS)
            .filter(|entry| entry.value["protected"] == true)
            .filter(|entry| !file.collections.contains_key(&entry.key[COLLECTIONS.len()..]))
            .map(|entry| entry.key.clone())
            .collect();
        for key in dropped {
            kept.push(Hub::write_own(&mut store, key, None)?);
        }
        let strict = store.get(SETTINGS).is_some_and(|entry| entry.value["strict"] == true);
        if strict != file.strict {
            kept.push(Hub::write_own(
                &mut store,
                SETTINGS.to_string(),
                Some(json!({ "strict": file.strict })),
            )?);
        }
        let changed = !kept.is_empty();
        self.commit(&mut store, kept)?;
        Ok(changed)
    }

    /// Takes one push, as one batch: one sync for all its entries, published only once they are durable. A refused
    /// entry is kept under `_rejected/<node>/…` and answered with `rejected`; with `restate`, the hub then states its own
    /// version with a later stamp, so that the sender's version is replaced everywhere.
    fn receive(
        &self,
        entries: Vec<Entry>,
        node: &str,
        restore: bool,
        direct: &mpsc::Sender<Frame>,
    ) -> anyhow::Result<()> {
        let mut store = self.store();
        store.begin();
        let (mut kept, mut answers, mut refusals) = (Vec::new(), Vec::new(), Vec::new());
        for entry in entries {
            let verdict = if restore {
                restoring(&entry)
            } else {
                self.refusal(&store, &entry, node)
            };
            match verdict {
                Verdict::Skip => {}
                Verdict::Take => {
                    let (held, changed) = store.accept(entry)?;
                    if changed && is_rule(&held.key) {
                        self.refresh_rules(&store);
                    }
                    if changed { kept.push(held) } else { answers.push(held) }
                }
                Verdict::Refuse { reason, restate } => {
                    let reason = short(&reason, MAX_REASON);
                    eprintln!("kharka: refused {} from {node}: {reason}", entry.key);
                    let record_key = format!("{REJECTED}{node}/{}-{}", entry.hlc.wall_ms, entry.hlc.counter);
                    if check_key(&record_key).is_ok() {
                        kept.push(Hub::write_own(
                            &mut store,
                            record_key,
                            Some(rejected_record(&entry, &reason)),
                        )?);
                    }
                    refusals.push(Frame::Rejected {
                        key: entry.key.clone(),
                        hlc: entry.hlc.clone(),
                        reason,
                        restated: restate,
                    });
                    if restate {
                        store.observe(&entry.hlc);
                        let current = store.held(&entry.key).and_then(Entry::live_value).cloned();
                        kept.push(Hub::write_own(&mut store, entry.key, current)?);
                    }
                }
            }
        }
        self.commit(&mut store, kept)?;
        drop(store);
        if !answers.is_empty() {
            let _ = direct.try_send(Frame::Changes { entries: answers });
        }
        for refusal in refusals {
            let _ = direct.try_send(refusal);
        }
        Ok(())
    }

    /// Why a pushed entry is not taken, and whether the hub restates its own version.
    fn refusal(&self, store: &Store, entry: &Entry, node: &str) -> Verdict {
        let refuse = |reason: String, restate: bool| Verdict::Refuse { reason, restate };
        if let Err(problem) = check_key(&entry.key) {
            return refuse(problem, false);
        }
        if entry.hlc.wall_ms > wall_ms() + MAX_DRIFT_MS {
            return refuse(
                "stamped more than five minutes ahead of the hub: check the sender's clock".into(),
                false,
            );
        }
        if entry.hlc.node != node {
            return refuse(format!("stamped by {} but sent by {node}", entry.hlc.node), true);
        }
        if entry.value.to_string().len() > MAX_VALUE {
            return refuse(format!("the value has more than {MAX_VALUE} bytes"), true);
        }
        if let Some(name) = entry.key.strip_prefix(COLLECTIONS) {
            if store
                .get(&entry.key)
                .is_some_and(|held| held.value["protected"] == true)
            {
                return refuse(
                    format!("collection {name} is protected: only the hub's kharka.toml changes it"),
                    true,
                );
            }
            return match entry.live_value().map(|value| check_definition(name, value)) {
                Some(Err(problem)) => refuse(problem, true),
                _ => Verdict::Take,
            };
        }
        if entry.key.starts_with(REJECTED) && entry.deleted {
            return Verdict::Take;
        }
        if entry.key.starts_with('_') {
            return refuse("keys starting with _ are written by kharka itself".into(), true);
        }
        if let Err(problem) = self.rules().check(&entry.key, entry.live_value(), node) {
            return refuse(problem, true);
        }
        Verdict::Take
    }
}

/// An entry a daemon restores after a reset (another node's, which the hub lost): taken as it is, so the latest write
/// still wins. Left out: what the hub decides itself (`_nodes/`, the protected collections and the settings, which come
/// from its file), and what could not be kept (a bad key, a value too large, a stamp from the future).
fn restoring(entry: &Entry) -> Verdict {
    let own = entry.key.starts_with(NODES)
        || entry.key == SETTINGS
        || (entry.key.starts_with(COLLECTIONS) && entry.value["protected"] == true);
    let unfit = check_key(&entry.key).is_err()
        || entry.hlc.wall_ms > wall_ms() + MAX_DRIFT_MS
        || entry.value.to_string().len() > MAX_VALUE;
    if own || unfit { Verdict::Skip } else { Verdict::Take }
}

/// What is kept of a refused write. Small enough to stay a record: without its value when that is too large.
fn rejected_record(entry: &Entry, reason: &str) -> Value {
    let mut record = json!({
        "key": entry.key,
        "value": entry.live_value(),
        "deleted": entry.deleted,
        "hlc": entry.hlc,
        "reason": reason,
    });
    if record.to_string().len() > MAX_VALUE / 2 {
        record["value"] = Value::Null;
        record["value_bytes"] = json!(entry.value.to_string().len());
    }
    record
}

/// A text cut to at most `max` characters.
fn short(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut cut: String = text.chars().take(max).collect();
    cut.push('…');
    cut
}

/// Runs the hub until `shutdown` completes.
pub async fn run(options: Options, shutdown: impl Future<Output = ()>) -> anyhow::Result<()> {
    check_node(&options.node).map_err(anyhow::Error::msg)?;
    let store = Store::open(&options.data, &options.node, store::Options::default())
        .with_context(|| format!("opening the hub's log in {}", options.data.display()))?;
    // a log cut while loading has lost changes the daemons already counted: a new epoch makes them restore everything
    let epoch = epoch(&options.data, store.damaged())?;
    let (file, files) = load_config(options.config.as_deref())?;
    let listener = socket::bind(&options.socket)?;
    let rules = Collections::from_entries(store.list(COLLECTIONS), store.get(SETTINGS));
    let (changes, _) = broadcast::channel(BACKLOG);
    let hub = Arc::new(Hub {
        store: Mutex::new(store),
        changes,
        rules: RwLock::new(rules),
        epoch,
        connected: Mutex::new(HashMap::new()),
        next_connection: AtomicU64::new(1),
    });
    hub.apply_file(&file)?;
    hub.everyone_offline()?;
    {
        let store = hub.store();
        eprintln!(
            "kharka: hub {} on {}, {} entries up to {}, collections: {}",
            options.node,
            options.socket.display(),
            store.live_count(),
            store.last_seq(),
            hub.rules().summary()
        );
    }
    let watching = tokio::spawn(watch_config(hub.clone(), options.config.clone(), files));
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    let hub = hub.clone();
                    tokio::spawn(async move {
                        if let Err(err) = serve(&hub, stream).await {
                            eprintln!("kharka: a connection ended: {err:#}");
                        }
                    });
                }
                Err(err) => {
                    eprintln!("kharka: accept failed: {err}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            () = &mut shutdown => break,
        }
    }
    watching.abort();
    let _ = fs::remove_file(&options.socket);
    Ok(())
}

/// The collections from kharka.toml and the files they came from; without the file, none.
fn load_config(path: Option<&Path>) -> anyhow::Result<(Collections, Vec<PathBuf>)> {
    match path {
        Some(path) if path.exists() => Collections::load_with_files(path),
        Some(path) => Ok((Collections::default(), vec![path.to_path_buf()])),
        None => Ok((Collections::default(), Vec::new())),
    }
}

/// A fingerprint of the files' contents (a missing file counts too): a change in any means reading the config again.
fn fingerprint(files: &[PathBuf]) -> u64 {
    let mut hasher = DefaultHasher::new();
    for file in files {
        fs::read(file).ok().hash(&mut hasher);
    }
    hasher.finish()
}

/// Reloads kharka.toml when it or one of its schema files changes, and on SIGHUP. A config that does not load changes
/// nothing, and neither does a missing one: a file that is away for a moment (relinked, checked out, rewritten by an
/// editor) must not drop every protected collection. A change reaches every connected daemon at once, as entries.
async fn watch_config(hub: Arc<Hub>, path: Option<PathBuf>, mut files: Vec<PathBuf>) {
    let mut seen = fingerprint(&files);
    let mut hangup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()).ok();
    let mut ticks = tokio::time::interval(CONFIG_CHECK);
    let mut reported_missing = false;
    loop {
        let asked = tokio::select! {
            _ = ticks.tick() => false,
            () = hangup_of(&mut hangup) => true,
        };
        if !asked && fingerprint(&files) == seen {
            continue;
        }
        // an editor may still be writing: give it a moment
        tokio::time::sleep(Duration::from_millis(200)).await;
        seen = fingerprint(&files);
        if let Some(path) = path.as_deref().filter(|path| !path.exists()) {
            if !reported_missing {
                eprintln!("kharka: {} is gone; the collections stay as they are", path.display());
                reported_missing = true;
            }
            continue;
        }
        reported_missing = false;
        match load_config(path.as_deref()) {
            Ok((file, read)) => {
                files = read;
                seen = fingerprint(&files);
                match hub.apply_file(&file) {
                    Ok(true) => eprintln!("kharka: collections reloaded: {}", hub.rules().summary()),
                    Ok(false) if asked => eprintln!("kharka: collections unchanged: {}", hub.rules().summary()),
                    Ok(false) => {}
                    Err(err) => eprintln!("kharka: could not apply the collections: {err:#}"),
                }
            }
            Err(err) => eprintln!("kharka: {err:#}; the collections stay as they were"),
        }
    }
}

/// The next SIGHUP; never, when the signal cannot be watched.
async fn hangup_of(signal: &mut Option<tokio::signal::unix::Signal>) {
    if let Some(signal) = signal
        && signal.recv().await.is_some()
    {
        return;
    }
    std::future::pending().await
}

/// The hub's identity: made once with its log, so that a hub that lost its data is recognised as a new one. `renew`
/// makes a new one (the log was cut: what the daemons counted on may be gone).
fn epoch(data: &Path, renew: bool) -> anyhow::Result<String> {
    let path = data.join("epoch");
    if !renew
        && let Ok(text) = fs::read_to_string(&path)
        && !text.trim().is_empty()
    {
        return Ok(text.trim().to_string());
    }
    if renew {
        eprintln!("kharka: the hub's log was cut while loading: a new epoch, every daemon restores what it has");
    }
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u128(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos()),
    );
    hasher.write_u32(std::process::id());
    let epoch = format!("{:016x}", hasher.finish());
    write_private(&path, &format!("{epoch}\n")).with_context(|| format!("writing {}", path.display()))?;
    Ok(epoch)
}

async fn serve(hub: &Arc<Hub>, stream: UnixStream) -> anyhow::Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = FramedRead::new(reader, frame::codec());
    let mut writer = FramedWrite::new(writer, frame::codec());
    let hello = timeout(HANDSHAKE, frame::recv(&mut reader))
        .await
        .context("no hello")??;
    let (version, node, epoch, since, prefixes) = match hello {
        Some(Frame::Hello {
            version,
            node,
            epoch,
            since,
            prefixes,
        }) => (version, node, epoch, since, prefixes),
        // a connection that closes at once is a probe (is a hub listening here?), not a problem
        None => return Ok(()),
        Some(other) => bail!("the first frame was {}, not hello", other.name()),
    };
    let scope = Scope::of(prefixes);
    let refusal = if version != PROTOCOL {
        Some(format!(
            "protocol {version}, the hub speaks {PROTOCOL}: update kharka on both sides"
        ))
    } else if let Err(problem) = check_node(&node) {
        Some(problem)
    } else if let Err(problem) = &scope {
        Some(problem.clone())
    } else if node == hub.store().node() {
        Some(format!(
            "{node} is the hub's own name: give the daemon another one (--node)"
        ))
    } else {
        None
    };
    if let Some(reason) = refusal {
        frame::send(&mut writer, &Frame::Refused { reason: reason.clone() }).await?;
        bail!("refused {node}: {reason}");
    }
    let scope = scope.expect("checked above");
    // what the node missed and the subscription for what follows, under one lock: nothing falls in between
    let (reset, backlog, changes, seq) = {
        let store = hub.store();
        let reset = epoch.as_deref() != Some(hub.epoch.as_str()) || since > store.last_seq();
        let backlog = store.since_where(if reset { 0 } else { since }, |entry| {
            scope.holds(&entry.key) || entry.hlc.node == node
        });
        (reset, backlog, hub.changes.subscribe(), store.last_seq())
    };
    let welcome = Frame::Welcome {
        epoch: hub.epoch.clone(),
        reset,
    };
    let (direct, direct_out) = mpsc::channel(ANSWERS);
    eprintln!(
        "kharka: {node} connected, from {}, syncing {scope}",
        if reset { 0 } else { since }
    );
    let stream = Stream {
        node: node.clone(),
        scope,
        changes,
        direct: direct_out,
    };
    let writing = tokio::spawn(write(hub.clone(), writer, welcome, backlog, seq, stream));
    let id = hub.next_connection.fetch_add(1, Ordering::Relaxed);
    let closer = Arc::new(Notify::new());
    if let Err(err) = hub.arrive(&node, id, closer.clone()) {
        eprintln!("kharka: could not note that {node} arrived: {err:#}");
    }
    let read = read(hub, &node, &mut reader, &direct, &closer).await;
    if let Err(err) = hub.leave(&node, id) {
        eprintln!("kharka: could not note that {node} left: {err:#}");
    }
    writing.abort();
    eprintln!("kharka: {node} disconnected");
    read
}

/// Everything a node sends: pushes, and pongs. A connection silent for longer than `SILENCE` is dead (the other side
/// pings every `PING`), as is one a newer connection of the same node replaced.
async fn read(
    hub: &Hub,
    node: &str,
    reader: &mut FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    direct: &mpsc::Sender<Frame>,
    closer: &Notify,
) -> anyhow::Result<()> {
    loop {
        let incoming = tokio::select! {
            () = closer.notified() => return Ok(()),
            incoming = timeout(SILENCE, frame::recv(reader)) => {
                incoming.map_err(|_| anyhow!("{node} went silent"))??
            }
        };
        match incoming {
            None => return Ok(()),
            Some(Frame::Push { entries, restore }) => hub.receive(entries, node, restore, direct)?,
            Some(Frame::Pong) => {}
            Some(Frame::Ping) => {
                let _ = direct.try_send(Frame::Pong);
            }
            Some(other) => bail!("{node} sent {} instead of push", other.name()),
        }
    }
}

/// What a connection follows after the catch-up: every change the node syncs (and its own writes, as the
/// confirmation), and the direct answers to its pushes.
struct Stream {
    node: String,
    scope: Scope,
    changes: broadcast::Receiver<Arc<Entry>>,
    direct: mpsc::Receiver<Frame>,
}

/// Everything to one node: the welcome, what it missed, `caught_up`, then every change in sequence order, the direct
/// answers, and a ping every `PING`. A connection too slow for the stream is caught up from the log.
async fn write(
    hub: Arc<Hub>,
    mut writer: FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    welcome: Frame,
    backlog: Vec<Entry>,
    seq: u64,
    stream: Stream,
) -> anyhow::Result<()> {
    let Stream {
        node,
        scope,
        mut changes,
        mut direct,
    } = stream;
    let wanted = |entry: &Entry| scope.holds(&entry.key) || entry.hlc.node == node;
    frame::send(&mut writer, &welcome).await?;
    send_entries(&mut writer, backlog).await?;
    frame::send(&mut writer, &Frame::CaughtUp { seq }).await?;
    // the position in the hub's sequence, also across entries this node does not sync
    let mut last = seq;
    let mut pings = tokio::time::interval_at(tokio::time::Instant::now() + PING, PING);
    loop {
        tokio::select! {
            change = changes.recv() => match change {
                Ok(entry) => {
                    if entry.seq > last {
                        last = entry.seq;
                        if wanted(&entry) {
                            send_entries(&mut writer, vec![Arc::unwrap_or_clone(entry)]).await?;
                        }
                    }
                }
                Err(RecvError::Lagged(_)) => {
                    let missed = {
                        let store = hub.store();
                        changes = hub.changes.subscribe();
                        let missed = store.since_where(last, wanted);
                        last = last.max(store.last_seq());
                        missed
                    };
                    send_entries(&mut writer, missed).await?;
                }
                Err(RecvError::Closed) => return Ok(()),
            },
            answer = direct.recv() => match answer {
                // an answer carrying an entry not streamed yet waits for the stream: sending it now would let the node
                // count past changes still on their way to it
                Some(Frame::Changes { mut entries }) => {
                    entries.retain(|entry| entry.seq <= last);
                    if !entries.is_empty() {
                        frame::send(&mut writer, &Frame::Changes { entries }).await?;
                    }
                }
                Some(answer) => frame::send(&mut writer, &answer).await?,
                None => return Ok(()),
            },
            _ = pings.tick() => frame::send(&mut writer, &Frame::Ping).await?,
        }
    }
}

async fn send_entries(
    writer: &mut FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    entries: Vec<Entry>,
) -> anyhow::Result<()> {
    for batch in frame::batches(entries) {
        frame::send(writer, &Frame::Changes { entries: batch }).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, value: Value, hlc: &str) -> Entry {
        Entry::new(key, Some(value), hlc.parse().unwrap())
    }

    #[test]
    fn a_rejected_record_stays_small() {
        let big = entry("tasks/1", json!("x".repeat(700_000)), "5-0-a");
        let record = rejected_record(
            &big,
            &short(&format!("\"{}\" is not one of [...]", "x".repeat(700_000)), MAX_REASON),
        );
        assert!(record.to_string().len() < 2000, "{} bytes", record.to_string().len());
        assert_eq!(record["value"], Value::Null);
        assert_eq!(record["value_bytes"], json!(700_002));
        let small = rejected_record(&entry("tasks/2", json!({ "a": 1 }), "5-0-a"), "no");
        assert_eq!(small["value"], json!({ "a": 1 }));
    }

    #[test]
    fn restoring_takes_other_nodes_writes_but_not_what_the_hub_decides() {
        assert!(matches!(restoring(&entry("tasks/1", json!(1), "5-0-b")), Verdict::Take));
        assert!(matches!(
            restoring(&entry("_nodes/b", json!({}), "5-0-hub")),
            Verdict::Skip
        ));
        assert!(matches!(
            restoring(&entry(SETTINGS, json!({ "strict": true }), "5-0-hub")),
            Verdict::Skip
        ));
        assert!(matches!(
            restoring(&entry("_collections/x", json!({ "protected": true }), "5-0-hub")),
            Verdict::Skip
        ));
        assert!(matches!(
            restoring(&entry("_collections/own", json!({}), "5-0-b")),
            Verdict::Take
        ));
        assert!(matches!(
            restoring(&entry("tasks/2", json!(1), "99999999999999-0-b")),
            Verdict::Skip
        ));
    }

    #[test]
    fn short_texts() {
        assert_eq!(short("abc", 5), "abc");
        assert_eq!(short("äbcdef", 3), "äbc…");
    }
}
