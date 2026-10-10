//! The daemon: this machine's replica behind a Unix socket, JSON lines in and out, and the sync with the hub.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, TryLockError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};

use crate::entry::{COLLECTIONS, Entry, MAX_VALUE, REJECTED, SETTINGS, check_key, check_prefix};
use crate::hlc::{Hlc, wall_ms};
use crate::hub::Heartbeat;
use crate::schema::{Collections, check_definition};
use crate::scope::Scope;
use crate::socket;
use crate::store::{self, Store};
use crate::sync::{self, HubTarget};

/// The longest request line: a value at its largest plus room for the rest.
const MAX_LINE: usize = 2 * MAX_VALUE;
/// Changes a watch may fall behind before it starts over (`reset`).
const BACKLOG: usize = 4096;
/// Lines waiting to be written to one connection.
const OUTBOX: usize = 1024;

pub struct Options {
    pub socket: PathBuf,
    pub data: PathBuf,
    pub node: String,
    /// The hub to sync with; without one the replica stays on this machine.
    pub hub: Option<HubTarget>,
    /// `kharka.toml`: without a hub, its collections are this machine's protected ones (with a hub, the hub's file
    /// counts).
    pub config: Option<PathBuf>,
    /// What this replica holds: everything, or the keys under some prefixes.
    pub sync: Scope,
    pub heartbeat: Heartbeat,
}

pub(crate) struct Shared {
    store: Mutex<Store>,
    /// Every change, local writes and the hub's, for the watches and the sync.
    pub changes: broadcast::Sender<Arc<Entry>>,
    /// The collections in force: the entries under `_collections/`, with the local file laid over them.
    rules: RwLock<Collections>,
    /// The local kharka.toml, when there is no hub.
    file: Option<Collections>,
    pub online: watch::Sender<bool>,
    hub: Option<String>,
    pub scope: Scope,
    /// The scope changed since the last sync: the next catch-up starts from the beginning, for the keys newly synced.
    pub full_catch_up: AtomicBool,
    /// Writes the hub refused for good (key, stamp): no longer pushed.
    refused: Mutex<BTreeSet<(String, String)>>,
    /// Threads waiting for the store's lock: the writer lets them in before its next group.
    waiting: AtomicUsize,
}

/// Whether a key holds rules: a collection or the settings.
fn is_rule(key: &str) -> bool {
    key.starts_with(COLLECTIONS) || key == SETTINGS
}

impl Shared {
    pub fn store(&self) -> MutexGuard<'_, Store> {
        match self.store.try_lock() {
            Ok(store) => store,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                self.waiting.fetch_add(1, Ordering::SeqCst);
                let store = self.store.lock().unwrap_or_else(PoisonError::into_inner);
                self.waiting.fetch_sub(1, Ordering::SeqCst);
                store
            }
        }
    }

    pub fn refused(&self) -> MutexGuard<'_, BTreeSet<(String, String)>> {
        self.refused.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn rules(&self) -> RwLockReadGuard<'_, Collections> {
        self.rules.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn rules_of(store: &Store, file: Option<&Collections>) -> Collections {
        let rules = Collections::from_entries(store.list(COLLECTIONS), store.get(SETTINGS));
        match file {
            Some(file) => rules.overlay(file),
            None => rules,
        }
    }

    /// Hands a change to the watches and the sync; one that holds rules puts them in force first. Called with the store
    /// locked, so that a watch, which subscribes under the same lock, neither misses nor repeats it.
    pub fn publish(&self, store: &Store, entry: &Entry) {
        if is_rule(&entry.key) {
            *self.rules.write().unwrap_or_else(PoisonError::into_inner) = Shared::rules_of(store, self.file.as_ref());
        }
        let _ = self.changes.send(Arc::new(entry.clone()));
    }

    /// Whether a collection is protected: by the hub's kharka.toml, or by the local one.
    fn protected(&self, store: &Store, name: &str) -> bool {
        let by_hub = store
            .get(&format!("{COLLECTIONS}{name}"))
            .is_some_and(|entry| entry.value["protected"] == true);
        by_hub
            || self
                .file
                .as_ref()
                .is_some_and(|file| file.collections.contains_key(name))
    }
}

/// Runs the daemon until `shutdown` completes.
pub async fn run(options: Options, shutdown: impl Future<Output = ()>) -> anyhow::Result<()> {
    let mut store = Store::open(&options.data, &options.node, store::Options::default())
        .with_context(|| format!("opening the replica in {}", options.data.display()))?;
    let file = match &options.config {
        Some(path) if options.hub.is_none() && path.exists() => Some(Collections::load(path)?),
        _ => None,
    };
    let full_catch_up = narrow_to_scope(&mut store, &options)?;
    let listener = socket::bind(&options.socket)?;
    let hub = options.hub.as_ref().map(ToString::to_string);
    let rules = Shared::rules_of(&store, file.as_ref());
    eprintln!(
        "kharka: daemon {} on {}, {} entries, hub: {}, syncing {}, collections: {}",
        store.node(),
        options.socket.display(),
        store.live_count(),
        hub.as_deref().unwrap_or("none"),
        options.sync,
        rules.summary()
    );
    options.heartbeat.announce(false);
    let (changes, _) = broadcast::channel(BACKLOG);
    let (online, _) = watch::channel(false);
    let shared = Arc::new(Shared {
        store: Mutex::new(store),
        changes,
        rules: RwLock::new(rules),
        file,
        online,
        hub,
        scope: options.sync.clone(),
        full_catch_up: AtomicBool::new(full_catch_up),
        refused: Mutex::new(sync::saved_refused(&options.data)),
        waiting: AtomicUsize::new(0),
    });
    let (writes, queue) = mpsc::unbounded_channel();
    let mut writer_stopped = start_writer(shared.clone(), queue);
    let syncing = options
        .hub
        .map(|hub| tokio::spawn(sync::run(shared.clone(), hub, options.data.clone(), options.heartbeat)));
    tokio::pin!(shutdown);
    // a daemon whose writer is gone answers no write any more: it ends, so that the service manager starts it anew
    let ended = loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    tokio::spawn(serve(stream, shared.clone(), writes.clone()));
                }
                Err(err) => {
                    eprintln!("kharka: accept failed: {err}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = &mut writer_stopped => break Err(anyhow!("the writer of the local writes stopped")),
            () = &mut shutdown => break Ok(()),
        }
    };
    if let Some(task) = syncing {
        task.abort();
    }
    let _ = fs::remove_file(&options.socket);
    ended
}

/// When the scope differs from the last run: the entries it no longer holds go (local writes the hub does not have yet
/// stay until it has them), and, with a hub, the next catch-up starts from the beginning for the keys it newly holds.
/// Returns whether that full catch-up is needed; the scope is noted once it is done.
fn narrow_to_scope(store: &mut Store, options: &Options) -> anyhow::Result<bool> {
    if sync::saved_scope(&options.data).as_ref() == Some(&options.sync) {
        return Ok(false);
    }
    let removed = store
        .retain(|entry| options.sync.holds(&entry.key))
        .context("removing what is no longer synced")?;
    if removed > 0 {
        eprintln!("kharka: removed {removed} entries this machine no longer syncs");
    }
    if options.hub.is_none() {
        sync::save_scope(&options.data, &options.sync)?;
        return Ok(false);
    }
    Ok(true)
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
enum Request {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: Value,
        #[serde(default)]
        if_hlc: Option<Hlc>,
    },
    Del {
        key: String,
        #[serde(default)]
        if_hlc: Option<Hlc>,
    },
    Ls {
        #[serde(default)]
        prefix: String,
    },
    Watch {
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        initial: bool,
    },
    Unwatch {
        watch: Value,
    },
    Whoami,
    Status,
    Collections,
    Define {
        name: String,
        definition: Value,
    },
    Undefine {
        name: String,
    },
}

/// One connection: requests in, answers and watch events out, through one writer so that lines never interleave.
async fn serve(stream: UnixStream, shared: Arc<Shared>, writes: mpsc::UnboundedSender<Write>) {
    let (reader, mut writer) = stream.into_split();
    let mut lines = FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_LINE));
    let (out, mut outbox) = mpsc::channel::<String>(OUTBOX);
    let writing = tokio::spawn(async move {
        while let Some(line) = outbox.recv().await {
            if writer.write_all(line.as_bytes()).await.is_err() {
                break;
            }
        }
    });
    let mut watches: HashMap<String, JoinHandle<()>> = HashMap::new();
    while let Some(line) = lines.next().await {
        let mut started = None;
        let answer = match line {
            Ok(line) if line.trim().is_empty() => continue,
            Ok(line) => match handle(&shared, &line, &writes, &mut watches, &mut started) {
                Answer::Now(answer) => answer,
                // the next request waits for it: answers keep the order of the requests
                Answer::Later(id, written) => written.await.unwrap_or_else(|_| error(&id, "io", "the writer stopped")),
            },
            Err(LinesCodecError::MaxLineLengthExceeded) => error(
                &Value::Null,
                "invalid",
                &format!("a request has at most {MAX_LINE} bytes"),
            ),
            Err(LinesCodecError::Io(_)) => break,
        };
        if out.send(line_of(answer)).await.is_err() {
            break;
        }
        // a watch starts after its answer is out, so that the answer always comes first; it subscribed already
        if let Some(start) = started {
            watches.insert(
                start.name.clone(),
                tokio::spawn(watch(shared.clone(), start, out.clone())),
            );
        }
    }
    for watch in watches.into_values() {
        watch.abort();
    }
    drop(out);
    let _ = writing.await;
}

/// A watch that was answered and is about to start.
struct WatchStart {
    name: String,
    id: Value,
    prefix: String,
    initial: bool,
    changes: broadcast::Receiver<Arc<Entry>>,
    online: watch::Receiver<bool>,
    current: Vec<Entry>,
}

/// What a request line gets: its answer, or, for a tool's write, its answer once the write's group is durable.
enum Answer {
    Now(Value),
    Later(Value, oneshot::Receiver<Value>),
}

/// The answer to one request line. A watch is subscribed here and left in `started` for the caller to run; a tool's
/// put or del goes to the writer.
fn handle(
    shared: &Shared,
    line: &str,
    writes: &mpsc::UnboundedSender<Write>,
    watches: &mut HashMap<String, JoinHandle<()>>,
    started: &mut Option<WatchStart>,
) -> Answer {
    let raw: Value = match serde_json::from_str(line) {
        Ok(raw) => raw,
        Err(err) => return Answer::Now(error(&Value::Null, "invalid", &format!("not JSON: {err}"))),
    };
    let id = raw.get("id").cloned().unwrap_or(Value::Null);
    let request = match Request::deserialize(raw) {
        Ok(request) => request,
        Err(err) => return Answer::Now(error(&id, "invalid", &err.to_string())),
    };
    let scope = &shared.scope;
    let not_synced = |what: &str| {
        error(
            &id,
            "not_synced",
            &format!("{what} is not synced to this machine, which syncs {scope}"),
        )
    };
    Answer::Now(match request {
        Request::Get { key } if check_key(&key).is_ok() && !scope.holds(&key) => not_synced(&key),
        Request::Put { key, .. } | Request::Del { key, .. } if check_key(&key).is_ok() && !scope.holds(&key) => {
            not_synced(&key)
        }
        Request::Ls { prefix } | Request::Watch { prefix, .. }
            if check_prefix(&prefix).is_ok() && !scope.overlaps(&prefix) =>
        {
            not_synced(&format!("nothing under {prefix:?}"))
        }
        Request::Get { key } => match check_key(&key) {
            Ok(()) => {
                let entry = shared.store().get(&key).map(|entry| entry_json(shared, entry));
                ok(&id, json!({ "entry": entry }))
            }
            Err(problem) => error(&id, "invalid", &problem),
        },
        Request::Put { key, value, if_hlc } => return queue(writes, id, key, Some(value), if_hlc),
        // a record of a refused write is cleared by deleting it
        Request::Del { key, if_hlc } if key.starts_with(REJECTED) && check_key(&key).is_ok() => {
            let mut store = shared.store();
            let current = store.get(&key).map(|entry| entry.hlc.clone());
            let result = match (&current, &if_hlc) {
                (_, Some(expected)) if current.as_ref() != Some(expected) => Err(store::Error::Conflict { current }),
                (Some(_), _) => store.write_own(&key, None).map(Some),
                (None, _) => Ok(None),
            };
            written(shared, &store, &id, result)
        }
        Request::Del { key, if_hlc } => return queue(writes, id, key, None, if_hlc),
        Request::Ls { prefix } => match check_prefix(&prefix) {
            Ok(()) => {
                let entries: Vec<Value> = shared
                    .store()
                    .list(&prefix)
                    .map(|entry| entry_json(shared, entry))
                    .collect();
                ok(&id, json!({ "entries": entries }))
            }
            Err(problem) => error(&id, "invalid", &problem),
        },
        Request::Watch { prefix, initial } => {
            if let Err(problem) = check_prefix(&prefix) {
                return Answer::Now(error(&id, "invalid", &problem));
            }
            let name = id.to_string();
            if watches.get(&name).is_some_and(|task| !task.is_finished()) {
                return Answer::Now(error(
                    &id,
                    "invalid",
                    "a watch with this id runs already on this connection",
                ));
            }
            let (changes, current) = subscribe(shared, &prefix, initial);
            let mut online = shared.online.subscribe();
            online.borrow_and_update();
            let answer = ok(&id, json!({ "watch": prefix }));
            *started = Some(WatchStart {
                name,
                id,
                prefix,
                initial,
                changes,
                online,
                current,
            });
            answer
        }
        Request::Unwatch { watch } => match watches.remove(&watch.to_string()) {
            Some(task) => {
                task.abort();
                ok(&id, json!({}))
            }
            None => error(&id, "invalid", "no such watch on this connection"),
        },
        Request::Whoami => ok(&id, json!({ "node": shared.store().node() })),
        Request::Status => {
            let store = shared.store();
            // writes the hub refused for good are not pending: they are no longer pushed; and they count as refused only
            // while they are held (a key written anew is no longer the refused write)
            let (refused, pending) = {
                let noted = shared.refused();
                let pending = store.pending();
                let refused = pending
                    .iter()
                    .filter(|entry| noted.contains(&(entry.key.clone(), entry.hlc.to_string())))
                    .count();
                (refused, pending.len() - refused)
            };
            let collections: Vec<String> = shared.rules().collections.keys().cloned().collect();
            ok(
                &id,
                json!({
                    "node": store.node(),
                    "hub": shared.hub,
                    "online": *shared.online.borrow(),
                    "entries": store.live_count(),
                    "pending": pending,
                    "seq": store.last_seq(),
                    "collections": collections,
                    "refused": refused,
                    "sync": (!scope.is_everything()).then(|| scope.prefixes()),
                }),
            )
        }
        Request::Collections => ok(&id, collections_json(shared)),
        Request::Define { name, definition } => {
            let mut store = shared.store();
            let result = match check_definition(&name, &definition) {
                Err(problem) => Err(store::Error::Invalid(problem)),
                Ok(_) if shared.protected(&store, &name) => return Answer::Now(protected(&id, &name)),
                Ok(_) => store
                    .write_own(&format!("{COLLECTIONS}{name}"), Some(definition))
                    .map(Some),
            };
            written(shared, &store, &id, result)
        }
        Request::Undefine { name } => {
            let mut store = shared.store();
            let key = format!("{COLLECTIONS}{name}");
            let result = if shared.protected(&store, &name) {
                return Answer::Now(protected(&id, &name));
            } else if store.get(&key).is_none() {
                Ok(None)
            } else {
                store.write_own(&key, None).map(Some)
            };
            written(shared, &store, &id, result)
        }
    })
}

/// A tool's put (with a value) or del (without), waiting for its group.
struct Write {
    id: Value,
    key: String,
    value: Option<Value>,
    if_hlc: Option<Hlc>,
    answer: oneshot::Sender<Value>,
}

/// Hands a write to the writer. Each connection waits for the answer before its next request, so the queue holds at
/// most one write per connection.
fn queue(
    writes: &mpsc::UnboundedSender<Write>,
    id: Value,
    key: String,
    value: Option<Value>,
    if_hlc: Option<Hlc>,
) -> Answer {
    let (answer, written) = oneshot::channel();
    // should the writer be gone, the write is dropped with its sender, and the answer says so
    let _ = writes.send(Write {
        id: id.clone(),
        key,
        value,
        if_hlc,
        answer,
    });
    Answer::Later(id, written)
}

/// Starts the writer on a thread of its own. What it returns completes once that thread ends, which, while the daemon
/// holds a sender of the queue, only a panic makes it do.
fn start_writer(shared: Arc<Shared>, queue: mpsc::UnboundedReceiver<Write>) -> oneshot::Receiver<()> {
    let (running, stopped) = oneshot::channel::<()>();
    thread::spawn(move || {
        // dropped when the thread ends, also by a panic
        let _running = running;
        writer(&shared, queue);
    });
    stopped
}

/// The tools' writes in groups, on a thread of its own, since the store's lock is held across the sync: what arrives
/// while one group is synced queues up and is written as the next, with one append and one sync for all of it.
fn writer(shared: &Shared, mut queue: mpsc::UnboundedReceiver<Write>) {
    // since when threads have been waiting for the store's lock after every group
    let mut kept_out = None;
    while let Some(first) = queue.blocking_recv() {
        let mut group = vec![first];
        while let Ok(write) = queue.try_recv() {
            group.push(write);
        }
        write_group(shared, group);
        if shared.waiting.load(Ordering::SeqCst) == 0 {
            kept_out = None;
        } else if kept_out.get_or_insert_with(Instant::now).elapsed() >= KEPT_OUT {
            let_them_in(shared);
            kept_out = None;
        }
    }
}

/// How long threads may wait for the store's lock while the writer takes it group after group, before it lets them in.
const KEPT_OUT: Duration = Duration::from_millis(100);
/// How long the writer waits, at most, for them to get it.
const HAND_OVER: Duration = Duration::from_millis(1);

/// The store's lock is free now. A mutex is not fair: under a steady stream of writes the writer takes it again before
/// a thread waiting for it gets in, and could keep it out for good. So once threads have waited through `KEPT_OUT` of
/// groups, the writer waits briefly for them to get it first, spinning rather than yielding (a yield costs a whole time
/// slice on a busy machine). Not after every group: the sync, let in, pushes what it has so far, and many small pushes
/// cost syncs on the hub and on every replica.
fn let_them_in(shared: &Shared) {
    let since = Instant::now();
    while shared.waiting.load(Ordering::SeqCst) > 0 && since.elapsed() < HAND_OVER {
        std::hint::spin_loop();
    }
}

/// Makes the writes of a group in their order, each checked against the state the ones before it left (a refused one
/// changes nothing), and commits them together. Each is answered, and published, only once it is durable. A commit that
/// fails took them out of memory again: each write whose answer the group's state decided gets the error.
fn write_group(shared: &Shared, group: Vec<Write>) {
    let mut store = shared.store();
    let mut made = Vec::new();
    store.begin();
    for write in group {
        let Write {
            id,
            key,
            value,
            if_hlc,
            answer,
        } = write;
        let result = make(shared, &mut store, &key, value, if_hlc.as_ref());
        made.push((id, answer, result));
    }
    let committed = store.commit();
    for (id, answer, result) in made {
        let result = match (&committed, result) {
            // what the state of the group decided goes with it: a write made, a delete that found nothing (an earlier
            // one of the group deleted it), a conflict or `ahead` against a stamp of the group
            (Err(err), Ok(_) | Err(store::Error::Conflict { .. } | store::Error::Ahead { .. })) => {
                Err(store::Error::Io(io::Error::new(err.kind(), err.to_string())))
            }
            // a check of the write alone (key, value, collection) stands
            (_, result) => result,
        };
        let _ = answer.send(written(shared, &store, &id, result));
    }
}

/// A tool's write, checked by the collections first.
fn make(
    shared: &Shared,
    store: &mut Store,
    key: &str,
    value: Option<Value>,
    if_hlc: Option<&Hlc>,
) -> Result<Option<Entry>, store::Error> {
    shared
        .rules()
        .check(key, value.as_ref(), store.node())
        .map_err(store::Error::Invalid)?;
    match value {
        Some(value) => store.put(key, value, if_hlc).map(Some),
        None => store.delete(key, if_hlc),
    }
}

fn protected(id: &Value, name: &str) -> Value {
    error(
        id,
        "protected",
        &format!("collection {name} is protected: only the hub's kharka.toml changes it"),
    )
}

/// The collections in force, each with who set it: `kharka.toml` for a protected one, else the node that wrote it.
fn collections_json(shared: &Shared) -> Value {
    let store = shared.store();
    let rules = shared.rules();
    let mut collections = Map::new();
    for (name, collection) in &rules.collections {
        let mut value = json!(collection);
        let entry = store.get(&format!("{COLLECTIONS}{name}"));
        value["by"] = match entry {
            _ if collection.protected => json!("kharka.toml"),
            Some(entry) => json!(entry.hlc.node),
            None => Value::Null,
        };
        if let Some(entry) = entry {
            value["hlc"] = json!(entry.hlc);
        }
        collections.insert(name.clone(), value);
    }
    json!({ "strict": rules.strict, "collections": collections })
}

/// The answer to a write; the change goes out while the caller still holds the store's lock.
fn written(shared: &Shared, store: &Store, id: &Value, result: Result<Option<Entry>, store::Error>) -> Value {
    match result {
        Ok(entry) => {
            if let Some(entry) = &entry {
                shared.publish(store, entry);
            }
            ok(id, json!({ "entry": entry }))
        }
        Err(store::Error::Conflict { current }) => {
            let mut answer = error(
                id,
                "conflict",
                &store::Error::Conflict {
                    current: current.clone(),
                }
                .to_string(),
            );
            answer["current"] = json!(current);
            answer
        }
        Err(err) => error(id, err.code(), &err.to_string()),
    }
}

/// A subscription and, for `initial`, the entries under the prefix right now, taken under one lock.
fn subscribe(shared: &Shared, prefix: &str, initial: bool) -> (broadcast::Receiver<Arc<Entry>>, Vec<Entry>) {
    let store = shared.store();
    let changes = shared.changes.subscribe();
    let current = if initial {
        store.list(prefix).cloned().collect()
    } else {
        Vec::new()
    };
    (changes, current)
}

/// A running watch: the current entries and `ready` (with `initial`), then one event per change under the prefix,
/// `sync` when the hub connection comes or goes, and `reset` plus everything again when it fell too far behind.
async fn watch(shared: Arc<Shared>, start: WatchStart, out: mpsc::Sender<String>) {
    let WatchStart {
        id,
        prefix,
        initial,
        mut changes,
        mut online,
        current,
        ..
    } = start;
    if initial && !replay(&shared, &out, &id, &current).await {
        return;
    }
    loop {
        tokio::select! {
            change = changes.recv() => match change {
                Ok(entry) => {
                    if entry.key.starts_with(&prefix) && !emit(&out, change_event(&shared, &id, &entry)).await {
                        return;
                    }
                }
                Err(RecvError::Lagged(_)) => {
                    let (fresh, current) = subscribe(&shared, &prefix, true);
                    changes = fresh;
                    if !emit(&out, json!({ "id": id, "event": "reset" })).await || !replay(&shared, &out, &id, &current).await {
                        return;
                    }
                }
                Err(RecvError::Closed) => return,
            },
            changed = online.changed() => {
                if changed.is_err() {
                    return;
                }
                let now = *online.borrow_and_update();
                if !emit(&out, json!({ "id": id, "event": "sync", "online": now })).await {
                    return;
                }
            }
        }
    }
}

async fn replay(shared: &Shared, out: &mpsc::Sender<String>, id: &Value, entries: &[Entry]) -> bool {
    for entry in entries {
        if !emit(out, change_event(shared, id, entry)).await {
            return false;
        }
    }
    emit(out, json!({ "id": id, "event": "ready" })).await
}

async fn emit(out: &mpsc::Sender<String>, value: Value) -> bool {
    out.send(line_of(value)).await.is_ok()
}

fn change_event(shared: &Shared, id: &Value, entry: &Entry) -> Value {
    json!({ "id": id, "event": "change", "entry": entry_json(shared, entry) })
}

/// An entry as tools see it: `stale` when it belongs to a state collection and is older than its TTL.
fn entry_json(shared: &Shared, entry: &Entry) -> Value {
    let mut value = json!(entry);
    if let Some(ttl) = shared.rules().ttl(&entry.key)
        && wall_ms().saturating_sub(entry.hlc.wall_ms) > ttl.as_millis() as u64
    {
        value["stale"] = json!(true);
    }
    value
}

fn ok(id: &Value, body: Value) -> Value {
    let mut answer = json!({ "id": id, "ok": true });
    if let (Value::Object(answer), Value::Object(body)) = (&mut answer, body) {
        answer.extend(body);
    }
    answer
}

fn error(id: &Value, code: &str, message: &str) -> Value {
    json!({ "id": id, "ok": false, "code": code, "error": message })
}

fn line_of(value: Value) -> String {
    let mut line = value.to_string();
    line.push('\n');
    line
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::hlc::MAX_DRIFT_MS;
    use crate::storage::Storage;
    use crate::store::tests::counting;

    /// The daemon's state around a store, without a hub (the sync's tests use it too).
    pub(crate) fn shared(store: Store) -> Shared {
        Shared {
            rules: RwLock::new(Shared::rules_of(&store, None)),
            store: Mutex::new(store),
            changes: broadcast::channel(BACKLOG).0,
            file: None,
            online: watch::channel(false).0,
            hub: None,
            scope: Scope::everything(),
            full_catch_up: AtomicBool::new(false),
            refused: Mutex::new(BTreeSet::new()),
            waiting: AtomicUsize::new(0),
        }
    }

    /// The writes through the writer, queued all at once, as by clients writing at the same time; their answers.
    fn write_all(shared: &Shared, writes: &[(&str, Option<Value>, Option<&Hlc>)]) -> Vec<Value> {
        let (writes_to, queued) = mpsc::unbounded_channel();
        let answers: Vec<_> = writes
            .iter()
            .enumerate()
            .map(|(n, (key, value, if_hlc))| {
                match queue(&writes_to, json!(n), key.to_string(), value.clone(), if_hlc.cloned()) {
                    Answer::Later(_, answer) => answer,
                    Answer::Now(answer) => panic!("answered at once: {answer}"),
                }
            })
            .collect();
        drop(writes_to);
        writer(shared, queued);
        answers
            .into_iter()
            .map(|answer| answer.blocking_recv().unwrap())
            .collect()
    }

    fn hlc(answer: &Value) -> Hlc {
        answer["entry"]["hlc"].as_str().unwrap().parse().unwrap()
    }

    fn published(changes: &mut broadcast::Receiver<Arc<Entry>>) -> Vec<(String, Value)> {
        let mut entries = Vec::new();
        while let Ok(entry) = changes.try_recv() {
            entries.push((entry.key.clone(), entry.value.clone()));
        }
        entries
    }

    fn appended(appends: &AtomicUsize) -> usize {
        appends.load(Ordering::SeqCst)
    }

    #[test]
    fn writes_arriving_together_are_one_append() {
        let (store, appends, _) = counting("a");
        let shared = shared(store);
        let mut changes = shared.changes.subscribe();
        let keys: Vec<String> = (0..20).map(|n| format!("k/{n}")).collect();
        let writes: Vec<_> = keys.iter().map(|key| (key.as_str(), Some(json!(key)), None)).collect();
        let answers = write_all(&shared, &writes);
        assert_eq!(appended(&appends), 1, "one append, one sync, for 20 writes");
        assert!(answers.iter().all(|answer| answer["ok"] == true), "{answers:?}");
        let stamps: Vec<Hlc> = answers.iter().map(hlc).collect();
        assert!(stamps.windows(2).all(|pair| pair[0] < pair[1]), "in their order");
        let published: Vec<String> = published(&mut changes).into_iter().map(|(key, _)| key).collect();
        assert_eq!(published, keys);
    }

    #[test]
    fn a_group_answers_each_write_as_if_it_came_alone() {
        let (store, appends, _) = counting("a");
        let shared = shared(store);
        let before = shared.store().put("k", json!(0), None).unwrap().hlc;
        let mut changes = shared.changes.subscribe();
        let answers = write_all(
            &shared,
            &[
                ("k", Some(json!(1)), Some(&before)),
                // the first changed the stamp: as if one came after the other
                ("k", Some(json!(2)), Some(&before)),
                ("_nodes/a", Some(json!(3)), None),
                ("missing", None, None),
                ("j", Some(json!(4)), None),
                ("k", None, Some(&before)),
            ],
        );
        assert_eq!(answers[0]["ok"], true);
        assert_eq!(answers[1]["code"], "conflict");
        assert_eq!(answers[1]["current"], answers[0]["entry"]["hlc"]);
        assert_eq!(answers[2]["code"], "invalid");
        assert_eq!((&answers[3]["ok"], &answers[3]["entry"]), (&json!(true), &Value::Null));
        assert_eq!(answers[4]["ok"], true);
        assert_eq!(answers[5]["code"], "conflict");
        assert_eq!(appended(&appends), 2, "the write before, then the group");
        assert_eq!(
            published(&mut changes),
            [("k".to_string(), json!(1)), ("j".to_string(), json!(4))]
        );
        let store = shared.store();
        assert_eq!(store.get("k").unwrap().value, json!(1));
        assert_eq!(store.get("j").unwrap().value, json!(4));
    }

    #[test]
    fn a_failed_group_answers_every_write_with_the_error_and_leaves_no_trace() {
        let (store, _, fail) = counting("a");
        let shared = shared(store);
        shared.store().put("k", json!(0), None).unwrap();
        let mut changes = shared.changes.subscribe();
        fail.store(true, Ordering::SeqCst);
        let answers = write_all(
            &shared,
            &[
                ("k", Some(json!(1)), None),
                ("_nodes/a", Some(json!(2)), None),
                ("j", Some(json!(3)), None),
            ],
        );
        let codes: Vec<&Value> = answers.iter().map(|answer| &answer["code"]).collect();
        assert_eq!(codes, [&json!("io"), &json!("invalid"), &json!("io")]);
        assert!(
            answers[0]["error"].as_str().unwrap().contains("disk full"),
            "{answers:?}"
        );
        assert!(
            published(&mut changes).is_empty(),
            "nothing announced that is not durable"
        );
        {
            let store = shared.store();
            assert_eq!(store.get("k").unwrap().value, json!(0));
            assert!(store.get("j").is_none());
        }
        fail.store(false, Ordering::SeqCst);
        let answers = write_all(&shared, &[("j", Some(json!(4)), None)]);
        assert_eq!(answers[0]["ok"], true);
    }

    #[test]
    fn a_failed_group_answers_each_write_its_state_decided_with_the_error() {
        let (store, _, fail) = counting("a");
        let shared = shared(store);
        shared.store().put("k", json!(0), None).unwrap();
        let mut changes = shared.changes.subscribe();
        fail.store(true, Ordering::SeqCst);
        let answers = write_all(
            &shared,
            &[
                ("k", None, None),
                // finds nothing to delete, since the one before deleted it
                ("k", None, None),
                ("j", Some(json!(1)), None),
                // a conflict with the stamp the one before gave j
                ("j", Some(json!(2)), Some(&"1-0-a".parse().unwrap())),
                ("_nodes/a", Some(json!(3)), None),
            ],
        );
        let codes: Vec<&str> = answers.iter().map(|answer| answer["code"].as_str().unwrap()).collect();
        assert_eq!(codes, ["io", "io", "io", "io", "invalid"], "{answers:?}");
        assert!(published(&mut changes).is_empty());
        let store = shared.store();
        assert_eq!(store.get("k").unwrap().value, json!(0));
        assert!(store.held("j").is_none());
    }

    /// Storage whose appends panic, as a bug in the writer would.
    struct Panicking;

    impl Storage for Panicking {
        fn load(&mut self) -> io::Result<Vec<Entry>> {
            Ok(Vec::new())
        }
        fn append(&mut self, _: &[Entry]) -> io::Result<()> {
            panic!("a bug in the writer");
        }
        fn compact(&mut self, _: &mut dyn Iterator<Item = &Entry>) -> io::Result<()> {
            Ok(())
        }
        fn appended(&self) -> usize {
            0
        }
        fn appended_bytes(&self) -> u64 {
            0
        }
        fn snapshot_bytes(&self) -> u64 {
            0
        }
        fn damaged(&self) -> bool {
            false
        }
    }

    #[test]
    fn a_writer_that_panics_is_noticed() {
        let store = Store::with_storage(Box::new(Panicking), "a", store::Options::default()).unwrap();
        let (writes, queued) = mpsc::unbounded_channel();
        let stopped = start_writer(Arc::new(shared(store)), queued);
        let answer = match queue(&writes, json!(1), "k".into(), Some(json!(1)), None) {
            Answer::Later(_, answer) => answer,
            Answer::Now(answer) => panic!("answered at once: {answer}"),
        };
        assert!(answer.blocking_recv().is_err(), "the write is dropped with the writer");
        assert!(
            stopped.blocking_recv().is_err(),
            "the daemon learns that its writer is gone, though it still holds the queue"
        );
        drop(writes);
    }

    #[test]
    fn a_write_in_place_of_an_own_entry_far_ahead_goes_with_its_group() {
        let (store, appends, _) = counting("a");
        let shared = shared(store);
        // as if loaded from the log: a write of this node made while its clock was far ahead, which the hub never had
        let far = Hlc {
            wall_ms: wall_ms() + MAX_DRIFT_MS + 60_000,
            counter: 0,
            node: "a".into(),
        };
        shared
            .store()
            .from_hub(Entry::new("far", Some(json!("ahead")), far.clone()))
            .unwrap();
        let mut changes = shared.changes.subscribe();
        let before = appended(&appends);
        let answers = write_all(
            &shared,
            &[
                ("a", Some(json!(1)), None),
                ("far", Some(json!(2)), None),
                ("b", Some(json!(3)), None),
            ],
        );
        assert!(answers.iter().all(|answer| answer["ok"] == true), "{answers:?}");
        let stamps: Vec<Hlc> = answers.iter().map(hlc).collect();
        assert!(stamps.windows(2).all(|pair| pair[0] < pair[1]) && stamps[1] < far);
        assert_eq!(
            appended(&appends) - before,
            0,
            "the group went in by one compaction, decided by the write's own reading of the clock"
        );
        let keys: Vec<String> = published(&mut changes).into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, ["a", "far", "b"]);
        assert_eq!(shared.store().get("far").unwrap().value, json!(2));
    }
}
