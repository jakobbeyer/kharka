//! The daemon: this machine's replica behind a Unix socket, JSON lines in and out, and the sync with the hub.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard};
use std::time::Duration;

use anyhow::Context;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};

use crate::entry::{COLLECTIONS, Entry, MAX_VALUE, REJECTED, SETTINGS, check_key, check_prefix};
use crate::hlc::{Hlc, wall_ms};
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
}

/// Whether a key holds rules: a collection or the settings.
fn is_rule(key: &str) -> bool {
    key.starts_with(COLLECTIONS) || key == SETTINGS
}

impl Shared {
    pub fn store(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
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
    });
    let syncing = options
        .hub
        .map(|hub| tokio::spawn(sync::run(shared.clone(), hub, options.data.clone())));
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    tokio::spawn(serve(stream, shared.clone()));
                }
                Err(err) => {
                    eprintln!("kharka: accept failed: {err}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            () = &mut shutdown => break,
        }
    }
    if let Some(task) = syncing {
        task.abort();
    }
    let _ = fs::remove_file(&options.socket);
    Ok(())
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
async fn serve(stream: UnixStream, shared: Arc<Shared>) {
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
            Ok(line) => handle(&shared, &line, &mut watches, &mut started),
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

/// The answer to one request line. A watch is subscribed here and left in `started` for the caller to run.
fn handle(
    shared: &Shared,
    line: &str,
    watches: &mut HashMap<String, JoinHandle<()>>,
    started: &mut Option<WatchStart>,
) -> Value {
    let raw: Value = match serde_json::from_str(line) {
        Ok(raw) => raw,
        Err(err) => return error(&Value::Null, "invalid", &format!("not JSON: {err}")),
    };
    let id = raw.get("id").cloned().unwrap_or(Value::Null);
    let request = match Request::deserialize(raw) {
        Ok(request) => request,
        Err(err) => return error(&id, "invalid", &err.to_string()),
    };
    let scope = &shared.scope;
    let not_synced = |what: &str| {
        error(
            &id,
            "not_synced",
            &format!("{what} is not synced to this machine, which syncs {scope}"),
        )
    };
    match request {
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
        Request::Put { key, value, if_hlc } => {
            let mut store = shared.store();
            let checked = shared.rules().check(&key, Some(&value), store.node());
            let result = match checked {
                Ok(()) => store.put(&key, value, if_hlc.as_ref()).map(Some),
                Err(problem) => Err(store::Error::Invalid(problem)),
            };
            written(shared, &store, &id, result)
        }
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
        Request::Del { key, if_hlc } => {
            let mut store = shared.store();
            let checked = shared.rules().check(&key, None, store.node());
            let result = match checked {
                Ok(()) => store.delete(&key, if_hlc.as_ref()),
                Err(problem) => Err(store::Error::Invalid(problem)),
            };
            written(shared, &store, &id, result)
        }
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
                return error(&id, "invalid", &problem);
            }
            let name = id.to_string();
            if watches.get(&name).is_some_and(|task| !task.is_finished()) {
                return error(&id, "invalid", "a watch with this id runs already on this connection");
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
            // writes the hub refused for good are not pending: they are no longer pushed
            let pending = {
                let refused = shared.refused();
                store
                    .pending()
                    .iter()
                    .filter(|entry| !refused.contains(&(entry.key.clone(), entry.hlc.to_string())))
                    .count()
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
                    "refused": shared.refused().len(),
                    "sync": (!scope.is_everything()).then(|| scope.prefixes()),
                }),
            )
        }
        Request::Collections => ok(&id, collections_json(shared)),
        Request::Define { name, definition } => {
            let mut store = shared.store();
            let result = match check_definition(&name, &definition) {
                Err(problem) => Err(store::Error::Invalid(problem)),
                Ok(_) if shared.protected(&store, &name) => return protected(&id, &name),
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
                return protected(&id, &name);
            } else if store.get(&key).is_none() {
                Ok(None)
            } else {
                store.write_own(&key, None).map(Some)
            };
            written(shared, &store, &id, result)
        }
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
