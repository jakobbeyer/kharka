//! The `kharka` command: the daemon, the hub, the bridge, and a client for scripts.

use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Context;
use clap::{Parser, Subcommand};
use serde_json::{Value, json};

use kharka::client::Client;
use kharka::schema::Collections;
use kharka::scope::Scope;
use kharka::sync::HubTarget;
use kharka::{bridge, daemon, entry, hub, paths, service};

/// A small local-first JSON store: every machine keeps a full replica, one hub orders and spreads the changes.
///
/// Exit codes: 0 ok, 1 error, 2 usage, 3 not found, 4 conflict (the key changed since --if-hlc).
#[derive(Parser)]
#[command(version, about, max_term_width = 110)]
struct Cli {
    /// The daemon's socket [default: $XDG_RUNTIME_DIR/kharka/daemon.sock, else ~/.local/state/kharka/daemon.sock; a client
    /// also looks in /run/user/<uid>/kharka/]
    #[arg(long, env = "KHARKA_SOCKET", global = true)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run this machine's replica: the socket for tools, the sync with the hub
    Daemon {
        /// Where the replica lives [default: $XDG_DATA_HOME/kharka/replica]
        #[arg(long, env = "KHARKA_DATA")]
        data: Option<PathBuf>,
        /// This machine's name [default: the host name]
        #[arg(long, env = "KHARKA_NODE")]
        node: Option<String>,
        /// The hub: an ssh destination (server, user@host), `local`, or the path of its socket
        #[arg(long, env = "KHARKA_HUB")]
        hub: Option<String>,
        /// Without a hub: this machine's protected collections [default: ~/.config/kharka/kharka.toml]
        #[arg(long, env = "KHARKA_CONFIG")]
        config: Option<PathBuf>,
        /// Sync only the keys under these prefixes, e.g. tasks/,status/ [default: everything]
        #[arg(long, env = "KHARKA_SYNC", value_delimiter = ',')]
        sync: Vec<String>,
    },
    /// Run the hub that every daemon syncs with
    Serve {
        /// Where the hub's log lives [default: $XDG_DATA_HOME/kharka/hub]
        #[arg(long, env = "KHARKA_HUB_DATA")]
        data: Option<PathBuf>,
        /// The hub's own name in its stamps
        #[arg(long, default_value = "hub")]
        node: String,
        /// The collections [default: ~/.config/kharka/kharka.toml]
        #[arg(long, env = "KHARKA_CONFIG")]
        config: Option<PathBuf>,
        /// The hub's socket [default: hub.sock next to the daemon's]
        #[arg(long, env = "KHARKA_HUB_SOCKET")]
        hub_socket: Option<PathBuf>,
    },
    /// Connect stdin and stdout to the hub's socket (what ssh runs on the hub's machine)
    Bridge {
        #[arg(long, env = "KHARKA_HUB_SOCKET")]
        hub_socket: Option<PathBuf>,
    },
    /// Print a value (exit 3 when the key does not exist)
    Get {
        key: String,
        /// The whole entry: value, stamp, sequence number
        #[arg(long)]
        entry: bool,
    },
    /// Write a value and print its stamp
    Put {
        key: String,
        /// JSON, or - to read it from stdin
        value: String,
        /// Only if the key still has this stamp (exit 4 otherwise)
        #[arg(long)]
        if_hlc: Option<String>,
    },
    /// Delete a key
    Del {
        key: String,
        /// Only if the key still has this stamp (exit 4 otherwise)
        #[arg(long)]
        if_hlc: Option<String>,
    },
    /// List the keys under a prefix
    Ls {
        #[arg(default_value = "")]
        prefix: String,
        /// The entries as JSON lines instead of the keys
        #[arg(long)]
        entries: bool,
    },
    /// Print every change under a prefix as a JSON line, until interrupted
    Watch {
        #[arg(default_value = "")]
        prefix: String,
        /// Start with the current entries, then a `ready` line
        #[arg(long)]
        initial: bool,
    },
    /// Print this machine's node name
    Whoami,
    /// Print the daemon's state: node, hub, online, entries, pending writes
    Status,
    /// Show the collections in force here, define your own, or check a kharka.toml
    ///
    /// Collections from the hub's kharka.toml are protected: only the file changes them. Any machine defines and
    /// changes the others.
    #[command(args_conflicts_with_subcommands = true)]
    Collections {
        #[command(subcommand)]
        action: Option<CollectionsAction>,
        /// One collection, with its schema, as JSON
        name: Option<String>,
        /// All of them as JSON
        #[arg(long)]
        json: bool,
        /// Check this kharka.toml and its schema files instead, without a daemon (exit 1 when it does not load)
        #[arg(long, value_name = "FILE")]
        check: Option<PathBuf>,
    },
    /// Print a launchd agent (macOS) or systemd user unit (Linux) that keeps a role running
    ///
    /// The arguments after the role are passed on, e.g. `kharka service daemon --hub server`.
    Service {
        #[arg(value_parser = ["daemon", "serve"])]
        role: String,
        /// Write for this system instead of the one here
        #[arg(long, value_parser = ["launchd", "systemd"])]
        system: Option<String>,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}

#[derive(Subcommand)]
enum CollectionsAction {
    /// Define or change a collection of your own (a protected one only changes in the hub's kharka.toml)
    Set {
        name: String,
        /// record: any machine writes, the latest write wins; state: each machine writes only NAME/<node>/…
        #[arg(long, default_value = "record", value_parser = ["record", "state"])]
        kind: String,
        /// For state: how long an entry stays fresh, e.g. 30s
        #[arg(long)]
        ttl: Option<String>,
        /// A JSON Schema file for the values, or - to read it from stdin
        #[arg(long)]
        schema: Option<String>,
    },
    /// Remove a collection of your own; its data stays
    Rm { name: String },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => code,
        Err(err) => {
            eprintln!("kharka: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> anyhow::Result<ExitCode> {
    // the daemon listens at the default place; a client looks where a daemon of this user may listen
    let explicit = cli.socket;
    let socket = explicit.clone().unwrap_or_else(paths::socket);
    match cli.command {
        Command::Daemon {
            data,
            node,
            hub,
            config,
            sync,
        } => {
            let node = node.unwrap_or_else(paths::node);
            entry::check_node(&node).map_err(anyhow::Error::msg)?;
            let options = daemon::Options {
                socket,
                data: data.unwrap_or_else(paths::data),
                node,
                hub: hub
                    .as_deref()
                    .map(HubTarget::parse)
                    .transpose()
                    .map_err(anyhow::Error::msg)?,
                config: Some(config.unwrap_or_else(paths::config)),
                sync: Scope::of(sync).map_err(anyhow::Error::msg)?,
            };
            runtime()?.block_on(daemon::run(options, shutdown()))?;
        }
        Command::Serve {
            data,
            node,
            config,
            hub_socket,
        } => {
            let options = hub::Options {
                socket: hub_socket.unwrap_or_else(paths::hub_socket),
                data: data.unwrap_or_else(paths::hub_data),
                node,
                config: Some(config.unwrap_or_else(paths::config)),
            };
            runtime()?.block_on(hub::run(options, shutdown()))?;
        }
        Command::Bridge { hub_socket } => {
            let sockets = hub_socket.map_or_else(paths::hub_socket_candidates, |socket| vec![socket]);
            // stdin is read on a blocking thread, which waits for the next byte: once the hub is gone, the runtime must
            // not wait for it, or the bridge (and with it the ssh connection) outlives the hub
            let runtime = runtime()?;
            let bridged = runtime.block_on(bridge::run(&sockets));
            runtime.shutdown_background();
            bridged?;
        }
        Command::Service { role, system, args } => {
            let system = match system.as_deref() {
                Some("launchd") => service::System::Launchd,
                Some("systemd") => service::System::Systemd,
                _ => service::System::here(),
            };
            let exe = std::env::current_exe().context("finding the kharka binary")?;
            let log = PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(format!("Library/Logs/kharka-{role}.log"));
            print!("{}", service::file(system, &exe, &role, &args, &log));
            eprintln!("Install it:\n{}", service::instructions(system, &role));
        }
        Command::Collections {
            check: Some(file),
            action: None,
            ..
        } => {
            let collections = Collections::load(&file)?;
            let mut out = io::stdout().lock();
            writeln!(out, "{}: valid", file.display())?;
            print_collections(&mut out, &collections, &Value::Null)?;
        }
        command => return client_command(&explicit.unwrap_or_else(paths::find_socket), command),
    }
    Ok(ExitCode::SUCCESS)
}

fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()
}

/// Ctrl-C or SIGTERM.
async fn shutdown() {
    use tokio::signal::unix::{SignalKind, signal};
    match signal(SignalKind::terminate()) {
        Ok(mut terminate) => {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
        }
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
        }
    }
}

fn client_command(socket: &std::path::Path, command: Command) -> anyhow::Result<ExitCode> {
    let mut client = Client::connect(socket)?;
    let mut out = io::stdout().lock();
    match command {
        Command::Get { key, entry } => {
            let answer = client.call(json!({ "op": "get", "key": key }))?;
            if answer["ok"] != true {
                return Ok(refused(&answer));
            }
            let found = &answer["entry"];
            if found.is_null() {
                eprintln!("kharka: {key}: not found");
                return Ok(ExitCode::from(3));
            }
            writeln!(out, "{}", if entry { found } else { &found["value"] })?;
        }
        Command::Put { key, value, if_hlc } => {
            let text = if value == "-" {
                let mut text = String::new();
                io::stdin().read_to_string(&mut text)?;
                text
            } else {
                value
            };
            let value: Value =
                serde_json::from_str(&text).context("the value is not JSON (a string needs quotes: '\"text\"')")?;
            let answer = client.call(json!({ "op": "put", "key": key, "value": value, "if_hlc": if_hlc }))?;
            if answer["ok"] != true {
                return Ok(refused(&answer));
            }
            writeln!(out, "{}", answer["entry"]["hlc"].as_str().unwrap_or_default())?;
        }
        Command::Del { key, if_hlc } => {
            let answer = client.call(json!({ "op": "del", "key": key, "if_hlc": if_hlc }))?;
            if answer["ok"] != true {
                return Ok(refused(&answer));
            }
            if answer["entry"].is_null() {
                eprintln!("kharka: {key}: not found");
                return Ok(ExitCode::from(3));
            }
        }
        Command::Ls { prefix, entries } => {
            let answer = client.call(json!({ "op": "ls", "prefix": prefix }))?;
            if answer["ok"] != true {
                return Ok(refused(&answer));
            }
            for found in answer["entries"].as_array().into_iter().flatten() {
                if entries {
                    writeln!(out, "{found}")?;
                } else {
                    writeln!(out, "{}", found["key"].as_str().unwrap_or_default())?;
                }
            }
        }
        Command::Watch { prefix, initial } => {
            let answer = client.call(json!({ "op": "watch", "prefix": prefix, "initial": initial }))?;
            if answer["ok"] != true {
                return Ok(refused(&answer));
            }
            loop {
                let mut event = client.read()?;
                if let Value::Object(fields) = &mut event {
                    fields.remove("id");
                }
                // a closed pipe (| head) ends the watch quietly
                if writeln!(out, "{event}").and_then(|()| out.flush()).is_err() {
                    break;
                }
            }
        }
        Command::Whoami => writeln!(out, "{}", client.whoami()?)?,
        Command::Status => {
            let mut answer = client.call(json!({ "op": "status" }))?;
            if let Value::Object(fields) = &mut answer {
                fields.remove("id");
                fields.remove("ok");
            }
            writeln!(out, "{}", serde_json::to_string_pretty(&answer)?)?;
        }
        Command::Collections {
            action:
                Some(CollectionsAction::Set {
                    name,
                    kind,
                    ttl,
                    schema,
                }),
            ..
        } => {
            let mut definition = json!({ "kind": kind });
            if let Some(ttl) = ttl {
                definition["ttl"] = json!(ttl);
            }
            if let Some(schema) = schema {
                let text = if schema == "-" {
                    let mut text = String::new();
                    io::stdin().read_to_string(&mut text)?;
                    text
                } else {
                    std::fs::read_to_string(&schema).with_context(|| format!("reading {schema}"))?
                };
                definition["schema"] = serde_json::from_str(&text).context("the schema is not JSON")?;
            }
            let answer = client.call(json!({ "op": "define", "name": name, "definition": definition }))?;
            if answer["ok"] != true {
                return Ok(refused(&answer));
            }
        }
        Command::Collections {
            action: Some(CollectionsAction::Rm { name }),
            ..
        } => {
            let answer = client.call(json!({ "op": "undefine", "name": name }))?;
            if answer["ok"] != true {
                return Ok(refused(&answer));
            }
            if answer["entry"].is_null() {
                eprintln!("kharka: no collection {name:?} of your own");
                return Ok(ExitCode::from(3));
            }
        }
        Command::Collections { name, json, .. } => {
            let answer = client.call(json!({ "op": "collections" }))?;
            if answer["ok"] != true {
                return Ok(refused(&answer));
            }
            let found = &answer["collections"];
            // the answer adds who set each collection; the definitions alone are read as collections
            let mut definitions = found.clone();
            if let Value::Object(collections) = &mut definitions {
                for collection in collections.values_mut() {
                    if let Value::Object(fields) = collection {
                        fields.remove("by");
                        fields.remove("hlc");
                    }
                }
            }
            if let Some(name) = name {
                if found[&name].is_null() {
                    eprintln!("kharka: no collection {name:?}");
                    return Ok(ExitCode::from(3));
                }
                writeln!(out, "{}", serde_json::to_string_pretty(&found[&name])?)?;
            } else if json {
                writeln!(
                    out,
                    "{}",
                    serde_json::to_string_pretty(&json!({ "strict": answer["strict"], "collections": found }))?
                )?;
            } else {
                let collections: Collections =
                    serde_json::from_value(json!({ "strict": answer["strict"], "collections": definitions }))
                        .context("the daemon sent collections that do not read")?;
                print_collections(&mut out, &collections, found)?;
                if collections.strict {
                    writeln!(out, "(strict: every key needs a collection)")?;
                } else {
                    writeln!(out, "(open: keys without a collection are not checked)")?;
                }
            }
        }
        Command::Daemon { .. } | Command::Serve { .. } | Command::Bridge { .. } | Command::Service { .. } => {
            unreachable!("handled in run")
        }
    }
    Ok(ExitCode::SUCCESS)
}

/// A refused request: its message, exit 4 for a conflict, else 1.
fn refused(answer: &Value) -> ExitCode {
    eprintln!("kharka: {}", answer["error"].as_str().unwrap_or("refused"));
    if answer["code"] == "conflict" {
        ExitCode::from(4)
    } else {
        ExitCode::FAILURE
    }
}

/// One line per collection: name, kind, TTL, what its schema asks for, and who set it (from `details`, the daemon's
/// answer: `kharka.toml` for a protected one, else a node).
fn print_collections(out: &mut impl Write, collections: &Collections, details: &Value) -> io::Result<()> {
    let width = collections.collections.keys().map(String::len).max().unwrap_or(0);
    for (name, collection) in &collections.collections {
        let kind = match collection.kind {
            kharka::schema::Kind::State => "state",
            kharka::schema::Kind::Record => "record",
        };
        let ttl = collection
            .ttl
            .as_deref()
            .map(|ttl| format!("ttl {ttl}"))
            .unwrap_or_default();
        let schema = collection
            .schema
            .as_ref()
            .map_or("no schema".to_string(), describe_schema);
        let by = match details[name]["by"].as_str() {
            Some("kharka.toml") => "  protected".to_string(),
            Some(node) => format!("  by {node}"),
            None => String::new(),
        };
        writeln!(out, "{name:width$}  {kind:6}  {ttl:9}  {schema}{by}")?;
    }
    Ok(())
}

/// A schema in a few words: `object, 6 properties, required: title, status`.
fn describe_schema(schema: &Value) -> String {
    let mut parts = vec![];
    match &schema["type"] {
        Value::String(kind) => parts.push(kind.clone()),
        Value::Array(kinds) => parts.push(kinds.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("|")),
        _ => {}
    }
    if let Some(properties) = schema["properties"].as_object() {
        parts.push(format!("{} properties", properties.len()));
    }
    if let Some(required) = schema["required"].as_array() {
        let names: Vec<&str> = required.iter().filter_map(Value::as_str).collect();
        parts.push(format!("required: {}", names.join(", ")));
    }
    if parts.is_empty() {
        "schema".to_string()
    } else {
        format!("schema: {}", parts.join(", "))
    }
}
