//! Benchmarks, ignored by `cargo test`: local writes, the fan-out through the hub, a catch-up and a load. Each prints
//! one line; the numbers are only comparable on one machine, before and after a change. Run them with
//!
//! ```sh
//! cargo test --release --test bench -- --ignored --nocapture --test-threads=1
//! ```

mod common;

use std::io::{BufRead, BufReader, Lines, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use common::*;
use kharka::entry::Entry;
use kharka::hlc::{Hlc, wall_ms};
use kharka::store::{Options, Store};
use serde_json::{Value, json};

/// A connection to a daemon's socket, asking one request at a time.
struct Client {
    writer: UnixStream,
    lines: Lines<BufReader<UnixStream>>,
    next_id: u64,
}

impl Client {
    fn connect(dir: &Path, node: &str) -> Client {
        let stream = UnixStream::connect(socket(dir, node)).unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            writer,
            lines: BufReader::new(stream).lines(),
            next_id: 0,
        }
    }

    fn ask(&mut self, mut request: Value) -> Value {
        self.next_id += 1;
        request["id"] = json!(self.next_id);
        writeln!(self.writer, "{request}").unwrap();
        let answer: Value = serde_json::from_str(&self.lines.next().unwrap().unwrap()).unwrap();
        assert_eq!(answer["ok"], true, "{request} → {answer}");
        answer
    }

    fn put(&mut self, key: &str, value: Value) {
        self.ask(json!({ "op": "put", "key": key, "value": value }));
    }

    fn status(&mut self) -> Value {
        self.ask(json!({ "op": "status" }))
    }
}

fn hub_target(dir: &Path) -> String {
    hub_socket(dir).display().to_string()
}

/// A small value, as a tool would report it.
fn small(n: usize) -> Value {
    json!({ "agent": "bench", "n": n, "busy": n.is_multiple_of(2) })
}

/// `clients` connections, each putting `each` small values one after the other: the time it took, and every put's
/// latency.
fn concurrent_puts(dir: &Path, node: &str, prefix: &str, clients: usize, each: usize) -> (Duration, Vec<Duration>) {
    let started = Instant::now();
    let threads: Vec<_> = (0..clients)
        .map(|c| {
            let mut client = Client::connect(dir, node);
            let prefix = prefix.to_string();
            thread::spawn(move || {
                (0..each)
                    .map(|n| {
                        let before = Instant::now();
                        client.put(&format!("{prefix}/{c}/{n}"), small(n));
                        before.elapsed()
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut latencies: Vec<Duration> = threads.into_iter().flat_map(|t| t.join().unwrap()).collect();
    let took = started.elapsed();
    latencies.sort();
    (took, latencies)
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn report_puts(name: &str, clients: usize, (took, latencies): (Duration, Vec<Duration>)) {
    println!(
        "bench {name}: {clients} client(s) x {} puts in {:.2} s = {:.0} writes/s, latency p50 {:.2} ms, p99 {:.2} ms",
        latencies.len() / clients,
        took.as_secs_f64(),
        latencies.len() as f64 / took.as_secs_f64(),
        ms(percentile(&latencies, 0.5)),
        ms(percentile(&latencies, 0.99)),
    );
}

#[test]
#[ignore]
fn puts_concurrent() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _daemon = start_daemon(dir, "a", None);
    report_puts("puts_concurrent", 8, concurrent_puts(dir, "a", "bench", 8, 500));
}

#[test]
#[ignore]
fn puts_sequential() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _daemon = start_daemon(dir, "a", None);
    report_puts("puts_sequential", 1, concurrent_puts(dir, "a", "bench", 1, 2000));
}

/// The CPU time a process has used so far (user and system), as `ps` reports it.
fn cpu_time(pid: u32) -> Duration {
    let output = Command::new("ps")
        .args(["-o", "time=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    // [[dd-]hh:]mm:ss.cc
    let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let (days, rest) = text.split_once('-').unwrap_or(("0", &text));
    let seconds = rest
        .split(':')
        .fold(0.0, |total, part| total * 60.0 + part.parse::<f64>().unwrap());
    Duration::from_secs_f64(days.parse::<f64>().unwrap() * 86400.0 + seconds)
}

/// A hub and `daemons` daemons; the first one writes 2000 keys from 8 connections at once. Reports how long the writes
/// took, when the hub had them all (nothing pending on the writer), when every other daemon held them all, and the
/// hub's CPU time meanwhile.
fn fan_out(daemons: usize) {
    const KEYS: usize = 2000;
    const CLIENTS: usize = 8;
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let hub = start_hub(dir);
    let target = hub_target(dir);
    let nodes: Vec<String> = (0..daemons).map(|n| format!("d{n}")).collect();
    let _daemons: Vec<Process> = nodes
        .iter()
        .map(|node| start_daemon(dir, node, Some(&target)))
        .collect();
    let mut clients: Vec<Client> = nodes.iter().map(|node| Client::connect(dir, node)).collect();
    // everyone synced and knowing everyone: the entry counts stay put until the writes come
    let mut before = Vec::new();
    for client in &mut clients {
        wait_until("synced, with every node known", || {
            let nodes = client.ask(json!({ "op": "ls", "prefix": "_nodes/" }));
            let online = nodes["entries"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|entry| entry["value"]["online"] == true)
                .count();
            client.status()["online"] == true && online == daemons
        });
        before.push(client.status()["entries"].as_u64().unwrap());
    }

    let cpu_before = cpu_time(hub.pid());
    let started = Instant::now();
    let (wrote, _) = concurrent_puts(dir, &nodes[0], "fan", CLIENTS, KEYS / CLIENTS);
    // polled only now, so that the polling does not slow the writes; no daemon can be done before the last put
    let mut done: Vec<Option<Duration>> = vec![None; daemons];
    let deadline = Instant::now() + Duration::from_secs(60);
    while done.iter().any(Option::is_none) {
        assert!(Instant::now() < deadline, "gave up waiting for the fan-out");
        for (n, client) in clients.iter_mut().enumerate() {
            if done[n].is_some() {
                continue;
            }
            let status = client.status();
            // the writer holds everything at once; it is done when the hub has confirmed every write
            let complete = if n == 0 {
                status["pending"] == 0
            } else {
                status["entries"].as_u64() == Some(before[n] + KEYS as u64)
            };
            if complete {
                done[n] = Some(started.elapsed());
            }
        }
        thread::sleep(Duration::from_millis(2));
    }
    let cpu = cpu_time(hub.pid()) - cpu_before;
    let confirmed = done[0].unwrap();
    let elsewhere = match done[1..].iter().flatten().max() {
        Some(last) => format!("every other daemon after {:.2} s", last.as_secs_f64()),
        None => "no other daemon".to_string(),
    };
    let everywhere = done.iter().flatten().max().copied().unwrap();
    println!(
        "bench fan_out_{daemons}: {daemons} daemon(s), {KEYS} puts on one from {CLIENTS} connections in {:.2} s \
         ({:.0} writes/s); hub confirmed all after {:.2} s, {elsewhere}; done {:.0} ms after the last put; hub CPU \
         {:.2} s",
        wrote.as_secs_f64(),
        KEYS as f64 / wrote.as_secs_f64(),
        confirmed.as_secs_f64(),
        ms(everywhere.saturating_sub(wrote)),
        cpu.as_secs_f64(),
    );
}

#[test]
#[ignore]
fn fan_out_1() {
    fan_out(1);
}

#[test]
#[ignore]
fn fan_out_5() {
    fan_out(5);
}

#[test]
#[ignore]
fn fan_out_10() {
    fan_out(10);
}

/// A hub's log holding `keys` entries of about 200 bytes each, written in-process in one batch.
fn seed_hub(dir: &Path, keys: usize) {
    let options = Options {
        sync: false,
        ..Options::default()
    };
    let mut store = Store::open(&dir.join("hub"), "hub", options).unwrap();
    let wall = wall_ms();
    let filler = "x".repeat(120);
    store.begin();
    for n in 0..keys {
        let hlc = Hlc {
            wall_ms: wall,
            counter: n as u32,
            node: "seed".into(),
        };
        let value = json!({ "title": format!("entry {n}"), "status": "open", "note": filler });
        store
            .accept(Entry::new(format!("bulk/{n:06}"), Some(value), hlc))
            .unwrap();
    }
    store.commit().unwrap();
}

/// A hub holding 100k keys: how long its start takes, a fresh daemon's catch-up from nothing, and that daemon's
/// restart, loading them from its own folder.
#[test]
#[ignore]
fn catch_up_and_load() {
    const KEYS: usize = 100_000;
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    seed_hub(dir, KEYS);
    // the commit compacts: the entries are in the snapshot
    let size: u64 = ["snapshot", "data.log"]
        .iter()
        .map(|file| std::fs::metadata(dir.join("hub").join(file)).unwrap().len())
        .sum();

    let started = Instant::now();
    let _hub = start_hub(dir);
    let hub_start = started.elapsed();
    let target = hub_target(dir);

    let has_all = |client: &mut Client| {
        let status = client.status();
        status["online"] == true && status["entries"].as_u64() >= Some(KEYS as u64)
    };
    let started = Instant::now();
    let daemon = start_daemon(dir, "fresh", Some(&target));
    let mut client = Client::connect(dir, "fresh");
    wait_until("the fresh daemon caught up", || has_all(&mut client));
    let catch_up = started.elapsed();

    drop(client);
    drop(daemon);
    std::fs::remove_file(socket(dir, "fresh")).unwrap();
    let started = Instant::now();
    let _daemon = start_daemon(dir, "fresh", Some(&target));
    let load = started.elapsed();
    let mut client = Client::connect(dir, "fresh");
    assert!(client.status()["entries"].as_u64() >= Some(KEYS as u64));
    wait_until("the restarted daemon synced", || has_all(&mut client));
    let resync = started.elapsed();

    println!(
        "bench catch_up_and_load: {KEYS} keys ({:.1} MB on disk); hub start {:.2} s; fresh daemon synced after {:.2} s; \
         restart: socket answers after {:.2} s, synced after {:.2} s",
        size as f64 / 1e6,
        hub_start.as_secs_f64(),
        catch_up.as_secs_f64(),
        load.as_secs_f64(),
        resync.as_secs_f64(),
    );
}
