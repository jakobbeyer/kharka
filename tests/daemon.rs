//! One daemon on its own: the command line, the socket protocol, watches, durability.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::process::Stdio;

use common::*;
use serde_json::{Value, json};

#[test]
fn put_get_ls_del_and_exit_codes() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _daemon = start_daemon(dir, "a", None);

    let first = run(dir, "a", &["put", "topics/1", r#"{"title":"Price list"}"#]);
    assert!(first.status.success(), "{}", stderr(&first));
    let hlc = stdout(&first);
    assert!(hlc.ends_with("-a"), "{hlc}");
    put(dir, "a", "topics/2", "2");

    assert_eq!(get(dir, "a", "topics/1").as_deref(), Some(r#"{"title":"Price list"}"#));
    let entry: Value = serde_json::from_str(&stdout(&run(dir, "a", &["get", "topics/1", "--entry"]))).unwrap();
    assert_eq!(entry["hlc"], json!(hlc));
    assert_eq!(stdout(&run(dir, "a", &["ls", "topics/"])), "topics/1\ntopics/2");
    assert_eq!(stdout(&run(dir, "a", &["whoami"])), "a");

    // a conditional write with an old stamp: exit 4, nothing written
    put(dir, "a", "topics/1", r#"{"title":"Price list v2"}"#);
    let stale = run(dir, "a", &["put", "topics/1", "{}", "--if-hlc", &hlc]);
    assert_eq!(stale.status.code(), Some(4), "{}", stderr(&stale));
    assert!(stderr(&stale).contains("changed meanwhile"));

    assert!(run(dir, "a", &["del", "topics/1"]).status.success());
    let missing = run(dir, "a", &["get", "topics/1"]);
    assert_eq!(missing.status.code(), Some(3));
    assert_eq!(stdout(&run(dir, "a", &["ls"])), "topics/2");

    let bad = run(dir, "a", &["put", "topics/3", "not json"]);
    assert_eq!(bad.status.code(), Some(1));
    let reserved = run(dir, "a", &["put", "_nodes/a", "1"]);
    assert_eq!(reserved.status.code(), Some(1));
    assert!(stderr(&reserved).contains("written by kharka itself"));

    let status = status(dir, "a");
    assert_eq!(status["node"], "a");
    assert_eq!(status["entries"], 1);
    assert_eq!(status["online"], false);
}

#[test]
fn values_from_stdin() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _daemon = start_daemon(dir, "a", None);
    let mut child = kharka(dir)
        .arg("--socket")
        .arg(socket(dir, "a"))
        .args(["put", "k", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(br#"{"from": "stdin"}"#).unwrap();
    assert!(child.wait().unwrap().success());
    assert_eq!(get(dir, "a", "k").as_deref(), Some(r#"{"from":"stdin"}"#));
}

#[test]
fn watch_streams_the_changes() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _daemon = start_daemon(dir, "a", None);
    put(dir, "a", "status/a", r#"{"agents":1}"#);

    let mut watch = kharka(dir)
        .arg("--socket")
        .arg(socket(dir, "a"))
        .args(["watch", "status/", "--initial"])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(watch.stdout.take().unwrap()).lines();
    let mut next = || serde_json::from_str::<Value>(&lines.next().unwrap().unwrap()).unwrap();

    let first = next();
    assert_eq!(
        (first["event"].as_str(), first["entry"]["key"].as_str()),
        (Some("change"), Some("status/a"))
    );
    assert_eq!(next()["event"], "ready");
    put(dir, "a", "topics/1", "1");
    put(dir, "a", "status/a", r#"{"agents":2}"#);
    let change = next();
    assert_eq!(
        change["entry"]["value"],
        json!({ "agents": 2 }),
        "only changes under the prefix"
    );
    assert!(run(dir, "a", &["del", "status/a"]).status.success());
    assert_eq!(next()["entry"]["deleted"], true);
    watch.kill().unwrap();
    watch.wait().unwrap();
}

#[test]
fn the_socket_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _daemon = start_daemon(dir, "a", None);
    let stream = UnixStream::connect(socket(dir, "a")).unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut lines = BufReader::new(stream).lines();
    let mut ask = |line: &str| {
        writeln!(writer, "{line}").unwrap();
        serde_json::from_str::<Value>(&lines.next().unwrap().unwrap()).unwrap()
    };

    let garbage = ask("garbage");
    assert_eq!(
        (garbage["ok"].as_bool(), garbage["code"].as_str()),
        (Some(false), Some("invalid"))
    );
    let unknown = ask(r#"{"id":"u","op":"nope"}"#);
    assert_eq!(
        (unknown["id"].as_str(), unknown["ok"].as_bool()),
        (Some("u"), Some(false))
    );
    let missing = ask(r#"{"id":1,"op":"put","key":"k"}"#);
    assert!(missing["error"].as_str().unwrap().contains("value"));
    let written = ask(r#"{"id":2,"op":"put","key":"k","value":null}"#);
    assert_eq!(written["ok"], true);
    assert_eq!(ask(r#"{"id":3,"op":"get","key":"k"}"#)["entry"]["value"], Value::Null);
    assert_eq!(ask(r#"{"id":4,"op":"get","key":"nothing"}"#)["entry"], Value::Null);
    let conflict = ask(r#"{"id":5,"op":"put","key":"k","value":1,"if_hlc":"1-0-a"}"#);
    assert_eq!(conflict["code"], "conflict");
    assert_eq!(conflict["current"], written["entry"]["hlc"]);
    assert_eq!(ask(r#"{"id":6,"op":"whoami"}"#)["node"], "a");
    assert_eq!(ask(r#"{"id":7,"op":"unwatch","watch":"none"}"#)["ok"], false);
}

#[test]
fn watch_and_write_on_one_connection() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _daemon = start_daemon(dir, "a", None);
    let stream = UnixStream::connect(socket(dir, "a")).unwrap();
    let mut writer = stream.try_clone().unwrap();
    let mut lines = BufReader::new(stream).lines();
    writeln!(writer, r#"{{"id":"w","op":"watch","prefix":"k"}}"#).unwrap();
    writeln!(writer, r#"{{"id":1,"op":"put","key":"k","value":2}}"#).unwrap();
    let messages: Vec<Value> = (0..3)
        .map(|_| serde_json::from_str(&lines.next().unwrap().unwrap()).unwrap())
        .collect();
    assert_eq!(messages[0]["watch"], "k");
    let answer = messages.iter().find(|message| message["id"] == 1).unwrap();
    let event = messages.iter().find(|message| message["event"] == "change").unwrap();
    assert_eq!(event["entry"]["hlc"], answer["entry"]["hlc"]);
    writeln!(writer, r#"{{"id":2,"op":"unwatch","watch":"w"}}"#).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&lines.next().unwrap().unwrap()).unwrap()["ok"],
        true
    );
}

#[test]
fn writes_survive_a_kill() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let daemon = start_daemon(dir, "a", None);
    for n in 0..50 {
        put(dir, "a", &format!("k/{n}"), &n.to_string());
    }
    drop(daemon); // SIGKILL: no shutdown, the socket stays behind
    let _daemon = start_daemon(dir, "a", None);
    assert_eq!(get(dir, "a", "k/49").as_deref(), Some("49"));
    assert_eq!(status(dir, "a")["entries"], 50);
}

#[test]
fn one_daemon_per_socket_and_folder() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let _daemon = start_daemon(dir, "a", None);
    let second = kharka(dir)
        .args(["daemon", "--node", "a", "--data"])
        .arg(dir.join("a"))
        .arg("--socket")
        .arg(socket(dir, "a"))
        .output()
        .unwrap();
    assert!(!second.status.success());
    let other_socket = kharka(dir)
        .args(["daemon", "--node", "a", "--data"])
        .arg(dir.join("a"))
        .arg("--socket")
        .arg(dir.join("x.sock"))
        .output()
        .unwrap();
    assert!(
        stderr(&other_socket).contains("another kharka process uses"),
        "{}",
        stderr(&other_socket)
    );
}

#[test]
fn collections_from_the_local_config() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    std::fs::write(
        dir.join("a.toml"),
        r#"
        strict = true
        [collections.status]
        kind = "state"
        ttl = "1ms"
        [collections.topics]
        schema = { type = "object", required = ["title"], properties = { title = { type = "string" } } }
        "#,
    )
    .unwrap();
    let _daemon = start_daemon(dir, "a", None);
    put(dir, "a", "topics/1", r#"{"title":"x"}"#);
    let invalid = run(dir, "a", &["put", "topics/2", r#"{"title":3}"#]);
    assert!(
        stderr(&invalid).contains("/title: number instead of string"),
        "{}",
        stderr(&invalid)
    );
    let foreign = run(dir, "a", &["put", "status/b", "{}"]);
    assert!(
        stderr(&foreign).contains("each node writes only status/a/"),
        "{}",
        stderr(&foreign)
    );
    let unknown = run(dir, "a", &["put", "notes/1", "{}"]);
    assert!(stderr(&unknown).contains("(strict)"), "{}", stderr(&unknown));
    // the file's collections are protected, own ones can be added
    let protected = run(dir, "a", &["collections", "set", "topics"]);
    assert!(stderr(&protected).contains("is protected"), "{}", stderr(&protected));
    assert!(run(dir, "a", &["collections", "set", "notes"]).status.success());
    put(dir, "a", "notes/1", "{}");
    put(dir, "a", "status/a", "{}");
    std::thread::sleep(std::time::Duration::from_millis(5));
    let entry: Value = serde_json::from_str(&stdout(&run(dir, "a", &["get", "status/a", "--entry"]))).unwrap();
    assert_eq!(entry["stale"], true, "past its TTL");
    assert_eq!(status(dir, "a")["collections"], json!(["notes", "status", "topics"]));
}
