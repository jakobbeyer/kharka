//! A client of the local daemon for the command line and the collectors: blocking, one connection, one request at a
//! time.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use anyhow::{Context, bail};
use serde_json::{Value, json};

/// A connection to the daemon's socket. Requests and answers are JSON lines (docs/protocol.md).
pub struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Client {
    /// Connects to the daemon listening at `socket`.
    pub fn connect(socket: &Path) -> anyhow::Result<Client> {
        let stream = UnixStream::connect(socket).with_context(|| {
            format!(
                "no kharka daemon at {} (start one with `kharka daemon`)",
                socket.display()
            )
        })?;
        Ok(Client {
            reader: BufReader::new(stream.try_clone()?),
            writer: stream,
            next_id: 1,
        })
    }

    /// Sends a request and returns its answer, `ok` true or false. Watch events that arrive meanwhile are skipped.
    pub fn call(&mut self, mut request: Value) -> anyhow::Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        request["id"] = json!(id);
        let mut line = request.to_string();
        line.push('\n');
        self.writer
            .write_all(line.as_bytes())
            .context("writing to the daemon")?;
        loop {
            let message = self.read()?;
            if message["id"] == id && message.get("ok").is_some() {
                return Ok(message);
            }
        }
    }

    /// The next message from the daemon.
    pub fn read(&mut self) -> anyhow::Result<Value> {
        let mut line = String::new();
        if self.reader.read_line(&mut line).context("reading from the daemon")? == 0 {
            bail!("the daemon closed the connection");
        }
        serde_json::from_str(&line).context("the daemon sent something that is not JSON")
    }

    /// This machine's node name.
    pub fn whoami(&mut self) -> anyhow::Result<String> {
        let answer = self.call(json!({ "op": "whoami" }))?;
        answer["node"]
            .as_str()
            .map(str::to_string)
            .context("the daemon did not say its name")
    }
}
