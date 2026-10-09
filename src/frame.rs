//! The frames between a daemon and the hub: a 4-byte big-endian length, then a JSON object.

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::entry::Entry;
use crate::hlc::Hlc;

/// Raised whenever a frame changes in a way an older peer cannot read.
pub const PROTOCOL: u32 = 1;
pub const MAX_FRAME: usize = 8 << 20;
/// Entries are sent in batches of about this many bytes.
const BATCH_BYTES: usize = 1 << 20;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// daemon → hub, first: who it is, which hub it last synced with, the last sequence number it has, and the key
    /// prefixes it syncs (none: everything).
    Hello {
        version: u32,
        node: String,
        #[serde(default)]
        epoch: Option<String>,
        since: u64,
        #[serde(default)]
        prefixes: Vec<String>,
    },
    /// hub → daemon, first. `reset`: the daemon's sequence numbers mean nothing here (another hub, or the hub lost its
    /// data); everything follows from the start and the daemon pushes all it has.
    Welcome {
        epoch: String,
        reset: bool,
    },
    /// hub → daemon: entries, in sequence order. Also the answer to a push that did not change the hub (the entry the hub
    /// holds for that key).
    Changes {
        entries: Vec<Entry>,
    },
    /// hub → daemon: the catch-up is complete, live changes follow.
    CaughtUp {
        seq: u64,
    },
    /// daemon → hub: local writes. `restore`: entries of other nodes, which a daemon pushes after a reset, so that a hub
    /// that lost its data (or a new hub) gets them back; the hub keeps them as they are, without its usual checks.
    Push {
        entries: Vec<Entry>,
        #[serde(default, skip_serializing_if = "is_false")]
        restore: bool,
    },
    /// hub → daemon: a pushed entry was not taken (it is kept under `_rejected/`). `restated`: the hub replaced it with
    /// its own version, a later stamp; otherwise the daemon stops pushing it.
    Rejected {
        key: String,
        hlc: Hlc,
        reason: String,
        #[serde(default)]
        restated: bool,
    },
    /// Either way, every 15 s: the connection is alive. A ping is answered with a pong.
    Ping,
    Pong,
    /// hub → daemon, instead of a welcome: the connection is not taken.
    Refused {
        reason: String,
    },
}

impl Frame {
    pub fn name(&self) -> &'static str {
        match self {
            Frame::Hello { .. } => "hello",
            Frame::Welcome { .. } => "welcome",
            Frame::Changes { .. } => "changes",
            Frame::CaughtUp { .. } => "caught_up",
            Frame::Push { .. } => "push",
            Frame::Rejected { .. } => "rejected",
            Frame::Refused { .. } => "refused",
            Frame::Ping => "ping",
            Frame::Pong => "pong",
        }
    }
}

pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder().max_frame_length(MAX_FRAME).new_codec()
}

pub async fn send<W: AsyncWrite + Unpin>(
    writer: &mut FramedWrite<W, LengthDelimitedCodec>,
    frame: &Frame,
) -> anyhow::Result<()> {
    writer.send(Bytes::from(serde_json::to_vec(frame)?)).await?;
    Ok(())
}

/// The next frame, or `None` when the other side closed the connection.
pub async fn recv<R: AsyncRead + Unpin>(
    reader: &mut FramedRead<R, LengthDelimitedCodec>,
) -> anyhow::Result<Option<Frame>> {
    match reader.next().await {
        None => Ok(None),
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes?)?)),
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Splits entries into batches that each fit a frame easily.
pub fn batches(entries: Vec<Entry>) -> Vec<Vec<Entry>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut size = 0;
    for entry in entries {
        let entry_size = entry.key.len() + entry.value.to_string().len() + 100;
        if !batch.is_empty() && size + entry_size > BATCH_BYTES {
            batches.push(std::mem::take(&mut batch));
            size = 0;
        }
        size += entry_size;
        batch.push(entry);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn frames_are_tagged_json() {
        let frame = Frame::CaughtUp { seq: 3 };
        assert_eq!(
            serde_json::to_value(&frame).unwrap(),
            json!({ "type": "caught_up", "seq": 3 })
        );
        let hello: Frame =
            serde_json::from_value(json!({ "type": "hello", "version": 1, "node": "mac", "since": 0 })).unwrap();
        assert_eq!(
            hello,
            Frame::Hello {
                version: 1,
                node: "mac".into(),
                epoch: None,
                since: 0,
                prefixes: vec![]
            }
        );
    }

    #[test]
    fn batches_stay_small() {
        let big = json!("x".repeat(400_000));
        let entries: Vec<Entry> = (0..7)
            .map(|n| Entry::new(format!("k{n}"), Some(big.clone()), "1-0-n".parse().unwrap()))
            .collect();
        let batches = batches(entries);
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2, 2, 1]);
        assert!(super::batches(vec![]).is_empty());
    }
}
