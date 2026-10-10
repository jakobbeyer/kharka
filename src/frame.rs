//! The frames between a daemon and the hub: a 4-byte big-endian length, then a JSON object.

use std::borrow::Borrow;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::entry::Entry;
use crate::hlc::Hlc;

/// Raised whenever a frame changes in a way an older peer cannot read.
pub const PROTOCOL: u32 = 1;
/// The largest frame, in bytes.
pub const MAX_FRAME: usize = 8 << 20;
/// Entries are sent in frames of about this many bytes.
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
    /// The frame's `type`, for logs and errors.
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

/// The length-delimited codec both sides frame their streams with.
pub fn codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder().max_frame_length(MAX_FRAME).new_codec()
}

/// Writes one frame.
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

/// Entries as `changes` or `push` frames of about `BATCH_BYTES` each (an entry larger than that in one of its own), every
/// entry serialised once: the same frames as `Frame::Changes` and `Frame::Push`, without measuring or copying the entries
/// first.
pub struct Entries {
    head: &'static [u8],
    open: Vec<u8>,
    full: Vec<Bytes>,
}

impl Entries {
    pub fn changes() -> Entries {
        Entries::with_head(br#"{"type":"changes","entries":["#)
    }

    pub fn push(restore: bool) -> Entries {
        Entries::with_head(if restore {
            br#"{"type":"push","restore":true,"entries":["#
        } else {
            br#"{"type":"push","entries":["#
        })
    }

    fn with_head(head: &'static [u8]) -> Entries {
        Entries {
            head,
            open: Vec::new(),
            full: Vec::new(),
        }
    }

    /// Adds an entry; whether a frame is full now (time to send). The entry is serialised into the open frame; only when
    /// that makes the frame too large are its bytes moved to a new one.
    pub fn add(&mut self, entry: &Entry) -> anyhow::Result<bool> {
        let before = self.open.len();
        if before == 0 {
            self.open.extend_from_slice(self.head);
        } else {
            self.open.push(b',');
        }
        let at = self.open.len();
        if let Err(err) = serde_json::to_writer(&mut self.open, entry) {
            self.open.truncate(before);
            return Err(err.into());
        }
        if before > 0 && before + (self.open.len() - at) > BATCH_BYTES {
            let mut next = Vec::with_capacity(self.head.len() + self.open.len() - at);
            next.extend_from_slice(self.head);
            next.extend_from_slice(&self.open[at..]);
            self.open.truncate(before);
            self.close();
            self.open = next;
        }
        Ok(!self.full.is_empty())
    }

    fn close(&mut self) {
        if !self.open.is_empty() {
            self.open.extend_from_slice(b"]}");
            self.full.push(Bytes::from(std::mem::take(&mut self.open)));
        }
    }

    /// The frames made so far, read back.
    #[cfg(test)]
    pub(crate) fn read_back(&mut self) -> Vec<Frame> {
        self.close();
        self.full
            .drain(..)
            .map(|frame| serde_json::from_slice(&frame).unwrap())
            .collect()
    }

    /// Writes every frame made so far, with one flush; nothing when there are none.
    pub async fn send<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut FramedWrite<W, LengthDelimitedCodec>,
    ) -> anyhow::Result<()> {
        self.close();
        self.send_full(writer).await
    }

    /// Writes the full frames, and keeps the open one for more entries.
    async fn send_full<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut FramedWrite<W, LengthDelimitedCodec>,
    ) -> anyhow::Result<()> {
        if self.full.is_empty() {
            return Ok(());
        }
        for frame in self.full.drain(..) {
            writer.feed(frame).await?;
        }
        SinkExt::<Bytes>::flush(writer).await?;
        Ok(())
    }
}

/// Writes entries as `frames` (`Entries::changes` or `Entries::push`), each frame as soon as it is full.
pub async fn send_entries<W: AsyncWrite + Unpin, E: Borrow<Entry>>(
    writer: &mut FramedWrite<W, LengthDelimitedCodec>,
    mut frames: Entries,
    entries: &[E],
) -> anyhow::Result<()> {
    for entry in entries {
        if frames.add(entry.borrow())? {
            frames.send_full(writer).await?;
        }
    }
    frames.send(writer).await
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

    /// The frames `send_entries` writes, read back.
    async fn written(frames: Entries, entries: &[Entry]) -> Vec<Frame> {
        let mut writer = FramedWrite::new(Vec::new(), codec());
        send_entries(&mut writer, frames, entries).await.unwrap();
        let bytes = writer.into_inner();
        let mut reader = FramedRead::new(&bytes[..], codec());
        let mut read = Vec::new();
        while let Some(frame) = recv(&mut reader).await.unwrap() {
            read.push(frame);
        }
        read
    }

    #[tokio::test]
    async fn entries_go_in_frames_of_about_a_megabyte() {
        let big = json!("x".repeat(400_000));
        let mut entries: Vec<Entry> = (0..7)
            .map(|n| Entry::new(format!("k{n}"), Some(big.clone()), "1-0-n".parse().unwrap()))
            .collect();
        entries[3].seq = 9;
        entries.push(Entry::new("gone", None, "2-0-n".parse().unwrap()));
        let changes = written(Entries::changes(), &entries).await;
        let expected: Vec<Frame> = entries
            .chunks(2)
            .map(|entries| Frame::Changes {
                entries: entries.to_vec(),
            })
            .collect();
        assert_eq!(changes.len(), 4);
        assert_eq!(changes, expected);

        // one larger than a frame goes in one of its own
        let huge = Entry::new("huge", Some(json!("x".repeat(1_200_000))), "3-0-n".parse().unwrap());
        let mixed = [entries[7].clone(), huge, entries[7].clone()];
        assert_eq!(
            written(Entries::changes(), &mixed).await,
            mixed
                .iter()
                .map(|entry| Frame::Changes {
                    entries: vec![entry.clone()]
                })
                .collect::<Vec<_>>()
        );

        let small = &entries[7..];
        for restore in [false, true] {
            assert_eq!(
                written(Entries::push(restore), small).await,
                [Frame::Push {
                    entries: small.to_vec(),
                    restore
                }]
            );
        }
        assert!(written(Entries::changes(), &[]).await.is_empty());
    }
}
