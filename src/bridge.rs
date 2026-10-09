//! `kharka bridge`: stdin and stdout become a connection to the hub's socket, so that `ssh <hub> kharka bridge` is a
//! daemon's way in. ssh decides who may connect; the hub opens no port.

use std::path::PathBuf;

use anyhow::bail;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// Connects to the first of `sockets` where a hub listens.
pub async fn run(sockets: &[PathBuf]) -> anyhow::Result<()> {
    let mut stream = None;
    for socket in sockets {
        if let Ok(connected) = UnixStream::connect(socket).await {
            stream = Some(connected);
            break;
        }
    }
    let Some(stream) = stream else {
        let tried: Vec<String> = sockets.iter().map(|socket| socket.display().to_string()).collect();
        bail!("no hub listens on {}", tried.join(" or "));
    };
    let (mut from_hub, mut to_hub) = stream.into_split();
    let up = async {
        tokio::io::copy(&mut tokio::io::stdin(), &mut to_hub).await?;
        to_hub.shutdown().await
    };
    // flushed after every read: stdout would otherwise hold a frame back until more arrives
    let down = async {
        let mut stdout = tokio::io::stdout();
        let mut buf = vec![0; 64 * 1024];
        loop {
            let read = from_hub.read(&mut buf).await?;
            if read == 0 {
                return Ok::<_, std::io::Error>(());
            }
            stdout.write_all(&buf[..read]).await?;
            stdout.flush().await?;
        }
    };
    tokio::select! {
        result = up => result?,
        result = down => result?,
    }
    Ok(())
}
