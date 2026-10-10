//! Unix sockets that only their owner can reach.

use std::fs;
use std::io;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use tokio::net::UnixListener;

use crate::paths::runtime_dir;
use crate::storage::private_dir;

/// Listens on `path`, a socket only the owner can use. kharka's own folder, and any folder it creates, is made one only
/// the owner can enter; a folder that was there before (`--socket ./x.sock`, `/tmp/x.sock`) keeps its permissions, and
/// the socket is made in a private folder next to `path` and linked into place, so others never get to connect to it. A
/// socket left behind by a process that is gone is replaced; one that still answers is not.
pub fn bind(path: &Path) -> anyhow::Result<UnixListener> {
    // no folder in the path (`x.sock`): the working folder, which is not kharka's
    let dir = path.parent().unwrap_or(Path::new(""));
    let private = !dir.as_os_str().is_empty() && (dir == runtime_dir() || !dir.exists());
    if private {
        private_dir(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    if fs::symlink_metadata(path).is_ok() {
        if std::os::unix::net::UnixStream::connect(path).is_ok() {
            bail!("another kharka process listens on {}", path.display());
        }
        fs::remove_file(path).with_context(|| format!("removing the old socket {}", path.display()))?;
    }
    if !private {
        return bind_staged(path);
    }
    let listener = UnixListener::bind(path).with_context(|| format!("listening on {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

/// Binds in a folder of its own next to `path` (a fresh name, created with mode 0700, never one that was there before),
/// makes the socket 0600 there and links it to `path`: the listener keeps working, nobody else could connect on the
/// way, and a socket another process put at `path` meanwhile is not replaced (a link, unlike a rename, fails then).
fn bind_staged(path: &Path) -> anyhow::Result<UnixListener> {
    let staging = staging_dir(path)?;
    let listener = (|| {
        // a short name: socket paths have a length limit
        let staged = staging.join("s");
        let listener = UnixListener::bind(&staged).with_context(|| format!("listening on {}", staged.display()))?;
        fs::set_permissions(&staged, fs::Permissions::from_mode(0o600))?;
        fs::hard_link(&staged, path).map_err(|error| match error.kind() {
            io::ErrorKind::AlreadyExists => anyhow!("another kharka process listens on {}", path.display()),
            _ => anyhow!(error).context(format!("linking the socket to {}", path.display())),
        })?;
        Ok(listener)
    })();
    let _ = fs::remove_dir_all(&staging);
    listener
}

/// A new folder `.k<hex>` next to `path`, mode 0700. Its name is not known in advance, and it is created, not taken: a
/// folder or link someone else put there is never used.
fn staging_dir(path: &Path) -> anyhow::Result<PathBuf> {
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos()) as u64;
    for attempt in 0..16u64 {
        let tag = (seed ^ (u64::from(process::id()) << 32)).wrapping_add(attempt.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let staging = path.with_file_name(format!(".k{:08x}", tag as u32));
        match fs::DirBuilder::new().mode(0o700).create(&staging) {
            Ok(()) => return Ok(staging),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).with_context(|| format!("creating {}", staging.display())),
        }
    }
    bail!("no free name for a staging folder next to {}", path.display())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_staged_socket_does_not_replace_what_another_process_put_there() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.sock");
        fs::write(&path, "someone else's").unwrap();
        let refused = bind_staged(&path).unwrap_err();
        assert!(
            refused.to_string().contains("another kharka process listens"),
            "{refused}"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "someone else's");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "no staging folder left");
    }

    #[test]
    fn a_staging_folder_is_always_a_new_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.sock");
        let first = staging_dir(&path).unwrap();
        let second = staging_dir(&path).unwrap();
        assert_ne!(first, second);
        assert_eq!(fs::metadata(&first).unwrap().permissions().mode() & 0o777, 0o700);
    }
}
