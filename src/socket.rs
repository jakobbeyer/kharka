//! Unix sockets that only their owner can reach.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use anyhow::{Context, bail};
use tokio::net::UnixListener;

use crate::storage::private_dir;

/// Listens on `path`, in a folder only the owner can enter. A socket left behind by a process that is gone is replaced;
/// one that still answers is not.
pub fn bind(path: &Path) -> anyhow::Result<UnixListener> {
    if let Some(dir) = path.parent() {
        private_dir(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    if fs::symlink_metadata(path).is_ok() {
        if std::os::unix::net::UnixStream::connect(path).is_ok() {
            bail!("another kharka process listens on {}", path.display());
        }
        fs::remove_file(path).with_context(|| format!("removing the old socket {}", path.display()))?;
    }
    let listener = UnixListener::bind(path).with_context(|| format!("listening on {}", path.display()))?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}
