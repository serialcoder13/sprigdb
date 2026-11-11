//! Start-time checks on the files that hold secrets (hardening round 2):
//! bearer and service tokens, the workload master keys, the backends file,
//! TLS private keys.
//!
//! A secret file must be a regular file (a symbolic link to one is fine,
//! as Kubernetes mounts secrets) and must not be writable by everyone:
//! anyone who can rewrite a token file can let themselves in. Such a file
//! is refused with 39998, naming it and its mode. A group-writable file is
//! allowed with a warning, because a per-user group (Debian's and Ubuntu's
//! default, `umask 002`) makes that common and harmless. Readable by
//! others is fine: the Compose bundle's files are 0644 inside a 0700
//! directory, because each container reads them as its own user.
//!
//! On other systems than Unix only the regular-file check applies.

use std::path::Path;

use crate::{Result, SprigCode, SprigError};

/// Checks `path` (see the module notes); `what` names the setting, such as
/// `[controller] admin_token_file`.
///
/// # Errors
/// 39998 for a file that is missing, not a regular file, or writable by
/// everyone.
pub fn check(path: &Path, what: &str) -> Result<()> {
    let md = std::fs::metadata(path).map_err(|e| {
        SprigError::new(
            SprigCode::ConfigInvalid,
            format!("{what} {}: {e}", path.display()),
        )
    })?;
    if !md.is_file() {
        return Err(SprigError::new(
            SprigCode::ConfigInvalid,
            format!("{what} {} isn't a regular file", path.display()),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = md.permissions().mode() & 0o7777;
        if mode & 0o002 != 0 {
            return Err(SprigError::new(
                SprigCode::ConfigInvalid,
                format!(
                    "{what} {} is writable by everyone (mode {mode:04o}): it holds a secret; \
                     chmod o-w it (0644 or 0600)",
                    path.display()
                ),
            ));
        }
        if mode & 0o020 != 0 {
            tracing::warn!(path = %path.display(), mode = format!("{mode:04o}"),
                "{what} is group-writable; 0644 or 0600 is enough");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
