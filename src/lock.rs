//! File-based locking to prevent concurrent sync operations.
//!
//! Uses `flock` (via fs2) to ensure only one sync runs at a time.

use anyhow::{Context, Result};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use crate::config::ConfigManager;

/// A guard that holds an exclusive lock on the sync lock file.
/// The lock is released when this guard is dropped.
pub struct SyncLock {
    _file: File,
    path: PathBuf,
}

impl SyncLock {
    /// Attempt to acquire an exclusive lock for sync operations.
    ///
    /// Returns `Ok(SyncLock)` if the lock was acquired, or an error if
    /// another sync is already running.
    pub fn acquire() -> Result<Self> {
        let lock_path = Self::lock_path()?;

        // Ensure parent directory exists
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create lock directory: {}", parent.display()))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&lock_path)
            .with_context(|| format!("Failed to open lock file: {}", lock_path.display()))?;

        // Try to acquire exclusive lock (non-blocking)
        match file.try_lock_exclusive() {
            Ok(()) => {
                log::debug!("Acquired sync lock: {}", lock_path.display());
                Ok(Self {
                    _file: file,
                    path: lock_path,
                })
            }
            Err(e) => {
                Err(anyhow::anyhow!(
                    "Another sync operation is already running. \
                     If you're sure no other sync is running, delete the lock file: {}\n\
                     Original error: {}",
                    lock_path.display(),
                    e
                ))
            }
        }
    }

    fn lock_path() -> Result<PathBuf> {
        let config_dir = ConfigManager::ensure_config_dir()?;
        Ok(config_dir.join("sync.lock"))
    }
}

impl Drop for SyncLock {
    fn drop(&mut self) {
        log::debug!("Releasing sync lock: {}", self.path.display());
        // File lock is automatically released when the file is closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::file_serial;
    use std::env;
    use tempfile::TempDir;

    #[test]
    #[file_serial]
    fn test_lock_acquire_and_release() {
        let temp_dir = TempDir::new().unwrap();
        let original_home = env::var("HOME").ok();
        env::set_var("HOME", temp_dir.path());

        // First lock should succeed
        let lock1 = SyncLock::acquire().unwrap();

        // Second lock should fail
        let lock2_result = SyncLock::acquire();
        match lock2_result {
            Err(e) => assert!(e.to_string().contains("already running")),
            Ok(_) => panic!("Expected lock acquisition to fail"),
        }

        // Drop first lock
        drop(lock1);

        // Now we can acquire again
        let _lock3 = SyncLock::acquire().unwrap();

        // Restore HOME
        if let Some(home) = original_home {
            env::set_var("HOME", home);
        } else {
            env::remove_var("HOME");
        }
    }
}
