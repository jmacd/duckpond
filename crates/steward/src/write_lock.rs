// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Process-level write exclusion for a single pond.
//!
//! `WriteLockGuard` wraps an OS advisory file lock (via [`fs2::FileExt`])
//! on `{control_dir}/write.lock`.  At most one process can hold the
//! exclusive lock at a time; concurrent attempts return
//! [`StewardError::PondLocked`] with the holder's PID, start time, and
//! txn_id parsed from the lockfile body.
//!
//! The lock is released automatically when the file descriptor closes,
//! which happens on normal `Drop`, panic unwind, and process death
//! (including `kill -9`).  We never explicitly unlock; the lockfile
//! itself persists with stale contents and is truncated/rewritten by
//! the next acquirer.
//!
//! Reads are intentionally not locked.  See `docs/d5.7-resume.md` for
//! the design discussion.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use tlogfs::PondTxnMetadata;

use crate::StewardError;

/// RAII handle that holds an exclusive advisory lock on `write.lock`
/// for the lifetime of a write transaction.  Dropping the guard
/// closes the underlying file descriptor, which releases the lock.
#[derive(Debug)]
pub(crate) struct WriteLockGuard {
    // Held until drop to keep the FD (and thus the kernel lock) open.
    #[allow(dead_code)]
    file: File,
    path: PathBuf,
}

impl WriteLockGuard {
    /// Attempt to acquire the write lock for `control_dir`.
    ///
    /// On success, the lockfile body is replaced with the current
    /// holder's PID, start timestamp, and `txn_id`.
    ///
    /// On conflict, returns [`StewardError::PondLocked`] populated
    /// with whatever holder info could be parsed from the existing
    /// lockfile body (fields are `None` if the file is empty, missing,
    /// or malformed — the lock is still rejected either way).
    pub(crate) fn try_acquire(
        control_dir: &Path,
        txn_meta: &PondTxnMetadata,
    ) -> Result<Self, StewardError> {
        let path = control_dir.join("write.lock");

        // Open without O_TRUNC: if another process holds the lock we
        // must not stomp their body before we discover the conflict.
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => {
                let _pos = file.seek(SeekFrom::Start(0))?;
                file.set_len(0)?;
                file.write_all(format_lock_body(txn_meta).as_bytes())?;
                file.flush()?;
                Ok(Self { file, path })
            }
            Err(err) if is_would_block(&err) => {
                let holder = read_holder_info(&path).unwrap_or_default();
                Err(StewardError::PondLocked {
                    path,
                    holder_pid: holder.pid,
                    holder_since: holder.since,
                    holder_txn_id: holder.txn_id,
                })
            }
            Err(err) => Err(StewardError::Io(err)),
        }
    }

    /// Path to the lockfile (useful for user-facing error messages).
    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for WriteLockGuard {
    fn drop(&mut self) {
        // The kernel releases the lock when `self.file` closes; no
        // explicit unlock_exclusive() needed.  We intentionally leave
        // the lockfile on disk so the next acquirer can discover the
        // last holder's identity via its (now stale) body before
        // overwriting it.  Trying to delete here would race with a
        // concurrent acquirer creating it fresh.
        let _ = &self.path; // suppress unused-field warning
    }
}

/// `WouldBlock` is the kind returned by `fs2` when another process
/// holds the lock.  On some platforms the precise OS error code is
/// `EWOULDBLOCK`/`EAGAIN` which both map to `ErrorKind::WouldBlock`.
fn is_would_block(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::WouldBlock
}

#[derive(Debug, Default, Clone)]
struct HolderInfo {
    pid: Option<u32>,
    since: Option<DateTime<Utc>>,
    txn_id: Option<String>,
}

fn read_holder_info(path: &Path) -> std::io::Result<HolderInfo> {
    let mut s = String::new();
    let _read = File::open(path)?.read_to_string(&mut s)?;
    let mut info = HolderInfo::default();
    for line in s.lines() {
        if let Some((k, v)) = line.split_once('=') {
            match k.trim() {
                "pid" => info.pid = v.trim().parse().ok(),
                "started" => {
                    info.since = DateTime::parse_from_rfc3339(v.trim())
                        .ok()
                        .map(|d| d.with_timezone(&Utc));
                }
                "txn_id" => info.txn_id = Some(v.trim().to_string()),
                _ => {}
            }
        }
    }
    Ok(info)
}

fn format_lock_body(txn_meta: &PondTxnMetadata) -> String {
    format!(
        "pid={}\nstarted={}\ntxn_id={}\n",
        std::process::id(),
        Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        txn_meta.user.txn_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tlogfs::PondUserMetadata;

    fn meta() -> PondTxnMetadata {
        PondTxnMetadata::new(1, PondUserMetadata::new(vec!["test".to_string()]))
    }

    #[test]
    fn acquire_succeeds_when_uncontended() {
        let dir = TempDir::new().expect("tempdir");
        let g =
            WriteLockGuard::try_acquire(dir.path(), &meta()).expect("first acquire should succeed");
        assert!(g.path().ends_with("write.lock"));
        assert!(g.path().exists());
    }

    #[test]
    fn acquire_writes_holder_body() {
        let dir = TempDir::new().expect("tempdir");
        let m = meta();
        let _g = WriteLockGuard::try_acquire(dir.path(), &m).expect("acquire");
        let body = std::fs::read_to_string(dir.path().join("write.lock")).expect("read lockfile");
        assert!(body.contains(&format!("pid={}", std::process::id())));
        assert!(body.contains(&format!("txn_id={}", m.user.txn_id)));
        assert!(body.contains("started="));
    }

    #[test]
    fn second_acquire_in_same_process_fails_with_pondlocked() {
        let dir = TempDir::new().expect("tempdir");
        let _g1 = WriteLockGuard::try_acquire(dir.path(), &meta()).expect("first");
        let err = WriteLockGuard::try_acquire(dir.path(), &meta()).expect_err("second should fail");
        match err {
            StewardError::PondLocked {
                holder_pid,
                holder_txn_id,
                ..
            } => {
                assert_eq!(holder_pid, Some(std::process::id()));
                assert!(holder_txn_id.is_some());
            }
            other => panic!("expected PondLocked, got {other:?}"),
        }
    }

    #[test]
    fn acquire_succeeds_after_previous_guard_dropped() {
        let dir = TempDir::new().expect("tempdir");
        {
            let _g1 = WriteLockGuard::try_acquire(dir.path(), &meta()).expect("first");
            // drops at end of block
        }
        let _g2 = WriteLockGuard::try_acquire(dir.path(), &meta())
            .expect("re-acquire after drop should succeed");
    }
}
