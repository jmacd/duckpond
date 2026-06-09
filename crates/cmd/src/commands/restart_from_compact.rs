// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond restart-from-compact <name>` (D6.4) -- recover a consumer that
//! has fallen below a remote's retention horizon by re-bootstrapping
//! from the remote's oldest compact bundle.
//!
//! When `pond pull` fails with "consumer is below retention horizon"
//! ([`sync_remote::RemoteError::BehindRetention`]), the bundles the
//! consumer still needs have been pruned by `pond maintain` retention.
//! The only recovery is to drop the affected pond's local footprint and
//! re-apply the remote's oldest surviving compact baseline, then catch
//! up to the latest bundle.
//!
//! This delegates to the generic
//! [`sync_remote::Remote::restart_pond_from_compact`], which operates
//! IN PLACE on the open consumer via duckpond's `ShipRemoteSteward`:
//!
//! * **Mirror** (`remote.store_id == local pond_id`): drops all of the
//!   consumer's own data and lifecycle records, then rebuilds from the
//!   compact baseline.  Because the `/sys/remotes/<name>` attachment and
//!   its mode/mount settings live under the local pond_id, they are
//!   dropped too -- so this command re-persists them afterwards so the
//!   remote stays usable for future pulls.
//! * **Cross-pond import** (`remote.store_id != local pond_id`): drops
//!   only the foreign pond's footprint on the consumer; the consumer's
//!   own data (including the attachment) and any sibling imports are
//!   untouched, so no re-persist is needed.

use crate::commands::remote::{load_remote_attachment, remote_config_path};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use steward::{
    PondUserMetadata, REMOTE_MODE_PREFIX, REMOTE_MOUNT_PATH_PREFIX, SYS_DIR, SYS_REMOTES_DIR,
    ShipRemoteSteward,
};
use sync_remote::Remote;
use tinyfs::EntryType;
use tokio::io::AsyncWriteExt;

/// Re-bootstrap the consumer for remote `name` from its oldest compact
/// bundle.  This is a recovery operation; it discards the affected
/// pond's local data before re-applying the baseline.
pub async fn restart_from_compact_command(ship_context: &ShipContext, name: String) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let attachment = load_remote_attachment(&mut ship, &name).await?;

    if attachment.url.starts_with("s3://") {
        sync_remote::register_s3_handlers();
    }

    let storage_options = attachment.to_storage_options()?;
    let remote = Remote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let foreign_pond_id = remote.store_id();
    let local_pond_id = ship
        .as_pond()
        .ok_or_else(|| {
            anyhow!("restart-from-compact requires a pond steward (not a host steward)")
        })?
        .control_table()
        .pond_id_uuid();
    let is_mirror = local_pond_id == foreign_pond_id;

    // For a mirror restart, the attachment (YAML + mode + mount) lives
    // under the local pond_id and will be dropped by the restart.
    // Capture it now so we can re-persist it afterwards.
    let preserved = if is_mirror {
        Some(capture_attachment(&mut ship, &name).await?)
    } else {
        None
    };

    if is_mirror {
        log::warn!(
            "[restart] mirror restart of `{}`: ALL local data for pond {} will be \
             dropped and rebuilt from the remote's compact baseline",
            name,
            local_pond_id
        );
    } else {
        log::info!(
            "[restart] cross-pond restart of `{}`: dropping only foreign pond {}'s \
             footprint and rebuilding from its compact baseline",
            name,
            foreign_pond_id
        );
    }

    {
        let ship_ref = ship.as_pond_mut().ok_or_else(|| {
            anyhow!("restart-from-compact requires a pond steward (not a host steward)")
        })?;
        let mut adapter = ShipRemoteSteward::new(ship_ref);
        remote
            .restart_pond_from_compact(&mut adapter)
            .await
            .map_err(|e| match e {
                sync_remote::RemoteError::NoRestartPoint => anyhow!(
                    "remote `{}` has no compact bundle to restart from; run `pond maintain` \
                     on the producer to create one (or the producer has never compacted)",
                    name
                ),
                other => anyhow!("restart-from-compact `{}`: {}", name, other),
            })?;
    }

    // Re-persist the attachment dropped by a mirror restart.
    if let Some(saved) = preserved {
        restore_attachment(&mut ship, &name, &saved).await?;
        log::info!("[restart] re-attached `{}` after mirror restart", name);
    }

    log::info!(
        "[OK] restart-from-compact {}: rebuilt from compact baseline and caught up to latest",
        name
    );

    Ok(())
}

/// The attachment state that a mirror restart would drop.
struct SavedAttachment {
    yaml: Vec<u8>,
    mode: Option<String>,
    mount: Option<String>,
}

/// Read the raw `/sys/remotes/<name>` YAML plus the `remote_mode:` and
/// `remote_mount_path:` settings before a mirror restart drops them.
async fn capture_attachment(ship: &mut steward::Steward, name: &str) -> Result<SavedAttachment> {
    let config_path = remote_config_path(name);
    let mode = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MODE_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("read remote mode for `{}`: {}", name, e))?;
    let mount = ship
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("read remote mount for `{}`: {}", name, e))?;

    let tx = ship
        .begin_read(&PondUserMetadata::new(vec![
            "restart".to_string(),
            "capture".to_string(),
            name.to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("begin read to capture attachment: {}", e))?;
    let yaml = {
        let fs = &*tx;
        let root = fs.root().await?;
        root.read_file_path_to_vec(&config_path).await?
    };
    let _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("commit read: {}", e))?;

    Ok(SavedAttachment { yaml, mode, mount })
}

/// Re-persist the attachment captured by [`capture_attachment`] after a
/// mirror restart has dropped it.  Restores the `remote_mode:` /
/// `remote_mount_path:` settings and rewrites the `/sys/remotes/<name>`
/// YAML in a fresh write transaction.
async fn restore_attachment(
    ship: &mut steward::Steward,
    name: &str,
    saved: &SavedAttachment,
) -> Result<()> {
    if let Some(mode) = &saved.mode {
        ship.control_table_mut()
            .raw_config_set(&format!("{REMOTE_MODE_PREFIX}{name}"), mode)
            .await
            .map_err(|e| anyhow!("restore remote mode for `{}`: {}", name, e))?;
    }
    if let Some(mount) = &saved.mount {
        ship.control_table_mut()
            .raw_config_set(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"), mount)
            .await
            .map_err(|e| anyhow!("restore remote mount for `{}`: {}", name, e))?;
    }

    let config_path = remote_config_path(name);
    let yaml = saved.yaml.clone();
    let name_owned = name.to_string();
    ship.write_transaction(
        &PondUserMetadata::new(vec![
            "restart".to_string(),
            "reattach".to_string(),
            name_owned.clone(),
        ]),
        async move |fs| {
            let root = fs.root().await?;
            let _ = root.create_dir_all(SYS_DIR).await?;
            let _ = root.create_dir_all(SYS_REMOTES_DIR).await?;

            if root.exists(&config_path).await {
                let remotes_dir = root.open_dir_path(SYS_REMOTES_DIR).await?;
                remotes_dir.remove_entry(&name_owned).await.map_err(|e| {
                    steward::StewardError::Aborted(format!(
                        "remove stale remote config {}: {}",
                        config_path, e
                    ))
                })?;
            }

            let mut writer = root
                .async_writer_path_with_type(&config_path, EntryType::FilePhysicalVersion)
                .await?;
            writer
                .write_all(&yaml)
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("write yaml: {}", e)))?;
            writer
                .shutdown()
                .await
                .map_err(|e| steward::StewardError::Aborted(format!("close yaml: {}", e)))?;
            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow!("re-persist attachment `{}`: {}", name, e))?;

    Ok(())
}
