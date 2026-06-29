// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! `pond pull [<name>]` -- pull the content-addressed object graph from one or
//! more remotes.  A root (or absent) mount path mirrors the source into the
//! local pond; a non-root mount path is a cross-pond import that rebuilds the
//! foreign pond's tree under its own pond_id and mounts it at the path.

use crate::commands::remote::{
    RemoteMode, list_remote_names, load_remote_attachment, remote_mode_for,
};
use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use steward::{REMOTE_MOUNT_PATH_PREFIX, StewardError};
use uuid::Uuid;

/// Pull from `name`, or from every remote in `pull`/`both` mode when `name`
/// is `None`.  Each remote is processed independently.
pub async fn pull_command(ship_context: &ShipContext, name: Option<String>) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    let targets: Vec<String> = if let Some(n) = name {
        vec![n]
    } else {
        let all = list_remote_names(&mut ship).await?;
        let mut filtered = Vec::new();
        for n in all {
            match remote_mode_for(&ship, &n).await? {
                RemoteMode::Pull | RemoteMode::Both => filtered.push(n),
                RemoteMode::Push => {
                    log::debug!("skip {}: mode=push", n);
                }
            }
        }
        filtered
    };

    if targets.is_empty() {
        log::info!("no remotes to pull from");
        return Ok(());
    }

    let mut had_error = false;
    for name in targets {
        if let Err(e) = pull_one(&mut ship, &name).await {
            log::error!("[ERR] pull {}: {}", name, e);
            had_error = true;
        }
    }

    if had_error {
        Err(anyhow!("one or more pulls failed"))
    } else {
        Ok(())
    }
}

async fn pull_one(ship: &mut steward::Steward, name: &str) -> Result<()> {
    let attachment = load_remote_attachment(ship, name).await?;

    if attachment.url.starts_with("s3://") {
        sync_remote::register_s3_handlers();
    }

    let ship_pre = ship
        .as_pond()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;
    let mount_path: Option<String> = ship_pre
        .control_table()
        .raw_config_get(&format!("{REMOTE_MOUNT_PATH_PREFIX}{name}"))
        .await
        .map_err(|e| anyhow!("read mount_path for `{}`: {}", name, e))?
        .filter(|s| !s.is_empty() && s != "/");

    // Mirror restart / backup restore (root or no mount): pull the full
    // content graph and rebuild the local pond by node_id.  Cross-pond import
    // (non-root mount): fetch the foreign content graph and rebuild it under
    // the foreign pond_id, then mount it.
    if mount_path.is_none() {
        return pull_mirror(ship, name, &attachment).await;
    }

    let storage_options = attachment.to_storage_options()?;
    let remote = sync_store::ContentRemote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let ship_ref = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;

    let local_pond_id = ship_ref.control_table().pond_id_uuid();
    let foreign_pond_id = remote.pond_id();

    // This is the import path: mount_path is guaranteed non-root here.
    let mount_path = mount_path.expect("import path requires a non-root mount_path");
    if foreign_pond_id == local_pond_id {
        return Err(anyhow!(
            "remote `{}` has mount_path `{}` but its store_id matches this pond's \
             pond_id; cross-pond import requires a foreign store_id",
            name,
            mount_path
        ));
    }

    // Fetch the foreign object graph and rebuild it under the foreign pond_id
    // partition, then mount it.  The local allocator stays contiguous; only the
    // foreign pond's seq frontier advances inside `import_pond`.
    let graph = steward::fetch_object_graph(&remote, "main")
        .await
        .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?;
    if graph.is_empty() {
        log::info!(
            "pull {}: remote ref `main` is empty; nothing to import",
            name
        );
        return Ok(());
    }
    let foreign_uuid7 = uuid7::Uuid::from(*foreign_pond_id.as_bytes());
    let outcome = steward::import_pond(ship_ref, &graph, foreign_uuid7)
        .await
        .map_err(|e| anyhow!("import from `{}`: {}", attachment.url, e))?;
    log::info!(
        "[OK] pull {} complete (cross-pond import: {:?})",
        name,
        outcome
    );

    materialize_mount(ship_ref, name, &mount_path, foreign_pond_id).await?;

    Ok(())
}

/// Mirror restart / backup restore: fetch the remote's full content graph
/// for ref `main` and rebuild the local pond by node_id.  Used when the
/// attachment has no mount path (or `/`).
async fn pull_mirror(
    ship: &mut steward::Steward,
    name: &str,
    attachment: &steward::RemoteAttachment,
) -> Result<()> {
    let storage_options = attachment.to_storage_options()?;
    let remote = sync_store::ContentRemote::open_at_url(&attachment.url, storage_options)
        .await
        .map_err(|e| anyhow!("open remote `{}` ({}): {}", name, attachment.url, e))?;

    let ship_ref = ship
        .as_pond_mut()
        .ok_or_else(|| anyhow!("pull requires a pond steward (not a host steward)"))?;

    let graph = steward::fetch_object_graph(&remote, "main")
        .await
        .map_err(|e| anyhow!("fetch from `{}`: {}", attachment.url, e))?;
    if graph.is_empty() {
        log::info!(
            "pull {}: remote ref `main` is empty; nothing to rebuild",
            name
        );
        return Ok(());
    }
    let outcome = steward::rebuild_pond(ship_ref, &graph)
        .await
        .map_err(|e| anyhow!("rebuild from `{}`: {}", attachment.url, e))?;
    log::info!(
        "[OK] pull {} complete (mirror rebuild: {:?})",
        name,
        outcome
    );
    Ok(())
}

/// Insert (idempotently) a directory entry at `mount_path` whose
/// `pond_id` is the foreign pond's id.  Reading through this entry
/// will yield the foreign pond's tree.
async fn materialize_mount(
    ship: &mut steward::Ship,
    name: &str,
    mount_path: &str,
    foreign_pond_id: Uuid,
) -> Result<()> {
    let (parent, leaf) = split_mount_path(mount_path)?;
    let name_owned = name.to_string();
    let mount_owned = mount_path.to_string();
    let parent_owned = parent.to_string();
    let leaf_owned = leaf.to_string();

    ship.write_transaction(
        &steward::PondUserMetadata::new(vec![
            "pull".to_string(),
            "mount".to_string(),
            name_owned,
            mount_owned,
        ]),
        async move |fs| {
            let root = fs.root().await?;
            let _ = root.create_dir_all(&parent_owned).await?;
            let parent_wd = root.open_dir_path(&parent_owned).await?;

            let foreign_uuid7 = uuid7::Uuid::from(*foreign_pond_id.as_bytes());

            if let Some(existing) = parent_wd.get(&leaf_owned).await? {
                let existing_pond = existing.node.id().pond_id();
                if existing_pond == foreign_uuid7 {
                    // Idempotent: mount already points at the right pond.
                    return Ok(());
                }
                return Err(StewardError::Aborted(format!(
                    "mount path `{}/{}` already exists with pond_id {}; cannot \
                     attach foreign pond {}",
                    parent_owned, leaf_owned, existing_pond, foreign_uuid7
                )));
            }

            let foreign_node = fs.foreign_root_node(foreign_uuid7).await?;
            let _ = parent_wd.insert_node(&leaf_owned, foreign_node).await?;
            Ok(())
        },
    )
    .await
    .map_err(|e| anyhow!("materialize mount `{}`: {}", mount_path, e))?;
    Ok(())
}

/// Split an absolute mount path into (parent_dir, leaf_name).
/// Errors if the path is `/` (root mount is mirror mode, handled
/// elsewhere) or has no leaf segment.
pub(crate) fn split_mount_path(path: &str) -> Result<(&str, &str)> {
    if !path.starts_with('/') {
        return Err(anyhow!("mount path `{}` must be absolute", path));
    }
    if path == "/" {
        return Err(anyhow!(
            "internal: split_mount_path called on `/` (mirror restart should be filtered earlier)"
        ));
    }
    let trimmed = path.trim_end_matches('/');
    let last_slash = trimmed
        .rfind('/')
        .ok_or_else(|| anyhow!("mount path `{}` has no leaf", path))?;
    let parent = if last_slash == 0 {
        "/"
    } else {
        &trimmed[..last_slash]
    };
    let leaf = &trimmed[last_slash + 1..];
    if leaf.is_empty() {
        return Err(anyhow!("mount path `{}` has empty leaf", path));
    }
    Ok((parent, leaf))
}

#[cfg(test)]
mod tests {
    use super::split_mount_path;

    #[test]
    fn split_mount_path_top_level() {
        assert_eq!(split_mount_path("/imports").unwrap(), ("/", "imports"));
    }

    #[test]
    fn split_mount_path_nested() {
        assert_eq!(
            split_mount_path("/imports/upstream").unwrap(),
            ("/imports", "upstream")
        );
    }

    #[test]
    fn split_mount_path_trailing_slash() {
        assert_eq!(
            split_mount_path("/imports/upstream/").unwrap(),
            ("/imports", "upstream")
        );
    }

    #[test]
    fn split_mount_path_root_rejected() {
        assert!(split_mount_path("/").is_err());
    }

    #[test]
    fn split_mount_path_relative_rejected() {
        assert!(split_mount_path("imports/upstream").is_err());
    }
}
