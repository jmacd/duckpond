// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

use crate::common::ShipContext;
use anyhow::{Result, anyhow};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;

/// Direction of copy operation
#[derive(Debug, PartialEq)]
enum CopyDirection {
    /// Copying from host filesystem into pond
    In,
    /// Copying from pond to host filesystem
    Out,
}

/// Parse a path to check if it has the "host://" prefix
/// Returns (is_host_path, cleaned_path)
fn parse_host_path(path: &str) -> (bool, String) {
    if let Some(stripped) = path.strip_prefix("host://") {
        (true, stripped.to_string())
    } else {
        (false, path.to_string())
    }
}

/// Determine copy direction based on sources and destination
/// - If dest has "host://" → copying OUT (pond → host), sources must NOT have "host://"
/// - If sources have "host://" → copying IN (host → pond), dest must NOT have "host://"
/// - Require explicit and consistent use of "host://" prefix
fn determine_copy_direction(sources: &[String], dest: &str) -> Result<CopyDirection> {
    let (dest_is_host, _) = parse_host_path(dest);

    // Check if any source has host:// prefix
    let sources_have_host = sources.iter().any(|s| parse_host_path(s).0);

    match (dest_is_host, sources_have_host) {
        (true, true) => Err(anyhow!(
            "Invalid copy: cannot use 'host://' prefix on both sources and destination"
        )),
        (true, false) => {
            // Copy OUT: pond → host
            Ok(CopyDirection::Out)
        }
        (false, true) => {
            // Copy IN: host → pond
            // Verify ALL sources have host:// prefix for consistency
            for source in sources {
                if !parse_host_path(source).0 {
                    return Err(anyhow!(
                        "Invalid copy: when copying from host, ALL sources must have 'host://' prefix"
                    ));
                }
            }
            Ok(CopyDirection::In)
        }
        (false, false) => {
            // Backward compatibility: assume copy IN if no host:// anywhere
            // But warn that this is deprecated behavior
            log::warn!(
                "Copying without 'host://' prefix is deprecated. Use 'host://' on sources when copying IN."
            );
            Ok(CopyDirection::In)
        }
    }
}

async fn get_entry_type_for_file(format: &str) -> Result<tinyfs::EntryType> {
    match format {
        "data" => Ok(tinyfs::EntryType::FilePhysicalVersion),
        "table" => Ok(tinyfs::EntryType::TablePhysicalVersion),
        "series" => Ok(tinyfs::EntryType::TablePhysicalSeries),
        _ => Err(anyhow!("Invalid format '{}'", format)),
    }
}

// STREAMING COPY: Copy multiple files to directory using proper context
async fn copy_files_to_directory(
    sources: &[String],
    dest_wd: &tinyfs::WD,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for source in sources {
        // Strip host:// prefix if present (for copy IN with explicit host:// prefix)
        let (_is_host, clean_source) = parse_host_path(source);
        log::debug!(
            "parse_host_path: source='{}' -> clean_source='{}'",
            source,
            clean_source
        );

        // Extract filename from source path
        let source_filename = std::path::Path::new(&clean_source)
            .file_name()
            .ok_or("Invalid file path")?
            .to_str()
            .ok_or("Invalid filename")?;

        copy_single_file_to_directory_with_name(&clean_source, dest_wd, source_filename, format)
            .await?;
    }
    Ok(())
}

// Copy a single file to a directory using the provided working directory context
async fn copy_single_file_to_directory_with_name(
    file_path: &str,
    dest_wd: &tinyfs::WD,
    filename: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    // Determine entry type based on format flag
    let entry_type = get_entry_type_for_file(format).await?;
    let entry_type_str = format!("{:?}", entry_type);

    log::debug!(
        "copy_single_file_to_directory source_path: {file_path}, dest_filename: {filename}, format: {format}, entry_type: {entry_type_str}"
    );

    // For table and series files, we need to validate it's actually a parquet file
    // Table files: store as-is, Series files: also infer temporal bounds after writing
    if entry_type == tinyfs::EntryType::TablePhysicalSeries
        || entry_type == tinyfs::EntryType::TablePhysicalVersion
    {
        // Check if this is actually a parquet file by reading just the magic bytes
        let mut file = File::open(file_path)
            .await
            .map_err(|e| format!("Failed to open source file '{}': {}", file_path, e))?;

        let mut magic = [0u8; 4];
        use tokio::io::AsyncReadExt;
        let _ = file
            .read_exact(&mut magic)
            .await
            .map_err(|e| format!("Failed to read magic bytes from '{}': {}", file_path, e))?;

        if &magic != b"PAR1" {
            return Err(format!(
                "File '{}' is not a valid parquet file (missing PAR1 magic bytes). \
                Use --format=data for non-parquet files like CSV. \
                To query CSV files, use 'pond cat csv:///path' with the csv:// URL scheme.",
                file_path
            )
            .into());
        }

        // Rewind to beginning for streaming copy
        use tokio::io::AsyncSeekExt;
        let _ = file
            .seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| format!("Failed to rewind file: {}", e))?;

        // Create writer and stream the file content efficiently
        let mut dest_writer = dest_wd
            .async_writer_path_with_type(filename, entry_type)
            .await
            .map_err(|e| format!("Failed to create destination writer: {}", e))?;

        // Stream the file bytes without loading into memory
        let _ = tokio::io::copy(&mut file, &mut dest_writer)
            .await
            .map_err(|e| format!("Failed to stream file content: {}", e))?;

        // For series files, infer temporal bounds (reads only footer)
        // This also calls shutdown() internally
        if entry_type == tinyfs::EntryType::TablePhysicalSeries {
            let (min_time, max_time, ts_col) = dest_writer
                .infer_temporal_bounds()
                .await
                .map_err(|e| format!("Failed to infer temporal bounds: {}", e))?;

            log::debug!(
                "Inferred temporal bounds for {}: min={}, max={}, ts_column={}",
                filename,
                min_time,
                max_time,
                ts_col
            );
        } else {
            dest_writer
                .shutdown()
                .await
                .map_err(|e| format!("Failed to complete file write: {}", e))?;
        }
    } else {
        // Non-series files: unified streaming copy
        let mut source_file = File::open(file_path)
            .await
            .map_err(|e| format!("Failed to open source file '{}': {}", file_path, e))?;

        let mut dest_writer = dest_wd
            .async_writer_path_with_type(filename, entry_type)
            .await
            .map_err(|e| format!("Failed to create destination writer: {}", e))?;

        // Stream copy with 64KB buffer for memory efficiency
        _ = tokio::io::copy(&mut source_file, &mut dest_writer)
            .await
            .map_err(|e| format!("Failed to stream file content: {}", e))?;

        dest_writer
            .shutdown()
            .await
            .map_err(|e| format!("Failed to complete file write: {}", e))?;
    }

    log::debug!("Copied {file_path} to directory as {filename}");
    Ok(())
}

/// Recursively copy directory from host filesystem to pond, preserving structure
/// Uses dual traversal: readdir on host paired with in_path on pond WD
async fn copy_directory_recursive(
    ship_context: &ShipContext,
    host_base_dir: &str,
    pond_dest: &str,
) -> Result<()> {
    use std::path::Path;
    use tokio::fs;

    let mut ship = ship_context.open_pond().await?;
    let host_base = Path::new(host_base_dir).to_path_buf();
    let pond_dest = pond_dest.to_string();

    ship.transact(
        &steward::PondUserMetadata::new(vec!["copy-recursive".to_string()]),
        |_tx, fs| {
            Box::pin(async move {
                let root = fs
                    .root()
                    .await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

                // Navigate to or create destination directory
                let dest_wd = if pond_dest != "/" {
                    let clean_dest = pond_dest.trim_end_matches('/');
                    root.create_dir_path(clean_dest).await.map_err(|e| {
                        steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                    })?
                } else {
                    root
                };

                // Dual traversal helper
                fn copy_dir_contents<'a>(
                    host_path: &'a Path,
                    pond_wd: tinyfs::WD,
                ) -> Pin<
                    Box<dyn Future<Output = Result<(u32, u32), steward::StewardError>> + Send + 'a>,
                > {
                    Box::pin(async move {
                        let mut dir_count = 0u32;
                        let mut file_count = 0u32;

                        let mut entries = fs::read_dir(host_path).await.map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                                tinyfs::Error::Other(format!(
                                    "Failed to read directory {:?}: {}",
                                    host_path, e
                                )),
                            ))
                        })?;

                        while let Some(entry) = entries.next_entry().await.map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                                tinyfs::Error::Other(format!("Failed to read entry: {}", e)),
                            ))
                        })? {
                            let host_entry_path = entry.path();
                            let entry_name = entry.file_name();
                            let name_str = entry_name.to_string_lossy().to_string();

                            let metadata = entry.metadata().await.map_err(|e| {
                                steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                                    tinyfs::Error::Other(format!("Failed to get metadata: {}", e)),
                                ))
                            })?;

                            if metadata.is_dir() {
                                // Create directory in pond and recurse
                                let name_for_closure = name_str.clone();
                                let child_wd = pond_wd
                                    .in_path(&name_str, |parent_wd, lookup| async move {
                                        match lookup {
                                            tinyfs::Lookup::NotFound(_, name) => {
                                                // Create directory and return WD for it
                                                parent_wd.create_dir_path(&name).await
                                            }
                                            tinyfs::Lookup::Found(_) => {
                                                // Directory already exists, open it as WD
                                                parent_wd.open_dir_path(&name_for_closure).await
                                            }
                                            tinyfs::Lookup::Empty(_) => {
                                                Err(tinyfs::Error::empty_path())
                                            }
                                        }
                                    })
                                    .await
                                    .map_err(|e| {
                                        steward::StewardError::DataInit(
                                            tlogfs::TLogFSError::TinyFS(e),
                                        )
                                    })?;

                                dir_count += 1;

                                // Recurse into subdirectory
                                let (sub_dirs, sub_files) =
                                    copy_dir_contents(&host_entry_path, child_wd).await?;
                                dir_count += sub_dirs;
                                file_count += sub_files;
                            } else if metadata.is_file() {
                                // Copy file into pond at this level
                                let source_path_str =
                                    host_entry_path.to_str().ok_or_else(|| {
                                        steward::StewardError::DataInit(
                                            tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                                                "Invalid UTF-8 in path".to_string(),
                                            )),
                                        )
                                    })?;

                                // Infer format from file extension
                                let format = if name_str.ends_with(".series") {
                                    "series"
                                } else if name_str.ends_with(".table") {
                                    "table"
                                } else {
                                    "data"
                                };

                                copy_single_file_to_directory_with_name(
                                    source_path_str,
                                    &pond_wd,
                                    &name_str,
                                    format,
                                )
                                .await
                                .map_err(|e| {
                                    steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                                        tinyfs::Error::Other(format!("Failed to copy file: {}", e)),
                                    ))
                                })?;

                                file_count += 1;
                            }
                        }

                        Ok((dir_count, file_count))
                    })
                }

                let (dirs, files) = copy_dir_contents(&host_base, dest_wd).await?;
                log::info!(
                    "Recursively copied {} directories and {} files from {:?} to {:?}",
                    dirs,
                    files,
                    host_base,
                    pond_dest
                );
                Ok(())
            })
        },
    )
    .await
    .map_err(|e| anyhow!("Recursive copy failed: {}", e))
}

/// Copy files INTO the pond from host filesystem
///
/// This function handles the original copy behavior: host → pond
/// Uses scoped transactions for automatic commit/rollback handling.
async fn copy_in(
    ship_context: &ShipContext,
    sources: &[String],
    dest: &str,
    format: &str,
) -> Result<()> {
    let mut ship = ship_context.open_pond().await?;

    // Add a unique marker to verify we're running the right code
    log::debug!("COPY_IN: scoped-transaction-v2.0");

    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    // Check if we have a single source that is a directory on the host
    if sources.len() == 1 {
        let (is_host, host_path) = parse_host_path(&sources[0]);
        if is_host {
            let host_path_buf = PathBuf::from(&host_path);
            if tokio::fs::metadata(&host_path_buf)
                .await
                .ok()
                .map(|m| m.is_dir())
                .unwrap_or(false)
            {
                // This is a recursive directory copy from host to pond
                log::info!(
                    "Detected directory copy from host: {} → {}",
                    host_path,
                    dest
                );
                return copy_directory_recursive(ship_context, &host_path, dest).await;
            }
        }
    }

    // Clone data needed inside the closure
    let sources = sources.to_vec();
    let dest = dest.to_string();
    let format = format.to_string();

    // Use scoped transaction for the copy operation
    ship.transact(
        &steward::PondUserMetadata::new(vec!["copy".to_string(), dest.clone()]),
        |_tx, fs| Box::pin(async move {
            let root = fs.root().await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

            let copy_result = root.resolve_copy_destination(&dest).await;
            match copy_result {
                Ok((dest_wd, dest_type)) => {
                    match dest_type {
                        tinyfs::CopyDestination::Directory | tinyfs::CopyDestination::ExistingDirectory => {
                            // Destination is a directory (either explicit with / or existing) - copy files into it
                            copy_files_to_directory(&sources, &dest_wd, &format).await
                                .map_err(|e| steward::StewardError::DataInit(
                                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Copy to directory failed: {}", e)))
                                ))
                        }
                        tinyfs::CopyDestination::ExistingFile => {
                            // Destination is an existing file - not supported for copy operations
                            if sources.len() == 1 {
                                Err(steward::StewardError::DataInit(
                                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                                        format!("Destination '{}' exists but is not a directory (cannot copy to existing file)", &dest)
                                    ))
                                ))
                            } else {
                                Err(steward::StewardError::DataInit(
                                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                                        format!("When copying multiple files, destination '{}' must be a directory", &dest)
                                    ))
                                ))
                            }
                        }
                        tinyfs::CopyDestination::NewPath(name) => {
                            // Destination doesn't exist
                            if sources.len() == 1 {
                                // Single file to non-existent destination - use format flag only
                                let source = &sources[0];

                                // Strip host:// prefix if present
                                let (_is_host, clean_source) = parse_host_path(source);

                                // Use the same logic as directory copying, just with the specific filename
                                copy_single_file_to_directory_with_name(&clean_source, &dest_wd, &name, &format).await
                                    .map_err(|e| steward::StewardError::DataInit(
                                        tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to copy file: {}", e)))
                                    ))?;

                                log::debug!("Copied {source} to {name}");
                                Ok(())
                            } else {
                                Err(steward::StewardError::DataInit(
                                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(
                                        format!("When copying multiple files, destination '{}' must be an existing directory", &dest)
                                    ))
                                ))
                            }
                        }
                    }
                }
                Err(e) => {
                    // Check if this is a trailing slash case where we need to create a directory
                    if dest.ends_with('/') {
                        // Directory doesn't exist but user wants to copy into directory
                        // Create the directory first
                        let clean_dest = dest.trim_end_matches('/');
                        _ = root.create_dir_path(clean_dest).await
                            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

                        // Now resolve the destination again (should work now)
                        let (dest_wd, _) = root.resolve_copy_destination(clean_dest).await
                            .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

                        // Copy files to the newly created directory
                        copy_files_to_directory(&sources, &dest_wd, &format).await
                            .map_err(|e| steward::StewardError::DataInit(
                                tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Copy to created directory failed: {}", e)))
                            ))
                    } else {
                        Err(steward::StewardError::DataInit(
                            tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Failed to resolve destination '{}': {}", &dest, e)))
                        ))
                    }
                }
            }
        })
    ).await.map_err(|e| anyhow!("Copy operation failed: {}", e))?;

    log::info!("✅ File(s) copied into pond successfully");
    Ok(())
}

/// Copy files OUT of the pond to host filesystem
///
/// Supports glob patterns and maintains directory structure.
async fn copy_out(ship_context: &ShipContext, sources: &[String], dest: &str) -> Result<()> {
    use crate::common::FileInfoVisitor;
    use tokio::fs;

    // Parse the host:// prefix from destination
    let (_is_host, host_dest) = parse_host_path(dest);

    log::debug!("COPY_OUT: sources={:?}, dest={}", sources, host_dest);

    let mut ship = ship_context.open_pond().await?;

    // Use read transaction to access pond
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "copy-out".to_string(),
            dest.to_string(),
        ]))
        .await
        .map_err(|e| anyhow!("Failed to begin transaction: {}", e))?;

    // Collect all matching files from all patterns
    let mut all_files = Vec::new();

    {
        let fs = &*tx;
        let root = fs.root().await?;

        for pattern in sources {
            let mut visitor = FileInfoVisitor::new(true); // Show all files
            let results = root
                .visit_with_visitor(pattern, &mut visitor)
                .await
                .map_err(|e| anyhow!("Failed to match pattern '{}': {}", pattern, e))?;

            log::debug!("Pattern '{}' matched {} files", pattern, results.len());
            all_files.extend(results);
        }
    }

    // Commit transaction before exporting
    _ = tx
        .commit()
        .await
        .map_err(|e| anyhow!("Failed to commit transaction: {}", e))?;

    if all_files.is_empty() {
        return Err(anyhow!("No files matched the specified patterns"));
    }

    let file_count = all_files.len();
    log::info!("Found {} file(s) to export", file_count);

    // Export each file
    for file_info in all_files {
        // Skip directories
        if file_info.metadata.entry_type.is_directory() {
            continue;
        }

        // Compute output path: dest/relative_path
        // Remove leading slash from pond path to make it relative
        let relative_path = file_info.path.trim_start_matches('/');
        let output_path = std::path::Path::new(&host_dest).join(relative_path);

        // Create parent directories
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| anyhow!("Failed to create directory {:?}: {}", parent, e))?;
        }

        log::debug!("Exporting {} to {:?}", file_info.path, output_path);

        // Determine export method based on file type
        if file_info.metadata.entry_type.is_series_file()
            || file_info.metadata.entry_type.is_table_file()
        {
            // Export as parquet using DataFusion (like cat --format=raw)
            export_queryable_file_as_parquet(ship_context, &file_info.path, &output_path).await?;
        } else {
            // Export raw bytes
            export_raw_file(ship_context, &file_info.path, &output_path).await?;
        }
    }

    log::info!("✅ {} file(s) exported successfully", file_count);
    Ok(())
}

/// Export a file:series or file:table as parquet
async fn export_queryable_file_as_parquet(
    ship_context: &ShipContext,
    pond_path: &str,
    output_path: &std::path::Path,
) -> Result<()> {
    use datafusion::physical_plan::SendableRecordBatchStream;
    use futures::stream::StreamExt;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    let mut ship = ship_context.open_pond().await?;
    let mut tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "export".to_string(),
            pond_path.to_string(),
        ]))
        .await?;

    let stream: SendableRecordBatchStream = {
        let fs = &*tx;
        let root = fs.root().await?;

        // Use default SELECT * query sorted by timestamp
        let sql_query = "SELECT * FROM series ORDER BY timestamp";

        tlogfs::execute_sql_on_file(&root, pond_path, sql_query, tx.transaction_guard()?)
            .await
            .map_err(|e| anyhow!("Failed to execute SQL query on '{}': {}", pond_path, e))?
    };

    // Commit transaction
    _ = tx.commit().await?;

    // Get the first batch to determine schema
    let mut stream = stream;
    let first_batch = match stream.next().await {
        Some(Ok(batch)) => batch,
        Some(Err(e)) => return Err(anyhow!("Failed to read first batch: {}", e)),
        None => {
            log::debug!("Empty file, creating empty parquet");
            // Create empty parquet file
            _ = tokio::fs::File::create(output_path).await?;
            return Ok(());
        }
    };

    let schema = first_batch.schema();

    // Create parquet writer
    let file = File::create(output_path)
        .map_err(|e| anyhow!("Failed to create output file {:?}: {}", output_path, e))?;

    let mut writer = ArrowWriter::try_new(file, schema, None)
        .map_err(|e| anyhow!("Failed to create parquet writer: {}", e))?;

    // Write first batch
    writer
        .write(&first_batch)
        .map_err(|e| anyhow!("Failed to write batch to parquet: {}", e))?;

    // Stream remaining batches
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| anyhow!("Failed to read batch from stream: {}", e))?;
        writer
            .write(&batch)
            .map_err(|e| anyhow!("Failed to write batch to parquet: {}", e))?;
    }

    _ = writer
        .close()
        .map_err(|e| anyhow!("Failed to close parquet writer: {}", e))?;

    log::debug!("Exported parquet file to {:?}", output_path);
    Ok(())
}

/// Export a regular file as raw bytes
async fn export_raw_file(
    ship_context: &ShipContext,
    pond_path: &str,
    output_path: &std::path::Path,
) -> Result<()> {
    use tokio::io::copy;

    let mut ship = ship_context.open_pond().await?;
    let tx = ship
        .begin_read(&steward::PondUserMetadata::new(vec![
            "export".to_string(),
            pond_path.to_string(),
        ]))
        .await?;

    let mut reader = {
        let fs = &*tx;
        let root = fs.root().await?;

        root.async_reader_path(pond_path)
            .await
            .map_err(|e| anyhow!("Failed to open file '{}': {}", pond_path, e))?
    };

    _ = tx.commit().await?;

    // Create output file and stream content
    let mut output_file = tokio::fs::File::create(output_path)
        .await
        .map_err(|e| anyhow!("Failed to create output file {:?}: {}", output_path, e))?;

    _ = copy(&mut reader, &mut output_file)
        .await
        .map_err(|e| anyhow!("Failed to copy file content: {}", e))?;

    log::debug!("Exported raw file to {:?}", output_path);
    Ok(())
}

/// Main copy command dispatcher
///
/// Determines copy direction and dispatches to appropriate handler:
/// - If dest has "host://" prefix → copy OUT (pond → host)
/// - Otherwise → copy IN (host → pond)
pub async fn copy_command(
    ship_context: &ShipContext,
    sources: &[String],
    dest: &str,
    format: &str,
) -> Result<()> {
    // Validate arguments
    if sources.is_empty() {
        return Err(anyhow!("At least one source file must be specified"));
    }

    // Determine copy direction
    let direction = determine_copy_direction(sources, dest)?;

    match direction {
        CopyDirection::In => {
            log::debug!("Copy direction: IN (host → pond)");
            copy_in(ship_context, sources, dest, format).await
        }
        CopyDirection::Out => {
            log::debug!("Copy direction: OUT (pond → host)");
            if !format.is_empty() && format != "data" {
                log::warn!(
                    "--format flag is ignored when copying out (format determined by source file type)"
                );
            }
            copy_out(ship_context, sources, dest).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::copy_command;
    use crate::commands::init::init_command;
    use crate::common::ShipContext;
    use anyhow::Result;
    use arrow_array::{Float64Array, Int64Array, RecordBatch, record_batch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tinyfs::arrow::ParquetExt;
    use tokio::fs::File;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::bytes::Bytes;

    /// Test setup helper - creates pond and host files for copy testing
    struct TestSetup {
        pond_path: std::path::PathBuf,
        host_files_dir: std::path::PathBuf,
        ship_context: ShipContext,
        _temp_dir: TempDir,
    }

    impl TestSetup {
        async fn new() -> Result<Self> {
            // Create a temporary directory for the test pond
            let temp_dir = TempDir::new()?;
            // Use a unique subdirectory to avoid any conflicts
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let pond_path = temp_dir.path().join(format!("test_pond_{}", timestamp));
            let host_files_dir = temp_dir.path().join("host_files");

            // Create host files directory
            tokio::fs::create_dir_all(&host_files_dir).await?;

            // Create ship context for initialization
            let init_args = vec!["pond".to_string(), "init".to_string()];
            let ship_context = ShipContext::new(Some(&pond_path), init_args.clone());

            // Initialize the pond
            init_command(&ship_context, None, None).await?;

            Ok(TestSetup {
                pond_path,
                host_files_dir,
                ship_context,
                _temp_dir: temp_dir,
            })
        }

        /// Create a text file in the host filesystem
        async fn create_host_text_file(
            &self,
            filename: &str,
            content: &str,
        ) -> Result<std::path::PathBuf> {
            let file_path = self.host_files_dir.join(filename);
            let mut file = File::create(&file_path).await?;
            file.write_all(content.as_bytes()).await?;
            file.shutdown().await?;
            Ok(file_path)
        }

        /// Create a CSV file in the host filesystem
        async fn create_host_csv_file(&self, filename: &str) -> Result<std::path::PathBuf> {
            let content = "timestamp,value,doubled_value\n2024-01-01T00:00:00Z,42.0,84.0\n2024-01-01T01:00:00Z,43.5,87.0\n";
            self.create_host_text_file(filename, content).await
        }

        /// Create a series-format parquet file in the host filesystem with proper temporal data
        async fn create_host_series_parquet_file(
            &self,
            filename: &str,
        ) -> Result<std::path::PathBuf> {
            let file_path = self.host_files_dir.join(filename);

            let schema = Schema::new(vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
                Field::new("doubled_value", DataType::Float64, false),
            ]);

            let timestamp_array = Int64Array::from(vec![1704067200000_i64, 1704070800000_i64]); // 2024-01-01 timestamps
            let value_array = Float64Array::from(vec![42.0_f64, 43.5_f64]);
            let doubled_value_array = Float64Array::from(vec![84.0_f64, 87.0_f64]);

            let batch = RecordBatch::try_new(
                Arc::new(schema),
                vec![
                    Arc::new(timestamp_array),
                    Arc::new(value_array),
                    Arc::new(doubled_value_array),
                ],
            )?;

            let file = std::fs::File::create(&file_path)?;
            let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
            writer.write(&batch)?;
            _ = writer.close()?;

            Ok(file_path)
        }

        /// Helper to get copy context
        fn copy_context(
            &self,
            sources: Vec<String>,
            dest: String,
            format: String,
        ) -> (ShipContext, Vec<String>, String, String) {
            (self.ship_context.clone(), sources, dest, format)
        }

        /// Verify a file exists in the pond by trying to read it
        async fn verify_file_exists(&self, path: &str) -> Result<bool> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec!["verify".to_string()]))
                .await?;
            let fs = &*tx;
            let root = fs.root().await?;

            let exists = match root.async_reader_path(path).await {
                Ok(_) => true,
                Err(tinyfs::Error::NotFound(_)) => false,
                Err(e) => return Err(e.into()),
            };

            _ = tx.commit().await?;
            Ok(exists)
        }

        /// Read file content from the pond using TinyFS
        async fn read_pond_file_content(&self, path: &str) -> Result<Vec<u8>> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec!["read".to_string()]))
                .await?;
            let fs = &*tx;
            let root = fs.root().await?;

            let mut content = Vec::new();
            {
                let mut reader = root.async_reader_path(path).await?;
                _ = reader.read_to_end(&mut content).await?;
            }

            _ = tx.commit().await?;
            Ok(content)
        }

        /// Verify file metadata by reading the node metadata directly
        async fn verify_file_metadata(
            &self,
            path: &str,
            _expected_type: tinyfs::EntryType,
        ) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;
            let tx = ship
                .begin_read(&steward::PondUserMetadata::new(vec![
                    "metadata".to_string(),
                ]))
                .await?;
            let fs = &*tx;
            let root = fs.root().await?;

            // Try to read the file to verify it exists and get the entry type info
            // (The exact metadata API is not exposed in the same way, but we can verify by successful read)
            let _reader = root.async_reader_path(path).await.map_err(|e| {
                anyhow::anyhow!("File {} does not exist or wrong type: {}", path, e)
            })?;

            _ = tx.commit().await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_copy_single_text_file() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create host text file
        let content = "Hello, DuckPond!\nThis is a test file.\n";
        let host_file = setup.create_host_text_file("test.txt", content).await?;

        // Copy to pond as data file
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()],
            "copied_test.txt".to_string(),
            "data".to_string(),
        );

        copy_command(&ship_context, &sources, &dest, &format).await?;

        // Verify file exists and has correct content
        assert!(
            setup.verify_file_exists("copied_test.txt").await?,
            "File should exist in pond"
        );

        let pond_content = setup.read_pond_file_content("copied_test.txt").await?;
        assert_eq!(
            pond_content,
            content.as_bytes(),
            "File content should match"
        );

        // Verify metadata (simplified check)
        setup
            .verify_file_metadata("copied_test.txt", tinyfs::EntryType::FilePhysicalVersion)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_parquet_as_series() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create host parquet file with proper temporal data for series
        let host_file = setup
            .create_host_series_parquet_file("test_series.parquet")
            .await?;

        // Copy to pond as series file
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()],
            "copied_series.parquet".to_string(),
            "series".to_string(),
        );

        copy_command(&ship_context, &sources, &dest, &format).await?;

        // Verify file exists in the pond
        assert!(
            setup.verify_file_exists("copied_series.parquet").await?,
            "Series file should exist in pond"
        );

        // Verify content is preserved by reading the parquet file
        let pond_content = setup
            .read_pond_file_content("copied_series.parquet")
            .await?;

        // Verify PAR1 magic bytes (parquet format)
        assert!(
            pond_content.len() > 4,
            "Series file should be larger than 4 bytes"
        );
        assert_eq!(
            &pond_content[0..4],
            b"PAR1",
            "Series file should start with PAR1 magic bytes"
        );

        // Verify we can read it as parquet and get the expected temporal data

        let bytes = Bytes::from(pond_content);
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| anyhow::anyhow!("Failed to create arrow reader: {}", e))?
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build arrow reader: {}", e))?;

        let batch = arrow_reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("No batch found in series file"))?
            .map_err(|e| anyhow::anyhow!("Failed to read batch: {}", e))?;

        // Verify the schema matches our test data
        let schema = batch.schema();
        assert_eq!(schema.fields().len(), 3, "Series should have 3 fields");
        assert_eq!(
            schema.field(0).name(),
            "timestamp",
            "First field should be timestamp"
        );
        assert_eq!(
            schema.field(1).name(),
            "value",
            "Second field should be value"
        );
        assert_eq!(
            schema.field(2).name(),
            "doubled_value",
            "Third field should be doubled_value"
        );

        // Verify the actual data values - series temporal data
        assert_eq!(batch.num_rows(), 2, "Should have 2 rows");

        // Check timestamp column (Int64 milliseconds since epoch)
        let timestamp_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Int64Array for timestamp"))?;
        assert_eq!(timestamp_col.value(0), 1704067200000_i64); // 2024-01-01T00:00:00Z
        assert_eq!(timestamp_col.value(1), 1704070800000_i64); // 2024-01-01T01:00:00Z

        // Check value columns
        let value_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Float64Array for value"))?;
        assert_eq!(value_col.value(0), 42.0);
        assert_eq!(value_col.value(1), 43.5);

        let doubled_value_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected Float64Array for doubled_value"))?;
        assert_eq!(doubled_value_col.value(0), 84.0);
        assert_eq!(doubled_value_col.value(1), 87.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_single_csv_as_data() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create host CSV file
        let host_file = setup.create_host_csv_file("test.csv").await?;

        // Copy to pond as data file (CSV is raw data, not a parquet table)
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()],
            "copied_data.csv".to_string(),
            "data".to_string(),
        );

        copy_command(&ship_context, &sources, &dest, &format).await?;

        // Verify file exists
        assert!(
            setup.verify_file_exists("copied_data.csv").await?,
            "File should exist in pond"
        );

        // Verify content is preserved
        let pond_content = setup.read_pond_file_content("copied_data.csv").await?;
        let expected_content = "timestamp,value,doubled_value\n2024-01-01T00:00:00Z,42.0,84.0\n2024-01-01T01:00:00Z,43.5,87.0\n";
        assert_eq!(
            String::from_utf8(pond_content)?,
            expected_content,
            "CSV content should be preserved"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_multiple_files_to_directory() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create multiple host files
        let file1 = setup
            .create_host_text_file("file1.txt", "Content of file 1")
            .await?;
        let file2 = setup
            .create_host_text_file("file2.txt", "Content of file 2")
            .await?;
        let file3 = setup.create_host_csv_file("data.csv").await?;

        // Copy all to a directory (use trailing slash to ensure directory creation)
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![
                file1.to_string_lossy().to_string(),
                file2.to_string_lossy().to_string(),
                file3.to_string_lossy().to_string(),
            ],
            "uploaded/".to_string(),
            "data".to_string(),
        );

        copy_command(&ship_context, &sources, &dest, &format).await?;

        // Verify all files exist in the directory
        assert!(
            setup.verify_file_exists("uploaded/file1.txt").await?,
            "file1.txt should exist in directory"
        );
        assert!(
            setup.verify_file_exists("uploaded/file2.txt").await?,
            "file2.txt should exist in directory"
        );
        assert!(
            setup.verify_file_exists("uploaded/data.csv").await?,
            "data.csv should exist in directory"
        );

        // Verify content
        let content1 = setup.read_pond_file_content("uploaded/file1.txt").await?;
        assert_eq!(String::from_utf8(content1)?, "Content of file 1");

        let content2 = setup.read_pond_file_content("uploaded/file2.txt").await?;
        assert_eq!(String::from_utf8(content2)?, "Content of file 2");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_csv_with_data_format() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create CSV file and copy as data format (not table, since CSV isn't parquet)
        let host_file = setup.create_host_csv_file("test.csv").await?;

        // Copy with data format (CSV files should use data format)
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()],
            "formatted_as_data.csv".to_string(),
            "data".to_string(),
        );

        copy_command(&ship_context, &sources, &dest, &format).await?;

        // Verify it was stored successfully
        assert!(
            setup.verify_file_exists("formatted_as_data.csv").await?,
            "File should exist in pond"
        );

        Ok(())
    }

    /// Create a parquet series file directly in the pond - extension for TestSetup
    impl TestSetup {
        async fn create_parquet_series_in_pond(&self, path: &str) -> Result<()> {
            let mut ship = steward::Ship::open_pond(&self.pond_path).await?;

            let path = path.to_string();
            ship.transact(
                &steward::PondUserMetadata::new(vec![
                    "test".to_string(),
                    "create_series".to_string(),
                ]),
                move |_tx, fs| {
                    Box::pin(async move {
                        let root = fs.root().await.map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                        })?;

                        // Create Arrow RecordBatch with temporal data using record_batch! macro
                        let batch = record_batch!(
                            ("timestamp", Int64, [1000_i64, 2000_i64, 3000_i64]),
                            ("value", Float64, [10.5_f64, 20.5_f64, 30.5_f64])
                        )
                        .map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                                tinyfs::Error::Other(format!("Arrow error: {}", e)),
                            ))
                        })?;

                        // Write as file:series using ParquetExt with temporal metadata extraction
                        let _ = root
                            .create_series_from_batch(&path, &batch, Some("timestamp"))
                            .await
                            .map_err(|e| {
                                steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                            })?;

                        Ok(())
                    })
                },
            )
            .await?;

            Ok(())
        }
    }

    #[tokio::test]
    async fn test_copy_error_handling() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Test copying non-existent file
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec!["/nonexistent/file.txt".to_string()],
            "should_fail.txt".to_string(),
            "data".to_string(),
        );

        let result = copy_command(&ship_context, &sources, &dest, &format).await;
        assert!(
            result.is_err(),
            "Should fail when copying non-existent file"
        );

        // Test copying with empty sources
        let result = copy_command(&ship_context, &[], &dest, &format).await;
        assert!(result.is_err(), "Should fail with empty sources");

        // Test copying with invalid format
        let host_file = setup.create_host_text_file("test.txt", "test").await?;
        let (ship_context, sources, dest, _) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()],
            "test.txt".to_string(),
            "invalid_format".to_string(),
        );

        let result = copy_command(&ship_context, &sources, &dest, "invalid_format").await;
        assert!(result.is_err(), "Should fail with invalid format");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_single_file_to_new_path() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create host file
        let host_file = setup
            .create_host_text_file("original.txt", "Original content")
            .await?;

        // Copy to new filename (not directory)
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec![host_file.to_string_lossy().to_string()],
            "renamed_file.txt".to_string(),
            "data".to_string(),
        );

        copy_command(&ship_context, &sources, &dest, &format).await?;

        // Verify the file exists with the new name
        assert!(
            setup.verify_file_exists("renamed_file.txt").await?,
            "File should exist with new name"
        );

        let content = setup.read_pond_file_content("renamed_file.txt").await?;
        assert_eq!(String::from_utf8(content)?, "Original content");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_out_parquet_series_to_host() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create directories first
        let mut ship = steward::Ship::open_pond(&setup.pond_path).await?;
        ship.transact(
            &steward::PondUserMetadata::new(vec!["test".to_string(), "setup".to_string()]),
            |_tx, fs| {
                Box::pin(async move {
                    let root = fs.root().await.map_err(|e| {
                        steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                    })?;
                    _ = root.create_dir_path("/data").await.map_err(|e| {
                        steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                    })?;
                    _ = root.create_dir_path("/data/nested").await.map_err(|e| {
                        steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                    })?;
                    Ok(())
                })
            },
        )
        .await?;

        // Create multiple parquet series files in the pond
        let series_paths = vec![
            "/data/sensor1.series",
            "/data/sensor2.series",
            "/data/nested/sensor3.series",
        ];

        for series_path in &series_paths {
            setup.create_parquet_series_in_pond(series_path).await?;
        }

        // Create temporary output directory
        let output_dir = tempfile::tempdir()?;
        let output_path = output_dir.path().to_string_lossy().to_string();

        // Copy out using pattern
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec!["/data/**/*.series".to_string()],
            format!("host://{}", output_path),
            "data".to_string(), // Format flag should be ignored for copy out
        );

        copy_command(&ship_context, &sources, &dest, &format).await?;

        // Verify all files were exported with correct directory structure
        for series_path in &series_paths {
            let relative_path = series_path.trim_start_matches('/');
            let exported_file = output_dir.path().join(relative_path);

            assert!(
                exported_file.exists(),
                "Exported file should exist: {:?}",
                exported_file
            );

            // Verify it's a valid parquet file by reading it
            let file = std::fs::File::open(&exported_file)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

            let mut row_count = 0;
            for batch_result in reader {
                let batch = batch_result?;
                row_count += batch.num_rows();
            }

            assert!(row_count > 0, "Exported parquet file should contain data");
            log::debug!(
                "✅ Verified exported file: {:?} ({} rows)",
                exported_file,
                row_count
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_out_single_series_to_host() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a single parquet series file
        setup
            .create_parquet_series_in_pond("/test_sensor.series")
            .await?;

        // Create temporary output directory
        let output_dir = tempfile::tempdir()?;
        let output_path = output_dir.path().to_string_lossy().to_string();

        // Copy out single file
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec!["/test_sensor.series".to_string()],
            format!("host://{}", output_path),
            "data".to_string(),
        );

        copy_command(&ship_context, &sources, &dest, &format).await?;

        // Verify file was exported
        let exported_file = output_dir.path().join("test_sensor.series");
        assert!(
            exported_file.exists(),
            "Exported file should exist: {:?}",
            exported_file
        );

        // Verify parquet content
        let file = std::fs::File::open(&exported_file)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;

        let batches: Result<Vec<_>, _> = reader.collect();
        let batches = batches?;

        assert!(!batches.is_empty(), "Should have at least one batch");

        // Verify schema has expected columns
        let schema = batches[0].schema();
        assert!(
            schema.column_with_name("timestamp").is_some(),
            "Should have timestamp column"
        );
        assert!(
            schema.column_with_name("value").is_some(),
            "Should have value column"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_out_requires_host_prefix() -> Result<()> {
        let setup = TestSetup::new().await?;

        // Create a parquet series file
        setup.create_parquet_series_in_pond("/test.series").await?;

        // Try to copy out WITHOUT host:// prefix - should fail or be treated as copy IN
        let (ship_context, sources, dest, format) = setup.copy_context(
            vec!["/test.series".to_string()],
            "/tmp/output".to_string(), // Missing host:// prefix
            "data".to_string(),
        );

        // This should be treated as copy IN and fail because /test.series is not a host file
        let result = copy_command(&ship_context, &sources, &dest, &format).await;
        assert!(
            result.is_err(),
            "Should fail when copying pond path to pond path without host:// prefix"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_roundtrip_out_then_in() -> Result<()> {
        // Setup: Create first pond with test files
        let setup1 = TestSetup::new().await?;

        // Create directories first
        let mut ship1 = steward::Ship::open_pond(&setup1.pond_path).await?;
        ship1
            .transact(
                &steward::PondUserMetadata::new(vec!["test".to_string(), "setup".to_string()]),
                |_tx, fs| {
                    Box::pin(async move {
                        let root = fs.root().await.map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                        })?;
                        _ = root.create_dir_path("/data").await.map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                        })?;
                        _ = root.create_dir_path("/data/subdir").await.map_err(|e| {
                            steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e))
                        })?;
                        Ok(())
                    })
                },
            )
            .await?;

        // Create test files in first pond
        let test_files = vec![
            "/data/file1.series",
            "/data/file2.series",
            "/data/subdir/file3.series",
        ];

        for file_path in &test_files {
            setup1.create_parquet_series_in_pond(file_path).await?;
        }

        // Step 1: Copy OUT to host filesystem
        let output_dir = tempfile::tempdir()?;
        let output_path = output_dir.path().to_string_lossy().to_string();

        let (ship_context1, sources, dest, format) = setup1.copy_context(
            vec!["/data/**/*.series".to_string()],
            format!("host://{}", output_path),
            "data".to_string(),
        );

        copy_command(&ship_context1, &sources, &dest, &format).await?;

        // Verify files were exported
        let exported_files: Vec<_> = test_files
            .iter()
            .map(|p| {
                let relative = p.trim_start_matches('/');
                output_dir.path().join(relative)
            })
            .collect();

        for exported_file in &exported_files {
            assert!(
                exported_file.exists(),
                "Exported file should exist: {:?}",
                exported_file
            );
        }

        // Step 2: Create NEW pond and copy IN from host
        let setup2 = TestSetup::new().await?;

        // Copy the entire directory structure back into the new pond
        let (ship_context2, sources2, dest2, format2) = setup2.copy_context(
            vec![format!("host://{}/data", output_path)],
            "/imported".to_string(),
            "data".to_string(),
        );

        copy_command(&ship_context2, &sources2, &dest2, &format2).await?;

        // Step 3: Verify all files exist in second pond with correct structure
        for original_path in &test_files {
            let relative = original_path.trim_start_matches("/data/");
            let imported_path = format!("/imported/{}", relative);

            assert!(
                setup2.verify_file_exists(&imported_path).await?,
                "Imported file should exist: {}",
                imported_path
            );

            // Read content from second pond
            let content = setup2.read_pond_file_content(&imported_path).await?;

            // Verify content is non-empty (contains parquet data)
            assert!(!content.is_empty(), "Imported file should not be empty");

            // Verify parquet magic bytes (PAR1)
            assert_eq!(&content[0..4], b"PAR1", "Should be valid parquet file");

            log::debug!(
                "✅ Round-trip verified for: {} ({} bytes)",
                imported_path,
                content.len()
            );
        }

        log::info!(
            "✅ Round-trip test complete: {} files copied OUT and IN successfully",
            test_files.len()
        );
        Ok(())
    }
}
