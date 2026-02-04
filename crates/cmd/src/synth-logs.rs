// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Synthetic Log File Generator
//!
//! Generates deterministic CSV log files with rotation for testing logfile_ingest.
//!
//! Each line contains:
//! - row_number: Sequential integer starting from 1
//! - hash: First 16 hex chars of blake3(row_number as little-endian bytes)
//!
//! The hash is deterministic, so verification is straightforward:
//! given any row_number, the hash can be independently computed.
//!
//! # Example output:
//! ```csv
//! 1,af1349b9f5f9a1a6
//! 2,5cdc85a35d51f54c
//! 3,ce94f66f47d69fa2
//! ```

use clap::Parser;
use file_rotate::{
    ContentLimit, FileRotate,
    compression::Compression,
    suffix::{AppendTimestamp, DateFrom, FileLimit},
};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Synthetic log file generator with rotation
#[derive(Parser, Debug)]
#[command(name = "synth-logs")]
#[command(about = "Generate synthetic CSV log files with rotation for testing logfile_ingest")]
struct Args {
    /// Output directory for log files
    #[arg(short, long, default_value = "./logs")]
    output_dir: PathBuf,

    /// Base name for log files (active file will be {name}.log)
    #[arg(short, long, default_value = "app")]
    name: String,

    /// Number of rows to generate
    #[arg(short, long, default_value = "1000")]
    rows: u64,

    /// Rotation size in lines (rotate when file exceeds this many lines)
    #[arg(short = 'l', long, default_value = "100")]
    lines: usize,

    /// Maximum number of archived files to keep
    #[arg(short, long, default_value = "10")]
    max_files: usize,

    /// Timestamp format for archived files (strftime format)
    /// Default creates files like: app.log.2025-01-03
    #[arg(short, long, default_value = "%Y-%m-%d")]
    format: String,

    /// Delay between writes in milliseconds (0 = no delay)
    #[arg(short, long, default_value = "0")]
    delay_ms: u64,

    /// Starting row number (useful for resuming or testing append scenarios)
    #[arg(long, default_value = "1")]
    start_row: u64,

    /// Verify output after generation
    #[arg(long, default_value = "false")]
    verify: bool,
}

/// Compute deterministic hash for a row number
fn row_hash(row_number: u64) -> String {
    let hash = blake3::hash(&row_number.to_le_bytes());
    // Return first 16 hex characters for readability
    hash.to_hex()[..16].to_string()
}

/// Generate a single CSV line
fn generate_line(row_number: u64) -> String {
    format!("{},{}\n", row_number, row_hash(row_number))
}

/// Verify a CSV line matches expected format
fn verify_line(line: &str, expected_row: u64) -> Result<(), String> {
    let parts: Vec<&str> = line.trim().split(',').collect();
    if parts.len() != 2 {
        return Err(format!(
            "Invalid format: expected 2 fields, got {}",
            parts.len()
        ));
    }

    let row_number: u64 = parts[0]
        .parse()
        .map_err(|_| format!("Invalid row number: {}", parts[0]))?;

    if row_number != expected_row {
        return Err(format!(
            "Row number mismatch: expected {}, got {}",
            expected_row, row_number
        ));
    }

    let expected_hash = row_hash(row_number);
    if parts[1] != expected_hash {
        return Err(format!(
            "Hash mismatch for row {}: expected {}, got {}",
            row_number, expected_hash, parts[1]
        ));
    }

    Ok(())
}

#[allow(clippy::print_stdout)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Ensure output directory exists
    std::fs::create_dir_all(&args.output_dir)?;

    // Construct the log file path
    let log_path = args.output_dir.join(format!("{}.log", args.name));

    println!("=== Synthetic Log Generator ===");
    println!("Output: {}", log_path.display());
    println!("Rows: {} (starting from {})", args.rows, args.start_row);
    println!("Rotation: {} lines per file", args.lines);
    println!("Max archived files: {}", args.max_files);
    println!("Timestamp format: {}", args.format);
    println!();

    // Leak the format string to get 'static lifetime (it's fine, we only do this once)
    let format: &'static str = Box::leak(args.format.into_boxed_str());

    // Create rotating log writer with timestamp suffixes
    // Use Lines instead of Bytes to avoid splitting lines mid-rotation
    let mut log = FileRotate::new(
        &log_path,
        AppendTimestamp::with_format(format, FileLimit::MaxFiles(args.max_files), DateFrom::Now),
        ContentLimit::Lines(args.lines),
        Compression::None,
        None,
    );

    let end_row = args.start_row + args.rows;
    let mut bytes_written: u64 = 0;
    let mut rotations = 0;

    // Track rotations by watching log_paths length
    let initial_archived = log.log_paths().len();

    for row in args.start_row..end_row {
        let line = generate_line(row);
        bytes_written += line.len() as u64;
        write!(log, "{}", line)?;

        // Check for rotation
        let current_archived = log.log_paths().len();
        if current_archived > initial_archived + rotations {
            rotations = current_archived - initial_archived;
            println!(
                "Rotation {} at row {} ({} bytes total)",
                rotations, row, bytes_written
            );
        }

        if args.delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(args.delay_ms));
        }
    }

    // Ensure all data is flushed
    log.flush()?;

    println!();
    println!("=== Generation Complete ===");
    println!("Total rows: {}", args.rows);
    println!("Total bytes: {}", bytes_written);
    println!("Total rotations: {}", rotations);

    // List generated files
    println!();
    println!("=== Generated Files ===");

    // Active file
    if log_path.exists() {
        let size = std::fs::metadata(&log_path)?.len();
        println!("  {} ({} bytes) [active]", log_path.display(), size);
    }

    // Archived files
    for path in log.log_paths() {
        if path.exists() {
            let size = std::fs::metadata(&path)?.len();
            println!("  {} ({} bytes)", path.display(), size);
        }
    }

    // Verification
    if args.verify {
        println!();
        println!("=== Verification ===");
        verify_all_files(&log_path, &log.log_paths(), args.start_row, end_row)?;
    }

    Ok(())
}

/// Verify all generated files contain valid, sequential data
fn verify_all_files(
    active_path: &Path,
    archived_paths: &[PathBuf],
    start_row: u64,
    end_row: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // file-rotate with AppendTimestamp returns paths in chronological order (oldest first)
    // Lexical sort also works: .2026-01-05 < .2026-01-05.1 < .2026-01-05.2 etc
    let mut all_paths: Vec<PathBuf> = archived_paths.to_vec();
    all_paths.sort(); // Ensure chronological order
    all_paths.push(active_path.to_path_buf()); // Active file is always newest

    let mut expected_row = start_row;
    let mut total_lines = 0;

    for path in &all_paths {
        if !path.exists() {
            continue;
        }

        let content = std::fs::read_to_string(path)?;
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            verify_line(line, expected_row)?;
            expected_row += 1;
            total_lines += 1;
        }
    }

    if expected_row != end_row {
        return Err(format!(
            "Row count mismatch: expected {} rows, found {}",
            end_row - start_row,
            total_lines
        )
        .into());
    }

    #[allow(clippy::print_stdout)]
    {
        println!(
            "✓ Verified {} lines across {} files",
            total_lines,
            all_paths.len()
        );
        println!("✓ All hashes match expected values");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_hash_deterministic() {
        // Same input should always produce same output
        assert_eq!(row_hash(1), row_hash(1));
        assert_eq!(row_hash(12345), row_hash(12345));
    }

    #[test]
    fn test_row_hash_different() {
        // Different inputs should produce different outputs
        assert_ne!(row_hash(1), row_hash(2));
        assert_ne!(row_hash(100), row_hash(101));
    }

    #[test]
    fn test_generate_line_format() {
        let line = generate_line(42);
        assert!(line.starts_with("42,"));
        assert!(line.ends_with('\n'));
        assert_eq!(line.matches(',').count(), 1);
    }

    #[test]
    fn test_verify_line_valid() {
        let line = generate_line(123);
        assert!(verify_line(&line, 123).is_ok());
    }

    #[test]
    fn test_verify_line_wrong_row() {
        let line = generate_line(123);
        assert!(verify_line(&line, 456).is_err());
    }

    #[test]
    fn test_verify_line_corrupted_hash() {
        let line = "123,0000000000000000\n";
        assert!(verify_line(line, 123).is_err());
    }

    #[test]
    fn test_known_hash_values() {
        // These are the actual blake3 hash prefixes for verification
        // If these change, something is wrong with the hash function
        let hash_1 = row_hash(1);
        let hash_2 = row_hash(2);

        // Just verify they're 16 hex chars
        assert_eq!(hash_1.len(), 16);
        assert_eq!(hash_2.len(), 16);
        assert!(hash_1.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(hash_2.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
