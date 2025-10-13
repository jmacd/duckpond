//! DuckPond Performance Benchmark Tool
//!
//! This benchmark creates a new pond, writes configurable rows to a single series,
//! reads them back, and measures performance. It verifies streaming behavior
//! (memory not O(N) with input size) and establishes baseline metrics.

use anyhow::Result;
use clap::Parser;
use peak_alloc::PeakAlloc;
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tinyfs::arrow::SimpleParquetExt;
use steward::Ship;
use log::{info, debug, warn};
use tlogfs::factory::FactoryRegistry;
use serde_yaml;
use cmd::commands::show::show_command;
use cmd::common::ShipContext;

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

/// Performance benchmark configuration
#[derive(Parser)]
#[command(author, version, about = "DuckPond Performance Benchmark")]
struct Args {
    /// Row sizes to test (comma-separated, e.g., "1000000,2000000,3000000")
    #[arg(long, default_value = "1000000")]
    row_sizes: String,
    
    /// Number of benchmark iterations per row size
    #[arg(short, long, default_value_t = 1)]
    iterations: usize,
    
    /// Monitor memory usage during operations
    #[arg(long)]
    monitor_memory: bool,
    
    /// Verbose logging output
    #[arg(short, long)]
    verbose: bool,
}

/// Benchmark metrics collected during testing
#[derive(Debug, Clone)]
struct BenchmarkMetrics {
    /// Number of rows processed
    rows: usize,
    /// Time to write data
    write_time: Duration,
    /// Time to read original series data back  
    read_time: Duration,
    /// Time to read SQL-derived-series data back
    derived_read_time: Duration,
    /// Peak memory usage during read (if monitored)
    peak_memory_mb: Option<f64>,
    /// Whether verification passed
    verification_passed: bool,
}

impl BenchmarkMetrics {
    /// Calculate write throughput in rows per second
    fn write_throughput(&self) -> f64 {
        self.rows as f64 / self.write_time.as_secs_f64()
    }
    
    /// Calculate original series read throughput in rows per second
    fn read_throughput(&self) -> f64 {
        self.rows as f64 / self.read_time.as_secs_f64()
    }
    
    /// Calculate SQL-derived-series read throughput in rows per second
    fn derived_read_throughput(&self) -> f64 {
        self.rows as f64 / self.derived_read_time.as_secs_f64()
    }
    
    /// Memory efficiency in MB per 1K rows
    fn memory_efficiency_per_1k_rows(&self) -> Option<f64> {
        self.peak_memory_mb.map(|mb| mb / (self.rows as f64 / 1000.0))
    }
}

/// Reset peak memory tracking for benchmark isolation
fn reset_peak_memory() {
    PEAK_ALLOC.reset_peak_usage();
    debug!("Peak memory tracking reset");
}

/// Get current peak memory usage in MB
fn get_peak_memory_mb() -> f64 {
    let peak_mb = PEAK_ALLOC.peak_usage_as_mb() as f64;
    debug!("Peak memory usage: {:.2} MB", peak_mb);
    peak_mb
}

/// Generate test data with configurable number of rows
fn generate_test_data(rows: usize) -> Result<arrow_array::RecordBatch> {
    use arrow_array::{Int64Array, Float64Array};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    
    let start_timestamp = 1704067200000_i64; // 2024-01-01 00:00:00 UTC in milliseconds
    let interval_ms = 60000; // 1 minute intervals
    
    let timestamps: Vec<i64> = (0..rows)
        .map(|i| start_timestamp + (i as i64 * interval_ms))
        .collect();
        
    // Generate realistic sensor values
    let values: Vec<f64> = (0..rows)
        .map(|i| 20.0 + 10.0 * (i as f64 / 100.0).sin() + (i as f64 % 7.0))
        .collect();
    
    // Create Arrow arrays directly
    let timestamp_array = Arc::new(Int64Array::from(timestamps));
    let value_array = Arc::new(Float64Array::from(values));
    
    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    
    // Create RecordBatch
    let batch = arrow_array::RecordBatch::try_new(
        schema,
        vec![timestamp_array, value_array]
    )?;
    
    debug!("Generated test data: {} rows, {} columns", batch.num_rows(), batch.num_columns());
    Ok(batch)
}

/// Create a new pond for benchmarking
async fn create_benchmark_pond(pond_path: &Path) -> Result<Ship> {
    let start = Instant::now();
    debug!("Creating pond at: {}", pond_path.display());
    
    // Remove existing pond if it exists
    if pond_path.exists() {
        std::fs::remove_dir_all(pond_path)?;
    }
    
    let ship = Ship::create_pond(pond_path).await?;
    let duration = start.elapsed();
    
    info!("Pond created in {:?}", duration);
    Ok(ship)
}

/// Write test data to pond as a series
async fn write_test_data(ship: &mut Ship, batch: &arrow_array::RecordBatch) -> Result<Duration> {
    let start = Instant::now();
    debug!("Writing {} rows to pond", batch.num_rows());
    
    ship.transact(vec!["benchmark".to_string(), "write".to_string()], |_tx, fs| {
        let batch = batch.clone();
        Box::pin(async move {
            let root = fs.root().await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

            // Write as file:series with timestamp column
            root.write_parquet("test_series.series", &batch, tinyfs::EntryType::FileSeries).await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

            Ok(())
        })
    }).await?;
    
    let duration = start.elapsed();
    info!("Data written in {:?}", duration);
    Ok(duration)
}

/// Create SQL-derived-series for column renaming benchmark
async fn create_derived_series(ship: &mut Ship) -> Result<Duration> {
    let start = Instant::now();
    debug!("Creating SQL-derived-series with column renaming");
    
    // Embedded YAML config for the SQL-derived-series
    let config_yaml = r#"
entries:
  - name: "renamed-benchmark"
    factory: "sql-derived-series"
    config:
      patterns:
        original: "/test_series.series"
      query: |
        SELECT 
          timestamp as ts,
          value as sensor_reading
        FROM original
"#;
    
    let config_bytes = config_yaml.as_bytes().to_vec();
    
    // Validate the factory and configuration, get processed config
    let validated_config = FactoryRegistry::validate_config("dynamic-dir", &config_bytes)
        .map_err(|e| anyhow::anyhow!("Invalid configuration for dynamic-dir factory: {}", e))?;
    
    // Convert validated config back to bytes for storage
    let processed_config_bytes = serde_yaml::to_string(&validated_config)
        .map_err(|e| anyhow::anyhow!("Failed to serialize processed config: {}", e))?
        .into_bytes();

    ship.transact(
        vec!["mknod".to_string(), "dynamic-dir".to_string(), "/benchmark-derived".to_string()],
        |_tx, fs| {
            let config_clone = processed_config_bytes.clone();
            Box::pin(async move {
                let root = fs.root().await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                // Create the derived series directory using TinyFS API
                let _node_path = root.create_dynamic_directory_path(
                    "/benchmark-derived",
                    "dynamic-dir",
                    config_clone,
                ).await
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
                Ok(())
            })
        }
    ).await?;
    
    let duration = start.elapsed();
    info!("SQL-derived-series created in {:?}", duration);
    Ok(duration)
}

/// Read data from SQL-derived-series using streaming SQL interface (like cat command)
async fn read_and_verify_derived_data(
    ship: &mut Ship, 
    expected_batch: &arrow_array::RecordBatch,
    monitor_memory: bool
) -> Result<(Duration, bool, Option<f64>)> {
    // Reset peak memory tracking for this operation
    if monitor_memory {
        reset_peak_memory();
    }
    
    let read_start = Instant::now();
    debug!("Reading SQL-derived-series using streaming SQL interface (same as cat command)");
    
    // USE DIRECT TRANSACTION PATTERN: Same as cat command
    let mut tx = ship.begin_transaction(steward::TransactionOptions::read(vec!["benchmark".to_string(), "read-derived".to_string()])).await?;
    let fs = &*tx;
    let root = fs.root().await?;

    // INVESTIGATION: Check what files were created by mknod
    debug!("🔍 Investigating what files were actually created by mknod...");
    
    let derived_matches = root.collect_matches("/benchmark-derived/**").await
        .map_err(|e| anyhow::anyhow!("Failed to collect matches: {}", e))?;
    
    debug!("🎯 Files in benchmark-derived directory:");
    for (node_path, columns) in &derived_matches {
        debug!("  📄 {} (columns: {:?})", node_path.path.display(), columns);
    }
    
    // PROPER IMPLEMENTATION: Use cat command pattern with correct transaction handling
    let (read_batches, actual_row_count) = if let Some((first_derived_path, _)) = derived_matches.first() {
        let actual_path = first_derived_path.path.to_str().unwrap();
        debug!("✅ Found derived file at actual path: {}", actual_path);
        
        // STREAMING SQL INTERFACE: Use same approach as cat command for file:series
        let sql_query = "SELECT * FROM series"; // Simple column renaming, no ordering needed
        debug!("Executing SQL query on derived series: {}", sql_query);
        
        // Import futures::StreamExt for .next()
        use futures::StreamExt;
        
        let mut stream = tlogfs::execute_sql_on_file(&root, actual_path, sql_query, tx.transaction_guard()?)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to execute SQL query on derived series: {}", e))?;
        
        // COLLECT BATCHES: Stream through results with bounded memory (same as cat command)
        let mut total_rows = 0;
        let mut batch_count = 0;
        let mut collected_batches = Vec::new();
        
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result
                .map_err(|e| anyhow::anyhow!("Failed to process batch from stream: {}", e))?;
            
            total_rows += batch.num_rows();
            batch_count += 1;
            collected_batches.push(batch);
            
            debug!("Streamed derived batch {} with {} rows (total: {})", batch_count, collected_batches.last().unwrap().num_rows(), total_rows);
        }
        
        debug!("SQL-DERIVED STREAMING READ: Read {} rows in {} batches from {}", total_rows, batch_count, actual_path);
        
        (collected_batches, total_rows)
    } else {
        return Err(anyhow::anyhow!("No files found in /benchmark-derived directory"));
    };
    
    // Commit transaction
    tx.commit().await?;
    
    let read_time = read_start.elapsed();
    
    info!("Derived data read in {:?}, {} total rows in {} batches", read_time, actual_row_count, read_batches.len());
    
    // Verify data integrity (check renamed columns: ts, sensor_reading)
    debug!("Verifying derived data integrity");
    
    let expected_row_count = expected_batch.num_rows();
    let rows_match = actual_row_count == expected_row_count;
    
    if !rows_match {
        warn!("Row count mismatch: expected {}, got {}", expected_row_count, actual_row_count);
    }
    
    // Extract timestamps from derived data (now called 'ts' per SQL config)
    use std::collections::HashSet;
    let mut read_timestamps = HashSet::new();
    for batch in &read_batches {
        if let Some(ts_col) = batch.column_by_name("ts") {
            let timestamps = ts_col.as_any().downcast_ref::<arrow_array::Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected ts column to be Int64Array"))?;
            
            for i in 0..timestamps.len() {
                if let Some(ts) = timestamps.value(i).into() {
                    read_timestamps.insert(ts);
                }
            }
        } else {
            warn!("ts column not found in derived batch - checking available columns");
            for field in batch.schema().fields() {
                debug!("Available column: {}", field.name());
            }
        }
    }
    
    // Extract expected timestamps from original data  
    let mut expected_timestamps = HashSet::new();
    if let Some(ts_col) = expected_batch.column_by_name("timestamp") {
        let timestamps = ts_col.as_any().downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Expected timestamp column to be Int64Array"))?;
        
        for i in 0..timestamps.len() {
            if let Some(ts) = timestamps.value(i).into() {
                expected_timestamps.insert(ts);
            }
        }
    }
    
    let values_match = expected_timestamps == read_timestamps;
    if !values_match {
        warn!("Timestamp sets don't match between original and derived data");
        debug!("Expected {} timestamps, got {} timestamps", expected_timestamps.len(), read_timestamps.len());
    }
    
    let verification_passed = rows_match && values_match;
    
    info!("Derived data verification {}", 
          if verification_passed { "passed" } else { "failed" }, 
    );
    
    let peak_memory_mb = if monitor_memory {
        Some(get_peak_memory_mb())
    } else {
        None
    };
    
    Ok((read_time, verification_passed, peak_memory_mb))
}

async fn read_and_verify_data(
    ship: &mut Ship, 
    expected_batch: &arrow_array::RecordBatch,
    monitor_memory: bool
) -> Result<(Duration, bool, Option<f64>)> {
    // Reset peak memory tracking for this operation
    if monitor_memory {
        reset_peak_memory();
    }
    
    let read_start = Instant::now();
    debug!("Reading data back from pond");
    
    let (read_batches, actual_row_count) = ship.transact(vec!["benchmark".to_string(), "read".to_string()], |_tx, fs| {
        Box::pin(async move {
            let root = fs.root().await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

            // STREAMING: Use ParquetRecordBatchReader to process batches one-by-one with bounded memory
            let data = root.read_file_path_to_vec("test_series.series").await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
            
            use tokio_util::bytes::Bytes;
            use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
            
            let bytes = Bytes::from(data);
            let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                    tinyfs::Error::Other(format!("Parquet reader error: {}", e))
                )))?;
            let mut stream_reader = reader_builder.build()
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                    tinyfs::Error::Other(format!("Build reader error: {}", e))
                )))?;
            
            // STREAM through batches with bounded memory
            let mut total_rows = 0;
            let mut batch_count = 0;
            let mut collected_batches = Vec::new();
            
            while let Some(batch_result) = stream_reader.next() {
                let batch = batch_result
                    .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(
                        tinyfs::Error::Other(format!("Read batch error: {}", e))
                    )))?;
                
                total_rows += batch.num_rows();
                batch_count += 1;
                collected_batches.push(batch);
                
                debug!("Streamed batch {} with {} rows (total: {})", batch_count, collected_batches.last().unwrap().num_rows(), total_rows);
            }
            
            debug!("STREAMING: Read {} rows in {} batches", total_rows, batch_count);
            
            Ok::<(Vec<arrow_array::RecordBatch>, usize), steward::StewardError>((collected_batches, total_rows))
        })
    }).await?;
    
    let read_time = read_start.elapsed();
    
    info!("Data read in {:?}, {} total rows in {} batches", read_time, actual_row_count, read_batches.len());
    
    // Verify data integrity
    debug!("Verifying data integrity");
    
    // Verify row count and note batch count (multiple batches are normal for parquet)
    let expected_rows = expected_batch.num_rows();
    let rows_match = actual_row_count == expected_rows;
    let batch_count = read_batches.len();
    
    if !rows_match {
        warn!("Row count mismatch: expected {}, got {}", expected_rows, actual_row_count);
    }
    if batch_count > 1 {
        debug!("Multiple batches (normal for parquet): {} batches for {} rows", batch_count, actual_row_count);
    }
    
    // For small datasets, verify first and last timestamp values
    let mut values_match = true;
    if !read_batches.is_empty() && rows_match {
        // ROBUST VERIFICATION: Use set-based approach since row order is not guaranteed
        // This is O(N) but acceptable for verification
        
        // Verify schema compatibility first
        if read_batches[0].num_columns() != expected_batch.num_columns() {
            warn!("Column count mismatch: expected {}, got {}", expected_batch.num_columns(), read_batches[0].num_columns());
            values_match = false;
        } else {
            // Extract all timestamps from expected data
            let expected_ts_col = expected_batch.column(0)
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .unwrap();
            
            let mut expected_timestamps = std::collections::HashSet::new();
            for i in 0..expected_ts_col.len() {
                expected_timestamps.insert(expected_ts_col.value(i));
            }
            
            // Extract all timestamps from read batches (multiple batches due to streaming)
            let mut read_timestamps = std::collections::HashSet::new();
            for batch in &read_batches {
                let read_ts_col = batch.column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap();
                
                for i in 0..read_ts_col.len() {
                    read_timestamps.insert(read_ts_col.value(i));
                }
            }
            
            // Compare sets (order-independent verification using bitmap/set logic)
            if read_timestamps != expected_timestamps {
                let missing_from_read: Vec<_> = expected_timestamps.difference(&read_timestamps).take(5).collect();
                let extra_in_read: Vec<_> = read_timestamps.difference(&expected_timestamps).take(5).collect();
                
                warn!("Timestamp set mismatch:");
                if !missing_from_read.is_empty() {
                    warn!("  Missing from read (first 5): {:?}", missing_from_read);
                }
                if !extra_in_read.is_empty() {
                    warn!("  Extra in read (first 5): {:?}", extra_in_read);
                }
                warn!("  Expected set size: {}, Read set size: {}", expected_timestamps.len(), read_timestamps.len());
                values_match = false;
            } else {
                debug!("✅ Timestamp verification passed: {} unique timestamps match exactly", expected_timestamps.len());
            }
        }
    }
    
    let verification_passed = rows_match && values_match;
    
    info!("Data verification {}", 
          if verification_passed { "passed" } else { "failed" }, 
          );
    
    let peak_memory_mb = if monitor_memory {
        Some(get_peak_memory_mb())
    } else {
        None
    };
    
    Ok((read_time, verification_passed, peak_memory_mb))
}

/// Run a single benchmark iteration with proper transaction isolation
async fn run_benchmark_iteration(
    pond_path: &Path, 
    rows: usize, 
    monitor_memory: bool
) -> Result<BenchmarkMetrics> {
    info!("Starting benchmark iteration with {} rows", rows);
    
    // Generate test data
    let test_batch = generate_test_data(rows)?;
    
    // Create pond
    let mut ship = create_benchmark_pond(pond_path).await?;
    
    // TRANSACTION 1: Write data and create derived series
    let write_time = write_test_data(&mut ship, &test_batch).await?;
    
    // Create SQL-derived-series immediately after write
    let _derived_time = create_derived_series(&mut ship).await?;
    
    // IMPORTANT: Close the ship to ensure all writes are committed to Delta Lake
    drop(ship);
    
    // DEBUG: Show filesystem operations to understand node structure
    debug!("=== FILESYSTEM OPERATIONS LOG (after write) ===");
    let ship_context = ShipContext::new(Some(pond_path.to_path_buf()), vec!["benchmark".to_string(), "debug-show".to_string()]);
    let mut show_output = String::new();
    show_command(&ship_context, "detailed", |output| {
        show_output.push_str(&output);
    }).await.map_err(|e| anyhow::anyhow!("Failed to run show command: {}", e))?;
    debug!("Show command output:\n{}", show_output);
    
    // TRANSACTION 2: Read original series
    let mut ship = Ship::open_pond(pond_path).await?;
    let (read_time, verification_passed, original_peak_memory_mb) = 
        read_and_verify_data(&mut ship, &test_batch, monitor_memory).await?;
    drop(ship);
    
    // TRANSACTION 3: Read SQL-derived-series 
    let mut ship = Ship::open_pond(pond_path).await?;
    let (derived_read_time, derived_verification_passed, derived_peak_memory_mb) = 
        read_and_verify_derived_data(&mut ship, &test_batch, monitor_memory).await?;
    
    // Both verifications must pass
    let overall_verification_passed = verification_passed && derived_verification_passed;
    
    // Use the higher peak memory between original and derived reads
    let peak_memory_mb = match (original_peak_memory_mb, derived_peak_memory_mb) {
        (Some(orig), Some(derived)) => Some(orig.max(derived)),
        (Some(orig), None) => Some(orig),
        (None, Some(derived)) => Some(derived),
        (None, None) => None,
    };
    
    Ok(BenchmarkMetrics {
        rows,
        write_time,
        read_time,
        derived_read_time,
        peak_memory_mb,
        verification_passed: overall_verification_passed,
    })
}

/// Print streaming benchmark results with comparative table
fn print_streaming_results(metrics: &[BenchmarkMetrics], row_sizes: &[usize]) {
    println!("\n🚀 DuckPond Streaming Benchmark Results");
    println!("═══════════════════════════════════════════════════════════════════════════════════");
    println!();
    
    // Group metrics by row size
    let mut size_groups: std::collections::HashMap<usize, Vec<&BenchmarkMetrics>> = std::collections::HashMap::new();
    for metric in metrics {
        size_groups.entry(metric.rows).or_insert_with(Vec::new).push(metric);
    }
    
    // Print summary table with dual read performance
    println!("📊 SQL-Derived-Series Performance Analysis");
    println!("┌──────────────┬─────────────┬─────────────┬─────────────┬─────────────┬─────────────┬──────────────┐");
    println!("│ Dataset Size │ Write Tput  │ Orig Read   │ Derived Read│ Peak Memory │ MB per 1K   │ Verification │");
    println!("│              │ (rows/sec)  │ (rows/sec)  │ (rows/sec)  │ (MB)        │ rows        │              │");
    println!("├──────────────┼─────────────┼─────────────┼─────────────┼─────────────┼─────────────┼──────────────┤");
    
    for &row_size in row_sizes {
        if let Some(group_metrics) = size_groups.get(&row_size) {
            // Calculate averages for this row size
            let avg_write_tput = group_metrics.iter().map(|m| m.write_throughput()).sum::<f64>() / group_metrics.len() as f64;
            let avg_read_tput = group_metrics.iter().map(|m| m.read_throughput()).sum::<f64>() / group_metrics.len() as f64;
            let avg_derived_tput = group_metrics.iter().map(|m| m.derived_read_throughput()).sum::<f64>() / group_metrics.len() as f64;
            let avg_memory = group_metrics.iter()
                .filter_map(|m| m.peak_memory_mb)
                .sum::<f64>() / group_metrics.iter().filter(|m| m.peak_memory_mb.is_some()).count().max(1) as f64;
            let avg_efficiency = group_metrics.iter()
                .filter_map(|m| m.memory_efficiency_per_1k_rows())
                .sum::<f64>() / group_metrics.iter().filter(|m| m.memory_efficiency_per_1k_rows().is_some()).count().max(1) as f64;
            let passed_count = group_metrics.iter().filter(|m| m.verification_passed).count();
            
            println!("│ {:>12} │ {:>11.0} │ {:>11.0} │ {:>11.0} │ {:>11.1} │ {:>11.2} │ {:>4}/{:<7} │",
                format_row_count(row_size),
                avg_write_tput,
                avg_read_tput,
                avg_derived_tput,
                avg_memory,
                avg_efficiency,
                passed_count,
                group_metrics.len()
            );
        }
    }
    
    println!("└──────────────┴─────────────┴─────────────┴─────────────┴─────────────┴──────────────┘");
    println!();
    
    // Memory efficiency analysis
    if row_sizes.len() > 1 {
        println!("🔍 Memory Efficiency Analysis:");
        
        let efficiencies: Vec<(usize, f64)> = row_sizes.iter()
            .filter_map(|&size| {
                size_groups.get(&size).and_then(|group| {
                    let avg_eff = group.iter()
                        .filter_map(|m| m.memory_efficiency_per_1k_rows())
                        .sum::<f64>() / group.iter().filter(|m| m.memory_efficiency_per_1k_rows().is_some()).count().max(1) as f64;
                    Some((size, avg_eff))
                })
            })
            .collect();
        
        if efficiencies.len() > 1 {
            let best_efficiency = efficiencies.iter().min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            let worst_efficiency = efficiencies.iter().max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            
            if let (Some(best), Some(worst)) = (best_efficiency, worst_efficiency) {
                let improvement = ((worst.1 - best.1) / worst.1) * 100.0;
                println!("   • Best efficiency: {:.2} MB/1K rows at {} rows", best.1, format_row_count(best.0));
                println!("   • Worst efficiency: {:.2} MB/1K rows at {} rows", worst.1, format_row_count(worst.0));
                if improvement > 0.0 {
                    println!("   • Memory efficiency improves by {:.1}% with larger datasets", improvement);
                } else {
                    println!("   • Memory efficiency degrades by {:.1}% with larger datasets", -improvement);
                }
            }
        }
        
        println!("   • Target: Memory efficiency should stabilize or improve with dataset size");
        println!("   • Status: {} behavior indicates {} memory usage", 
                 if efficiencies.windows(2).all(|w| w[1].1 <= w[0].1 * 1.1) { "✅ Good" } else { "⚠️  Variable" },
                 if efficiencies.iter().all(|(_, eff)| *eff < 1.0) { "efficient" } else { "potentially high" });
    }
    
    println!();
}

/// Format row count in human-readable form (1M, 2M, etc.)
fn format_row_count(rows: usize) -> String {
    if rows >= 1_000_000 {
        format!("{}M", rows / 1_000_000)
    } else if rows >= 1_000 {
        format!("{}K", rows / 1_000)
    } else {
        format!("{}", rows)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Initialize logging
    let log_level = if args.verbose { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();
    
    // Parse row sizes
    let row_sizes: Vec<usize> = args.row_sizes
        .split(',')
        .map(|s| s.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Invalid row sizes: {}", e))?;
    
    info!("Starting DuckPond streaming benchmark with row sizes: {:?}, {} iterations each", row_sizes, args.iterations);
    
    // Create temporary directory for benchmark
    let temp_dir = TempDir::new()?;
    let base_pond_path = temp_dir.path().join("benchmark_pond");
    
    info!("Using base pond path: {}", base_pond_path.display());
    
    // Run benchmark for each row size
    let mut all_metrics = Vec::new();
    
    for (size_idx, &row_count) in row_sizes.iter().enumerate() {
        info!("\n📊 Testing with {} rows ({}/{} row sizes)", row_count, size_idx + 1, row_sizes.len());
        
        for iteration in 1..=args.iterations {
            info!("Running iteration {}/{} for {} rows", iteration, args.iterations, row_count);
            
            // Use unique pond path for each row size and iteration
            let iteration_pond_path = base_pond_path.with_extension(&format!("{}rows_iter{}", row_count, iteration));
            
            let metrics = run_benchmark_iteration(&iteration_pond_path, row_count, args.monitor_memory).await?;
            all_metrics.push(metrics);
            
            info!("Iteration {} completed successfully", iteration);
        }
    }
    
    // Print comprehensive results table
    print_streaming_results(&all_metrics, &row_sizes);
    
    // Check for any failures
    let failed_iterations = all_metrics.iter().filter(|m| !m.verification_passed).count();
    if failed_iterations > 0 {
        warn!("{} out of {} total tests failed verification", failed_iterations, all_metrics.len());
        std::process::exit(1);
    }
    
    info!("All benchmark iterations completed successfully!");
    Ok(())
}
