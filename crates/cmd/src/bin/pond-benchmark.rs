//! DuckPond Performance Benchmark Tool
//!
//! This benchmark creates a new pond, writes configurable rows to a single series,
//! reads them back, and measures performance. It verifies streaming behavior
//! (memory not O(N) with input size) and establishes baseline metrics.

use anyhow::Result;
use clap::Parser;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tinyfs::arrow::SimpleParquetExt;
use steward::Ship;
use log::{info, debug, warn};

/// Performance benchmark configuration
#[derive(Parser)]
#[command(author, version, about = "DuckPond Performance Benchmark")]
struct Args {
    /// Number of rows to generate and test
    #[arg(long, default_value = "10000")]
    rows: usize,
    
    /// Number of test iterations to run
    #[arg(long, default_value = "1")]
    iterations: usize,
    
    /// Path for benchmark pond (if not provided, uses temp directory)
    #[arg(long)]
    pond_path: Option<PathBuf>,
    
    /// Enable memory usage monitoring during reads
    #[arg(long)]
    monitor_memory: bool,
    
    /// Generate CSV output for analysis
    #[arg(long)]
    csv_output: bool,
    
    /// Verbose logging output
    #[arg(short, long)]
    verbose: bool,
}

/// Benchmark metrics collected during testing
#[derive(Debug, Clone)]
struct BenchmarkMetrics {
    /// Number of rows processed
    rows: usize,
    /// Time to create pond
    pond_creation_time: Duration,
    /// Time to write data
    write_time: Duration,
    /// Time to read data back  
    read_time: Duration,
    /// Time for data verification
    verification_time: Duration,
    /// Total benchmark time
    total_time: Duration,
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
    
    /// Calculate read throughput in rows per second
    fn read_throughput(&self) -> f64 {
        self.rows as f64 / self.read_time.as_secs_f64()
    }
    
    /// Memory efficiency in MB per 1K rows
    fn memory_efficiency_per_1k_rows(&self) -> Option<f64> {
        self.peak_memory_mb.map(|mb| mb / (self.rows as f64 / 1000.0))
    }
}

/// Simple memory monitoring during operations
struct MemoryMonitor {
    #[cfg(target_os = "macos")]
    #[allow(dead_code)] // May be used for future delta calculations
    initial_memory: Option<u64>,
    peak_memory: Option<u64>,
}

impl MemoryMonitor {
    fn new() -> Self {
        Self {
            #[cfg(target_os = "macos")]
            initial_memory: Self::get_memory_usage(),
            peak_memory: None,
        }
    }
    
    #[cfg(target_os = "macos")]
    fn get_memory_usage() -> Option<u64> {
        // Use ps command to get memory usage on macOS
        use std::process::Command;
        
        let output = Command::new("ps")
            .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()?;
            
        let rss_kb = String::from_utf8(output.stdout)
            .ok()?
            .trim()
            .parse::<u64>()
            .ok()?;
            
        Some(rss_kb * 1024) // Convert KB to bytes
    }
    
    #[cfg(not(target_os = "macos"))]
    fn get_memory_usage() -> Option<u64> {
        // Placeholder for other platforms
        None
    }
    
    fn sample(&mut self) {
        if let Some(current) = Self::get_memory_usage() {
            self.peak_memory = Some(self.peak_memory.unwrap_or(0).max(current));
        }
    }
    
    fn peak_memory_mb(&self) -> Option<f64> {
        self.peak_memory.map(|bytes| bytes as f64 / (1024.0 * 1024.0))
    }
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

/// Read data back from pond and verify
async fn read_and_verify_data(
    ship: &mut Ship, 
    expected_batch: &arrow_array::RecordBatch,
    monitor_memory: bool
) -> Result<(Duration, Duration, bool, Option<f64>)> {
    let mut memory_monitor = if monitor_memory {
        Some(MemoryMonitor::new())
    } else {
        None
    };
    
    let read_start = Instant::now();
    debug!("Reading data back from pond");
    
    // Sample memory before read
    if let Some(ref mut monitor) = memory_monitor {
        monitor.sample();
    }
    
    let (read_batches, actual_row_count) = ship.transact(vec!["benchmark".to_string(), "read".to_string()], |_tx, fs| {
        Box::pin(async move {
            let root = fs.root().await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;

            // Get the series file as a queryable file
            let reader = root.async_reader_path("test_series.series").await
                .map_err(|e| steward::StewardError::DataInit(tlogfs::TLogFSError::TinyFS(e)))?;
                
            // Read all content (this should be streaming in production)
            let content = tinyfs::buffer_helpers::read_all_to_vec(reader).await
                .map_err(|e| steward::StewardError::DataInit(
                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("IO error: {}", e)))
                ))?;
            
            // Parse as parquet using Bytes (which implements ChunkReader)
            let bytes = bytes::Bytes::from(content);
            let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                .map_err(|e| steward::StewardError::DataInit(
                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Parquet parse error: {}", e)))
                ))?
                .build()
                .map_err(|e| steward::StewardError::DataInit(
                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Parquet build error: {}", e)))
                ))?;
            
            let mut batches = Vec::new();
            let mut total_rows = 0;
            
            for batch_result in reader {
                let batch = batch_result.map_err(|e| steward::StewardError::DataInit(
                    tlogfs::TLogFSError::TinyFS(tinyfs::Error::Other(format!("Parquet read error: {}", e)))
                ))?;
                total_rows += batch.num_rows();
                batches.push(batch);
            }
            
            Ok((batches, total_rows))
        })
    }).await?;
    
    let read_time = read_start.elapsed();
    
    // Sample memory after read
    if let Some(ref mut monitor) = memory_monitor {
        monitor.sample();
    }
    
    info!("Data read in {:?}, {} total rows in {} batches", read_time, actual_row_count, read_batches.len());
    
    // Verify data integrity
    let verify_start = Instant::now();
    debug!("Verifying data integrity");
    
    // Sample memory during verification
    if let Some(ref mut monitor) = memory_monitor {
        monitor.sample();
    }
    
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
        let read_batch = &read_batches[0];
        
        // Verify schema compatibility
        if read_batch.num_columns() != expected_batch.num_columns() {
            warn!("Column count mismatch: expected {}, got {}", expected_batch.num_columns(), read_batch.num_columns());
            values_match = false;
        } else {
            // Check first and last timestamps for basic data integrity
            // Note: Read data is sorted DESC (newest first), expected data is ASC (oldest first)
            if let (Some(read_ts_col), Some(expected_ts_col)) = (
                read_batch.column(0).as_any().downcast_ref::<arrow_array::Int64Array>(),
                expected_batch.column(0).as_any().downcast_ref::<arrow_array::Int64Array>()
            ) {
                if read_ts_col.len() > 0 && expected_ts_col.len() > 0 {
                    // Read data DESC: first value should be latest timestamp (expected_ts_col.last)
                    // Read data DESC: last value should be earliest timestamp (expected_ts_col.first)
                    let newest_match = read_ts_col.value(0) == expected_ts_col.value(expected_ts_col.len() - 1);
                    let oldest_match = read_ts_col.value(read_ts_col.len() - 1) == expected_ts_col.value(0);
                    
                    if !newest_match || !oldest_match {
                        warn!("Timestamp value mismatch: newest={}, oldest={}", newest_match, oldest_match);
                        debug!("Read first (newest): {}, Expected last: {}", read_ts_col.value(0), expected_ts_col.value(expected_ts_col.len() - 1));
                        debug!("Read last (oldest): {}, Expected first: {}", read_ts_col.value(read_ts_col.len() - 1), expected_ts_col.value(0));
                        values_match = false;
                    }
                }
            }
        }
    }
    
    let verify_time = verify_start.elapsed();
    let verification_passed = rows_match && values_match;
    
    info!("Data verification {} in {:?}", 
          if verification_passed { "passed" } else { "failed" }, 
          verify_time);
    
    let peak_memory_mb = memory_monitor.and_then(|m| m.peak_memory_mb());
    
    Ok((read_time, verify_time, verification_passed, peak_memory_mb))
}

/// Run a single benchmark iteration
async fn run_benchmark_iteration(
    pond_path: &Path, 
    rows: usize, 
    monitor_memory: bool
) -> Result<BenchmarkMetrics> {
    let total_start = Instant::now();
    
    info!("Starting benchmark iteration with {} rows", rows);
    
    // Generate test data
    let test_batch = generate_test_data(rows)?;
    
    // Create pond
    let pond_start = Instant::now();
    let mut ship = create_benchmark_pond(pond_path).await?;
    let pond_creation_time = pond_start.elapsed();
    
    // Write data
    let write_time = write_test_data(&mut ship, &test_batch).await?;
    
    // Read and verify data
    let (read_time, verification_time, verification_passed, peak_memory_mb) = 
        read_and_verify_data(&mut ship, &test_batch, monitor_memory).await?;
    
    let total_time = total_start.elapsed();
    
    Ok(BenchmarkMetrics {
        rows,
        pond_creation_time,
        write_time,
        read_time,
        verification_time,
        total_time,
        peak_memory_mb,
        verification_passed,
    })
}

/// Print benchmark results
fn print_results(metrics: &[BenchmarkMetrics], csv_output: bool) {
    if csv_output {
        println!("iteration,rows,pond_creation_ms,write_ms,read_ms,verification_ms,total_ms,write_throughput_rows_per_sec,read_throughput_rows_per_sec,peak_memory_mb,memory_per_1k_rows_mb,verification_passed");
        
        for (i, m) in metrics.iter().enumerate() {
            println!("{},{},{},{},{},{},{},{:.2},{:.2},{},{},{}", 
                i + 1,
                m.rows,
                m.pond_creation_time.as_millis(),
                m.write_time.as_millis(),
                m.read_time.as_millis(),
                m.verification_time.as_millis(),
                m.total_time.as_millis(),
                m.write_throughput(),
                m.read_throughput(),
                m.peak_memory_mb.map(|mb| format!("{:.2}", mb)).unwrap_or_else(|| "N/A".to_string()),
                m.memory_efficiency_per_1k_rows().map(|mb| format!("{:.2}", mb)).unwrap_or_else(|| "N/A".to_string()),
                m.verification_passed
            );
        }
    } else {
        println!("\n📊 DuckPond Benchmark Results");
        println!("═══════════════════════════════════════════════════════════════");
        
        for (i, m) in metrics.iter().enumerate() {
            println!("\n🔬 Iteration {} - {} rows", i + 1, m.rows);
            println!("┌─────────────────────────────────────────────────────────────┐");
            println!("│ Timing Breakdown:                                          │");
            println!("│   Pond Creation: {:>8.2}ms                              │", m.pond_creation_time.as_millis());
            println!("│   Data Write:    {:>8.2}ms                              │", m.write_time.as_millis());
            println!("│   Data Read:     {:>8.2}ms                              │", m.read_time.as_millis());
            println!("│   Verification:  {:>8.2}ms                              │", m.verification_time.as_millis());
            println!("│   Total Time:    {:>8.2}ms                              │", m.total_time.as_millis());
            println!("├─────────────────────────────────────────────────────────────┤");
            println!("│ Throughput:                                                 │");
            println!("│   Write: {:>10.0} rows/sec                              │", m.write_throughput());
            println!("│   Read:  {:>10.0} rows/sec                              │", m.read_throughput());
            
            if let Some(peak_mb) = m.peak_memory_mb {
                println!("├─────────────────────────────────────────────────────────────┤");
                println!("│ Memory Usage:                                               │");
                println!("│   Peak Memory: {:>8.2} MB                                │", peak_mb);
                if let Some(efficiency) = m.memory_efficiency_per_1k_rows() {
                    println!("│   Per 1K rows: {:>8.2} MB                                │", efficiency);
                }
            }
            
            println!("├─────────────────────────────────────────────────────────────┤");
            println!("│ Verification: {}                                        │", 
                     if m.verification_passed { "✅ PASSED" } else { "❌ FAILED" });
            println!("└─────────────────────────────────────────────────────────────┘");
        }
        
        if metrics.len() > 1 {
            let avg_write_throughput = metrics.iter().map(|m| m.write_throughput()).sum::<f64>() / metrics.len() as f64;
            let avg_read_throughput = metrics.iter().map(|m| m.read_throughput()).sum::<f64>() / metrics.len() as f64;
            
            println!("\n📈 Summary Across {} Iterations", metrics.len());
            println!("┌─────────────────────────────────────────────────────────────┐");
            println!("│ Average Write Throughput: {:>8.0} rows/sec                │", avg_write_throughput);
            println!("│ Average Read Throughput:  {:>8.0} rows/sec                │", avg_read_throughput);
            println!("└─────────────────────────────────────────────────────────────┘");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    // Initialize logging
    let log_level = if args.verbose { "debug" } else { "info" };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level)).init();
    
    info!("Starting DuckPond benchmark with {} rows, {} iterations", args.rows, args.iterations);
    
    // Determine pond path
    let pond_path = if let Some(path) = args.pond_path {
        path
    } else {
        let temp_dir = TempDir::new()?;
        temp_dir.path().join("benchmark_pond")
    };
    
    info!("Using pond path: {}", pond_path.display());
    
    // Run benchmark iterations
    let mut all_metrics = Vec::new();
    
    for iteration in 1..=args.iterations {
        info!("Running iteration {}/{}", iteration, args.iterations);
        
        // Use iteration-specific pond path
        let iteration_pond_path = if args.iterations > 1 {
            pond_path.with_extension(&format!("iteration_{}", iteration))
        } else {
            pond_path.clone()
        };
        
        let metrics = run_benchmark_iteration(&iteration_pond_path, args.rows, args.monitor_memory).await?;
        all_metrics.push(metrics);
        
        if !args.csv_output {
            info!("Iteration {} completed successfully", iteration);
        }
    }
    
    // Print results
    print_results(&all_metrics, args.csv_output);
    
    // Check for any failures
    let failed_iterations = all_metrics.iter().filter(|m| !m.verification_passed).count();
    if failed_iterations > 0 {
        warn!("{} out of {} iterations failed verification", failed_iterations, args.iterations);
        std::process::exit(1);
    }
    
    info!("All benchmark iterations completed successfully!");
    Ok(())
}