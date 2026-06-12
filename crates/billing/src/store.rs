// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Reference-table read/write helpers.
//!
//! Reference tables (`*.parquet` in `/data/`) are small and fully rewritten
//! on every mutation. Reading an absent table yields `Vec::new()`; writing
//! always overwrites via `WD::async_writer_path_with_type` with
//! `EntryType::TablePhysicalVersion` (single-version-per-file semantics).

use serde::{Deserialize, Serialize};
use tinyfs::ResultExt;
use tinyfs::arrow::ForArrow;
use tinyfs::{EntryType, Result, WD};
use tokio::io::AsyncWriteExt;

/// Read all rows from a reference table. Returns `Ok(vec![])` if the file
/// (or any of its parent directories) does not yet exist -- the operator
/// hasn't touched that table. Also returns empty if the file exists but
/// has no row groups (which is what tinyfs produces when we write a
/// 0-row batch -- e.g., after `bills reverse` removes the last row).
pub async fn read_table<T>(wd: &WD, path: &str) -> Result<Vec<T>>
where
    T: ForArrow + for<'de> Deserialize<'de> + Send + Sync,
{
    use tinyfs::arrow::ParquetExt;
    match wd.read_table_as_items::<T, _>(path).await {
        Ok(items) => Ok(items),
        Err(tinyfs::Error::NotFound(_)) => Ok(Vec::new()),
        Err(tinyfs::Error::Other(msg)) if msg.contains("No data in parquet file") => Ok(Vec::new()),
        Err(e) => Err(e),
    }
}

/// Overwrite a reference table with the given rows. Creates the file if
/// missing; replaces its contents if present (by removing then recreating,
/// so the file always shows up as version 1 in the underlying storage --
/// avoids the multi-version concat behavior that DataFusion / `pond cat`
/// uses for series files).
pub async fn write_table<T>(wd: &WD, path: &str, items: &[T]) -> Result<()>
where
    T: ForArrow + Serialize + Send + Sync,
{
    let fields = T::for_arrow();
    let batch = serde_arrow::to_record_batch::<&[T]>(&fields, &items)
        .map_other_context(format!("serialize {path}"))?;

    let buffer = serialize_batch_to_parquet(&batch)?;

    // Remove the existing file (if any) so the recreated file is v1.
    // Without this, every write creates a new version and DataFusion
    // queries see all versions concatenated -- duplicate rows.
    remove_if_exists(wd, path).await?;

    let mut writer = wd
        .async_writer_path_with_type(path, EntryType::TablePhysicalVersion)
        .await?;
    writer
        .write_all(&buffer)
        .await
        .map_other_context(format!("write {path}"))?;
    writer
        .shutdown()
        .await
        .map_other_context(format!("shutdown {path}"))?;
    Ok(())
}

async fn remove_if_exists(wd: &WD, path: &str) -> Result<()> {
    let p = std::path::Path::new(path);
    let parent = p
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    let name = p
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| tinyfs::Error::Other(format!("invalid path {path}")))?;

    // If the parent directory doesn't exist, the file can't exist either.
    let parent_wd = match wd.open_dir_path(&parent).await {
        Ok(p) => p,
        Err(tinyfs::Error::NotFound(_)) => return Ok(()),
        Err(e) => return Err(e),
    };
    match parent_wd.remove_entry(&name).await {
        Ok(()) => Ok(()),
        Err(tinyfs::Error::NotFound(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Read **all versions** of a series file and concatenate the rows in
/// chronological version order. Returns `Ok(vec![])` if the file does not
/// yet exist.
///
/// Each `expense add` / `payment add` / `journal write_transaction` writes
/// a new version (one batch per version). This helper unions them in the
/// order they were written -- the same order the operator created them.
pub async fn read_series<T>(wd: &WD, path: &str) -> Result<Vec<T>>
where
    T: ForArrow + for<'de> Deserialize<'de> + Send + Sync,
{
    let versions = match wd.list_file_versions(path).await {
        Ok(v) => v,
        Err(tinyfs::Error::NotFound(_)) => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut all = Vec::new();
    let fields = T::for_arrow();
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields.clone()));
    for v in versions {
        let bytes = wd.read_file_version(path, v.version).await?;
        let batch = parse_parquet_batches(bytes)?;
        for b in batch {
            // serde_arrow expects the batch's schema to match T::for_arrow().
            // Project columns by name to be tolerant of column-order
            // differences across versions.
            let projected = project_to_schema(&b, &schema)?;
            let items: Vec<T> = serde_arrow::from_record_batch(&projected)
                .map_other_context(format!("deserialize {path}"))?;
            all.extend(items);
        }
    }
    Ok(all)
}

/// Write a new version to a series file. Creates the file (with
/// `EntryType::TablePhysicalSeries`) on first call, appends a version on
/// subsequent calls. Temporal bounds are extracted from `timestamp_column`
/// in the batch.
pub async fn write_series<T>(wd: &WD, path: &str, items: &[T], timestamp_column: &str) -> Result<()>
where
    T: ForArrow + Serialize + Send + Sync,
{
    use tinyfs::arrow::ParquetExt;
    let fields = T::for_arrow();
    let batch = serde_arrow::to_record_batch::<&[T]>(&fields, &items)
        .map_other_context(format!("serialize {path}"))?;
    let _ = wd
        .write_series_from_batch(path, &batch, Some(timestamp_column))
        .await?;
    Ok(())
}

/// Allocate the next id as `max(id_fn(row) for row in items) + 1`, starting
/// at 1 if the table is empty.
pub fn next_id<T>(items: &[T], id_fn: impl Fn(&T) -> i32) -> i32 {
    items.iter().map(id_fn).max().unwrap_or(0) + 1
}

// ---------------------------------------------------------------------------
// Parquet plumbing
// ---------------------------------------------------------------------------

fn serialize_batch_to_parquet(batch: &arrow_array::RecordBatch) -> Result<Vec<u8>> {
    use parquet::arrow::ArrowWriter;
    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None)
            .map_other_context("ArrowWriter::try_new")?;
        writer
            .write(batch)
            .map_other_context("ArrowWriter::write")?;
        let _ = writer.close().map_other_context("ArrowWriter::close")?;
    }
    Ok(buffer)
}

fn parse_parquet_batches(bytes: Vec<u8>) -> Result<Vec<arrow_array::RecordBatch>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tokio_util::bytes::Bytes;
    let bytes = Bytes::from(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_other_context("ParquetReader::try_new")?
        .build()
        .map_other_context("ParquetReader::build")?;
    let mut out = Vec::new();
    for b in reader {
        let b = b.map_other_context("Parquet batch")?;
        out.push(b);
    }
    Ok(out)
}

/// Project a RecordBatch to the columns named in `target_schema`, in
/// order. Errors if any required column is missing. This makes the
/// series-read path tolerant of historical schema variants where columns
/// might be reordered.
fn project_to_schema(
    batch: &arrow_array::RecordBatch,
    target_schema: &arrow_schema::Schema,
) -> Result<arrow_array::RecordBatch> {
    use arrow_array::RecordBatch;
    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        let idx = batch
            .schema()
            .index_of(field.name())
            .map_err(|_| tinyfs::Error::Other(format!("missing column `{}`", field.name())))?;
        columns.push(batch.column(idx).clone());
    }
    RecordBatch::try_new(std::sync::Arc::new(target_schema.clone()), columns)
        .map_other_context("project_to_schema")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::CustomerRow;

    #[tokio::test]
    async fn read_absent_table_yields_empty() {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let rows: Vec<CustomerRow> = read_table(&wd, "/data/customers.parquet")
            .await
            .expect("read");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn roundtrip_overwrite() {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");

        let v1 = vec![CustomerRow {
            customer_id: 1,
            name: "Alice".into(),
            billing_address: "addr1".into(),
            contact: None,
            active: true,
            notes: None,
            merged_into_customer_id: None,
        }];
        write_table(&wd, "/data/customers.parquet", &v1)
            .await
            .expect("write v1");

        let read1: Vec<CustomerRow> = read_table(&wd, "/data/customers.parquet")
            .await
            .expect("read v1");
        assert_eq!(read1, v1);

        // Overwrite with two rows.
        let v2 = vec![
            v1[0].clone(),
            CustomerRow {
                customer_id: 2,
                name: "Bob".into(),
                billing_address: "addr2".into(),
                contact: Some("bob@example.com".into()),
                active: true,
                notes: None,
                merged_into_customer_id: None,
            },
        ];
        write_table(&wd, "/data/customers.parquet", &v2)
            .await
            .expect("write v2");

        let read2: Vec<CustomerRow> = read_table(&wd, "/data/customers.parquet")
            .await
            .expect("read v2");
        assert_eq!(read2, v2);
    }

    #[test]
    fn next_id_empty_returns_one() {
        let items: Vec<CustomerRow> = vec![];
        assert_eq!(next_id(&items, |c| c.customer_id), 1);
    }

    #[test]
    fn next_id_returns_max_plus_one() {
        let items = vec![
            CustomerRow {
                customer_id: 5,
                name: "a".into(),
                billing_address: "x".into(),
                contact: None,
                active: true,
                notes: None,
                merged_into_customer_id: None,
            },
            CustomerRow {
                customer_id: 12,
                name: "b".into(),
                billing_address: "y".into(),
                contact: None,
                active: true,
                notes: None,
                merged_into_customer_id: None,
            },
            CustomerRow {
                customer_id: 7,
                name: "c".into(),
                billing_address: "z".into(),
                contact: None,
                active: true,
                notes: None,
                merged_into_customer_id: None,
            },
        ];
        assert_eq!(next_id(&items, |c| c.customer_id), 13);
    }

    use crate::schema::ExpenseRow;

    #[tokio::test]
    async fn read_absent_series_yields_empty() {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let rows: Vec<ExpenseRow> = read_series(&wd, "/data/expenses.series")
            .await
            .expect("read");
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn series_versions_concatenate_in_order() {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        let _ = wd.create_dir_all("/data").await.expect("mkdir");

        let v1 = vec![ExpenseRow {
            expense_id: 1,
            paid_date: 1_700_000_000_000_000,
            account_code: 5100,
            vendor: Some("ChemCo".into()),
            amount_cents: 12345,
            memo: None,
            cycle_id: Some(1),
            void_of: None,
        }];
        write_series(&wd, "/data/expenses.series", &v1, "paid_date")
            .await
            .expect("write v1");

        let v2 = vec![ExpenseRow {
            expense_id: 2,
            paid_date: 1_700_000_010_000_000,
            account_code: 5200,
            vendor: Some("PG&E".into()),
            amount_cents: 6789,
            memo: None,
            cycle_id: Some(1),
            void_of: None,
        }];
        write_series(&wd, "/data/expenses.series", &v2, "paid_date")
            .await
            .expect("write v2");

        let all: Vec<ExpenseRow> = read_series(&wd, "/data/expenses.series")
            .await
            .expect("read");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].expense_id, 1);
        assert_eq!(all[1].expense_id, 2);
    }
}
