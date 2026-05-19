// SPDX-License-Identifier: Apache-2.0

//! Tests for the remote table schema, row round-trips, and the
//! milestone-critical assumption that delta-rs commits a single batch
//! containing rows for multiple partition values atomically.

use deltalake::DeltaTable;
use deltalake::protocol::SaveMode;
use sync_remote::schema::{
    BLAKE3_LEN, CHUNK_SIZE_BYTES, PARTITION_KIND_CHECKSUM, PARTITION_KIND_DATA,
    PARTITION_KIND_MANIFEST, RemoteRow, RowBody, arrow_schema, delta_columns, partition_columns,
    record_batch_to_rows, rows_to_record_batch,
};
use sync_steward::CommitKind;
use sync_store::checksum::ChecksumKind;
use tempfile::TempDir;
use url::Url;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

fn b3(seed: u8) -> [u8; BLAKE3_LEN] {
    let mut a = [0u8; BLAKE3_LEN];
    a[0] = seed;
    a
}

fn manifest_row(seq: i64, kind: CommitKind, parent_seq: i64) -> RemoteRow {
    RemoteRow {
        txn_seq: seq,
        ts_micros: 1_700_000_000_000_000,
        body: RowBody::Manifest {
            commit_kind: kind,
            parent_seq,
        },
    }
}

fn checksum_row(seq: i64, partition_key: &str, kind: ChecksumKind, seed: u8) -> RemoteRow {
    RemoteRow {
        txn_seq: seq,
        ts_micros: 1_700_000_000_000_000,
        body: RowBody::Checksum {
            partition_key: partition_key.into(),
            checksum_kind: kind,
            checksum_bytes: b3(seed),
        },
    }
}

fn data_add_row(
    seq: i64,
    path: &str,
    chunk_id: i64,
    chunk_count: i64,
    payload: Vec<u8>,
) -> RemoteRow {
    let chunk_blake3 = *blake3::hash(&payload).as_bytes();
    let file_blake3 = *blake3::hash(b"file-pretend-hash").as_bytes();
    let file_size = payload.len() as i64 * chunk_count;
    RemoteRow {
        txn_seq: seq,
        ts_micros: 1_700_000_000_000_000,
        body: RowBody::DataAdd {
            file_path: path.into(),
            file_size,
            file_blake3,
            chunk_count,
            chunk_id,
            chunk_data: payload,
            chunk_blake3,
        },
    }
}

fn data_remove_row(seq: i64, path: &str) -> RemoteRow {
    RemoteRow {
        txn_seq: seq,
        ts_micros: 1_700_000_000_000_000,
        body: RowBody::DataRemove {
            file_path: path.into(),
        },
    }
}

#[test]
fn chunk_size_is_a_meaningful_constant() {
    init_logger();
    assert_eq!(CHUNK_SIZE_BYTES, 16 * 1024 * 1024);
    assert_eq!(BLAKE3_LEN, 32);
}

#[test]
fn manifest_round_trip() {
    init_logger();
    let row = manifest_row(7, CommitKind::Compact, 6);
    let batch = rows_to_record_batch(std::slice::from_ref(&row)).unwrap();
    let decoded = record_batch_to_rows(&batch).unwrap();
    assert_eq!(decoded, vec![row]);
}

#[test]
fn checksum_round_trip_under_both_strategies() {
    init_logger();
    let m = checksum_row(3, "p1", ChecksumKind::Merkle, 0xAA);
    let h = checksum_row(3, "p2", ChecksumKind::Homomorphic, 0xBB);
    let batch = rows_to_record_batch(&[m.clone(), h.clone()]).unwrap();
    let decoded = record_batch_to_rows(&batch).unwrap();
    assert_eq!(decoded, vec![m, h]);
}

#[test]
fn data_add_and_remove_round_trip() {
    init_logger();
    let a = data_add_row(5, "data/part-001.parquet", 0, 1, vec![1, 2, 3, 4]);
    let r = data_remove_row(5, "data/part-002.parquet");
    let batch = rows_to_record_batch(&[a.clone(), r.clone()]).unwrap();
    let decoded = record_batch_to_rows(&batch).unwrap();
    assert_eq!(decoded, vec![a, r]);
}

#[test]
fn full_bundle_round_trip() {
    init_logger();
    let rows = vec![
        manifest_row(10, CommitKind::Compact, 9),
        checksum_row(10, "p1", ChecksumKind::Merkle, 0x01),
        checksum_row(10, "p2", ChecksumKind::Merkle, 0x02),
        data_add_row(10, "data/merged.parquet", 0, 1, b"merged-bytes".to_vec()),
        data_remove_row(10, "data/small-001.parquet"),
    ];
    let batch = rows_to_record_batch(&rows).unwrap();
    assert_eq!(batch.num_rows(), 5);

    let decoded = record_batch_to_rows(&batch).unwrap();
    assert_eq!(decoded, rows);
}

#[test]
fn record_batch_to_rows_rejects_unknown_partition_kind() {
    init_logger();
    use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
    use std::sync::Arc;

    let schema = arrow_schema();
    let bad = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["nope"])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
        ],
    )
    .unwrap();
    let err = record_batch_to_rows(&bad).unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("unknown partition_kind") && msg.contains("nope"),
        "expected unknown partition_kind error, got: {}",
        msg,
    );
}

#[test]
fn record_batch_to_rows_rejects_manifest_missing_commit_kind() {
    init_logger();
    use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
    use std::sync::Arc;

    let schema = arrow_schema();
    let bad = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![PARTITION_KIND_MANIFEST])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int64Array::from(vec![Some(0i64)])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
        ],
    )
    .unwrap();
    let err = record_batch_to_rows(&bad).unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("commit_kind"),
        "expected commit_kind missing error, got: {}",
        msg,
    );
}

#[test]
fn record_batch_to_rows_rejects_data_unknown_action() {
    init_logger();
    use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};
    use std::sync::Arc;

    let schema = arrow_schema();
    let bad = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![PARTITION_KIND_DATA])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(StringArray::from(vec![Some("data/x.parquet")])),
            Arc::new(StringArray::from(vec![Some("zonkify")])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(Int64Array::from(vec![None::<i64>])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
            Arc::new(BinaryArray::from_opt_vec(vec![None])),
        ],
    )
    .unwrap();
    let err = record_batch_to_rows(&bad).unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("unknown file_action") && msg.contains("zonkify"),
        "expected unknown file_action error, got: {}",
        msg,
    );
}

/// Milestone smoke test: the entire push design depends on delta-rs
/// committing a single batch containing rows for multiple partition
/// values as ONE atomic Delta version.  Verify directly.
#[tokio::test]
async fn multi_partition_single_commit_is_one_delta_version() {
    init_logger();
    let dir = TempDir::new().unwrap();
    let url = Url::from_directory_path(dir.path()).unwrap();
    let table = DeltaTable::try_from_url(url)
        .await
        .unwrap()
        .create()
        .with_columns(delta_columns())
        .with_partition_columns(partition_columns())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .unwrap();

    let v_initial = table.version().unwrap_or(0);

    let rows = vec![
        manifest_row(1, CommitKind::Write, 0),
        checksum_row(1, "p1", ChecksumKind::Merkle, 0x10),
        checksum_row(1, "p2", ChecksumKind::Merkle, 0x20),
        data_add_row(1, "data/a.parquet", 0, 1, b"first chunk".to_vec()),
        data_add_row(1, "data/b.parquet", 0, 1, b"second chunk".to_vec()),
    ];
    let batch = rows_to_record_batch(&rows).unwrap();

    let new_table = table.write(vec![batch]).await.unwrap();
    let v_after = new_table.version().unwrap_or(0);

    assert_eq!(
        v_after,
        v_initial + 1,
        "exactly one new Delta version produced for the multi-partition batch"
    );

    for kind in [
        PARTITION_KIND_MANIFEST,
        PARTITION_KIND_CHECKSUM,
        PARTITION_KIND_DATA,
    ] {
        let part_dir = dir.path().join(format!("partition_kind={}", kind));
        assert!(
            part_dir.exists(),
            "partition directory `{}` was created by the single commit",
            part_dir.display(),
        );
    }

    use datafusion::execution::context::SessionContext;
    use std::sync::Arc;
    let ctx = SessionContext::new();
    let _ = ctx.register_table("remote", Arc::new(new_table)).unwrap();
    let batches = ctx
        .sql("SELECT * FROM remote")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let mut got = Vec::new();
    for b in &batches {
        // record_batch_to_rows accepts any schema with matching column
        // count and looks up columns by name; it handles
        // dictionary-encoded partition columns internally, so no
        // projection is needed.
        let rows = record_batch_to_rows(b).unwrap();
        got.extend(rows);
    }
    assert_eq!(got.len(), rows.len(), "round-tripped row count");

    let mut expected_partition_kinds: Vec<&str> =
        rows.iter().map(|r| r.body.partition_kind()).collect();
    expected_partition_kinds.sort();
    let mut got_partition_kinds: Vec<&str> = got.iter().map(|r| r.body.partition_kind()).collect();
    got_partition_kinds.sort();
    assert_eq!(got_partition_kinds, expected_partition_kinds);
}
