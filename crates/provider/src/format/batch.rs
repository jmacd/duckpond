// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for non-streaming format providers.
//!
//! Several format providers (`jsonlogs`, `weblog`, `oteljson`, ...) read their
//! entire input, build a single in-memory [`RecordBatch`], and present it as a
//! one-item stream. These helpers centralize that boilerplate so each provider
//! only writes its format-specific parsing and schema logic.

use crate::{Error, Result};
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use futures::stream::Stream;
use std::pin::Pin;

/// Boxed, pinned stream of [`RecordBatch`] results as returned by
/// [`crate::format::FormatProvider::open_stream`].
pub(crate) type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// A [`Stream`] that yields exactly one [`RecordBatch`] then terminates.
struct SingleBatchStream {
    batch: Option<RecordBatch>,
}

impl Stream for SingleBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.batch.take().map(Ok))
    }
}

/// Wrap a single [`RecordBatch`] in a boxed stream that yields it once.
pub(crate) fn single_batch_stream(batch: RecordBatch) -> BatchStream {
    Box::pin(SingleBatchStream { batch: Some(batch) })
}

/// Build a [`RecordBatch`] from columns, handling the empty (zero-row) case.
///
/// [`RecordBatch::try_new`] cannot infer the row count when there are zero
/// columns, so this always supplies an explicit row count derived from the
/// first column (or zero when there are none). Arrow errors are mapped to
/// [`Error::Arrow`].
pub(crate) fn finish_batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    let row_count = columns.first().map_or(0, |c| c.len());
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(row_count)),
    )
    .map_err(|e| Error::Arrow(e.to_string()))
}
