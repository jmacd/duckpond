// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Shared delegation for dynamic factory files that lazily build an inner
//! [`SqlDerivedFile`](crate::factory::sql_derived::SqlDerivedFile).
//!
//! Factories like `temporal-reduce` and `timeseries-join` generate SQL on first
//! access and delegate all file/query behavior to an inner `SqlDerivedFile`.
//! They share byte-identical `File`, `Metadata`, and `QueryableFile`
//! implementations; this macro generates them.

/// Generate `tinyfs::File`, `tinyfs::Metadata`, and `tinyfs::QueryableFile`
/// implementations that delegate to a lazily-built inner `SqlDerivedFile`.
///
/// The target type MUST provide:
/// - a field `inner: std::sync::Arc<tokio::sync::Mutex<Option<SqlDerivedFile>>>`
/// - an async method `ensure_inner(&self) -> tinyfs::Result<()>` that populates
///   `inner` on first call.
///
/// `$label` is a short string used only in delegation debug logging.
macro_rules! impl_lazy_sql_derived_delegation {
    ($ty:ty, $label:literal) => {
        #[async_trait::async_trait]
        impl tinyfs::File for $ty {
            async fn async_reader(
                &self,
            ) -> tinyfs::Result<std::pin::Pin<Box<dyn tinyfs::AsyncReadSeek>>> {
                self.ensure_inner().await?;
                let inner_guard = self.inner.lock().await;
                let inner = inner_guard
                    .as_ref()
                    .expect("inner initialized by ensure_inner");
                inner.async_reader().await
            }

            async fn async_writer(
                &self,
            ) -> tinyfs::Result<std::pin::Pin<Box<dyn tinyfs::FileMetadataWriter>>> {
                self.ensure_inner().await?;
                let inner_guard = self.inner.lock().await;
                let inner = inner_guard
                    .as_ref()
                    .expect("inner initialized by ensure_inner");
                inner.async_writer().await
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_queryable(&self) -> Option<&dyn tinyfs::QueryableFile> {
                Some(self)
            }
        }

        #[async_trait::async_trait]
        impl tinyfs::Metadata for $ty {
            async fn metadata(&self) -> tinyfs::Result<tinyfs::NodeMetadata> {
                // Lightweight metadata without expensive schema discovery, so
                // list operations stay fast; schema discovery is deferred until
                // actual content access (as_table_provider, async_reader, ...).
                Ok(tinyfs::NodeMetadata {
                    version: 1,
                    size: None,
                    blake3: None,
                    bao_outboard: None,
                    entry_type: tinyfs::EntryType::TableDynamic,
                    timestamp: 0,
                })
            }
        }

        #[async_trait::async_trait]
        impl tinyfs::QueryableFile for $ty {
            async fn as_table_provider(
                &self,
                id: tinyfs::FileID,
                context: &tinyfs::ProviderContext,
            ) -> tinyfs::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
                log::debug!(
                    concat!("DELEGATING ", $label, " to inner SqlDerivedFile: id={}"),
                    id
                );
                self.ensure_inner().await?;
                let inner_guard = self.inner.lock().await;
                let inner = inner_guard
                    .as_ref()
                    .expect("inner initialized by ensure_inner");
                inner.as_table_provider(id, context).await
            }
        }
    };
}

pub(crate) use impl_lazy_sql_derived_delegation;
