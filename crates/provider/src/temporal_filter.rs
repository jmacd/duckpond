//! Temporal filtering for DataFusion ListingTables
//!
//! Provides low-level per-file temporal bounds filtering to filter out garbage
//! data outside a file's valid time range (set via `pond set-temporal-bounds`).

use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Constraints, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::datasource::TableType;
use datafusion::datasource::listing::ListingTable;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use log::debug;
use std::any::Any;
use std::sync::Arc;

/// Wrapper that applies temporal filtering to a ListingTable
///
/// Used for low-level per-file data quality filtering based on temporal bounds
/// metadata (from `pond set-temporal-bounds`). This filters out garbage data
/// outside the file's valid time range at the table provider level.
///
/// For unbounded files, use `(i64::MIN, i64::MAX)` to disable filtering.
pub struct TemporalFilteredListingTable {
    listing_table: ListingTable,
    min_time: i64,
    max_time: i64,
    cached_schema: std::sync::OnceLock<SchemaRef>,
}

impl TemporalFilteredListingTable {
    /// Create a new temporal filtered listing table
    ///
    /// # Arguments
    /// * `listing_table` - The underlying ListingTable to wrap
    /// * `min_time` - Minimum timestamp in milliseconds (use i64::MIN for unbounded)
    /// * `max_time` - Maximum timestamp in milliseconds (use i64::MAX for unbounded)
    #[must_use]
    pub fn new(listing_table: ListingTable, min_time: i64, max_time: i64) -> Self {
        Self {
            listing_table,
            min_time,
            max_time,
            cached_schema: std::sync::OnceLock::new(),
        }
    }

    fn apply_temporal_filter_to_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        min_seconds: i64,
        max_seconds: i64,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = plan.schema();

        // Find timestamp column index
        let timestamp_col_index = schema
            .fields()
            .iter()
            .position(|f| f.name() == "timestamp")
            .ok_or_else(|| {
                DataFusionError::Plan("Timestamp column not found in schema".to_string())
            })?;

        // Check the timestamp column's timezone
        let timestamp_field = &schema.fields()[timestamp_col_index];
        let timestamp_timezone = match timestamp_field.data_type() {
            DataType::Timestamp(TimeUnit::Second, tz) => tz.clone(),
            _ => {
                return Err(DataFusionError::Plan(
                    "Expected timestamp column with second precision".to_string(),
                ));
            }
        };

        // Create temporal bound expressions with matching timezone
        let min_timestamp = Arc::new(Literal::new(ScalarValue::TimestampSecond(
            Some(min_seconds),
            timestamp_timezone.clone(),
        )));

        let max_timestamp = Arc::new(Literal::new(ScalarValue::TimestampSecond(
            Some(max_seconds),
            timestamp_timezone,
        )));

        // Create column reference for timestamp
        let timestamp_col = Arc::new(Column::new("timestamp", timestamp_col_index));

        // Create filter expressions: timestamp >= min_timestamp AND timestamp <= max_timestamp
        let min_filter = Arc::new(BinaryExpr::new(
            timestamp_col.clone(),
            Operator::GtEq,
            min_timestamp,
        ));

        let max_filter = Arc::new(BinaryExpr::new(
            timestamp_col,
            Operator::LtEq,
            max_timestamp,
        ));

        let combined_filter = Arc::new(BinaryExpr::new(min_filter, Operator::And, max_filter));

        // Apply filter to the plan
        let filter_exec = FilterExec::try_new(combined_filter, plan)?;
        Ok(Arc::new(filter_exec))
    }
}

impl std::fmt::Debug for TemporalFilteredListingTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TemporalFilteredListingTable")
            .field("listing_table", &"<ListingTable>")
            .field("min_time", &self.min_time)
            .field("max_time", &self.max_time)
            .finish()
    }
}

#[async_trait]
impl TableProvider for TemporalFilteredListingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.cached_schema.get_or_init(|| {
            let schema = self.listing_table.schema();
            let field_count = schema.fields().len();
            let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            let field_names_str = field_names.join(", ");
            debug!("🔍 TemporalFilteredListingTable.schema() FIRST CALL - caching {field_count} fields: [{field_names_str}]");
            schema
        }).clone()
    }

    fn table_type(&self) -> TableType {
        self.listing_table.table_type()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.listing_table.constraints()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        self.listing_table.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        debug!("🚨 TemporalFilteredListingTable.scan() called - temporal filtering is active!");

        // Convert from milliseconds to seconds for HydroVu data
        let min_seconds = self.min_time / 1000;
        let max_seconds = self.max_time / 1000;

        debug!("⚡ Temporal filtering range: {min_seconds} to {max_seconds} (seconds)");

        // Check if we actually need to apply temporal filtering
        if self.min_time == i64::MIN && self.max_time == i64::MAX {
            debug!("⚡ No temporal bounds - delegating to base ListingTable");
            return self
                .listing_table
                .scan(state, projection, filters, limit)
                .await;
        }

        // Check if this is an empty projection (COUNT case)
        let is_empty_projection = projection.as_ref().is_some_and(|p| p.is_empty());

        if is_empty_projection {
            debug!(
                "📊 Empty projection detected (COUNT query) - need to include timestamp for filtering"
            );

            // For temporal filtering with empty projection, we need to:
            // 1. Scan with timestamp column included
            // 2. Apply temporal filter
            // 3. Project back to empty schema

            // Find timestamp column index in the full schema
            let full_schema = self.listing_table.schema();
            let timestamp_col_index = full_schema
                .fields()
                .iter()
                .position(|f| f.name() == "timestamp")
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "No 'timestamp' field found in schema for temporal filtering".to_string(),
                    )
                })?;

            debug!("🔍 Found timestamp column at index {timestamp_col_index}");

            // Scan with timestamp column included
            let timestamp_projection = vec![timestamp_col_index];
            let base_plan = self
                .listing_table
                .scan(state, Some(&timestamp_projection), filters, limit)
                .await?;

            // Apply temporal filtering
            let filtered_plan =
                self.apply_temporal_filter_to_plan(base_plan, min_seconds, max_seconds)?;

            // Project back to empty schema for COUNT
            let empty_projection: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
            let projection_exec = ProjectionExec::try_new(empty_projection, filtered_plan)?;

            debug!("✅ Temporal filtering applied successfully for COUNT query");
            return Ok(Arc::new(projection_exec));
        }

        // For non-empty projections, proceed normally but ensure timestamp is included if needed
        let base_plan = self
            .listing_table
            .scan(state, projection, filters, limit)
            .await?;

        // Apply temporal filtering to the base plan
        let filtered_plan =
            self.apply_temporal_filter_to_plan(base_plan, min_seconds, max_seconds)?;

        debug!("✅ Temporal filtering applied successfully");
        Ok(filtered_plan)
    }
}
