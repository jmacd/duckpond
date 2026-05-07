// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Arrow schemas for every billing table.
//!
//! Schemas use `tinyfs::ForArrow` so each row type can round-trip through
//! `WD::create_table_from_items` / `read_table_as_items`. Series tables also
//! expose a constant `TIMESTAMP_COLUMN` naming the time column used by
//! `WD::write_series_from_batch`.
//!
//! Every monetary field is `Int64` cents. Every date is `Date32`. Every
//! timestamp is `Timestamp(Microsecond, UTC)`. IDs are `Int32`.

use arrow_schema::{DataType, Field, FieldRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tinyfs::arrow::ForArrow;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn field(name: &str, dt: DataType, nullable: bool) -> FieldRef {
    Arc::new(Field::new(name, dt, nullable))
}

fn ts_us() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
}

// ---------------------------------------------------------------------------
// Conventional pond paths for every table
// ---------------------------------------------------------------------------

pub const BUSINESS_PATH: &str = "/data/business.parquet";
pub const CHART_OF_ACCOUNTS_PATH: &str = "/data/chart_of_accounts.parquet";
pub const BILLING_POLICIES_PATH: &str = "/data/billing_policies.parquet";
pub const CONNECTIONS_PATH: &str = "/data/connections.parquet";
pub const CUSTOMERS_PATH: &str = "/data/customers.parquet";
pub const TENANCIES_PATH: &str = "/data/tenancies.parquet";
pub const CYCLES_PATH: &str = "/data/cycles.parquet";
pub const AMORTIZATION_RULES_PATH: &str = "/data/amortization_rules.parquet";
pub const BILL_BREAKDOWNS_PATH: &str = "/data/bill_breakdowns.parquet";
pub const CYCLE_TOTALS_MATERIALIZED_PATH: &str = "/data/cycle_totals_materialized.parquet";

pub const EXPENSES_PATH: &str = "/data/expenses.series";
pub const PAYMENTS_PATH: &str = "/data/payments.series";
pub const JOURNAL_PATH: &str = "/data/journal.series";

// ---------------------------------------------------------------------------
// business.parquet -- one row, company identity (design doc section 4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusinessRow {
    pub name: String,
    pub address: String,
    pub contact: String,
    pub ein: Option<String>,
}

impl ForArrow for BusinessRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("name", DataType::Utf8, false),
            field("address", DataType::Utf8, false),
            field("contact", DataType::Utf8, false),
            field("ein", DataType::Utf8, true),
        ]
    }
}

// ---------------------------------------------------------------------------
// chart_of_accounts.parquet (design doc section 4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChartOfAccountsRow {
    pub code: i32,
    pub name: String,
    pub kind: String,        // asset | liability | equity | income | expense
    pub normal_side: String, // debit | credit
    pub description: Option<String>,
}

impl ForArrow for ChartOfAccountsRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("code", DataType::Int32, false),
            field("name", DataType::Utf8, false),
            field("kind", DataType::Utf8, false),
            field("normal_side", DataType::Utf8, false),
            field("description", DataType::Utf8, true),
        ]
    }
}

// ---------------------------------------------------------------------------
// billing_policies.parquet (design doc section 4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BillingPolicyRow {
    pub policy_id: i32,
    pub name: String,
    pub kind: String,        // discriminator for a Rust strategy
    pub config_yaml: String, // shape depends on kind
    pub effective_from: chrono::NaiveDate,
    pub effective_to: Option<chrono::NaiveDate>,
    pub description: Option<String>,
}

impl ForArrow for BillingPolicyRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("policy_id", DataType::Int32, false),
            field("name", DataType::Utf8, false),
            field("kind", DataType::Utf8, false),
            field("config_yaml", DataType::Utf8, false),
            field("effective_from", DataType::Date32, false),
            field("effective_to", DataType::Date32, true),
            field("description", DataType::Utf8, true),
        ]
    }
}

// ---------------------------------------------------------------------------
// connections.parquet (design doc section 4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionRow {
    pub connection_id: i32,
    pub name: String,
    pub service_address: String,
    pub first_active: chrono::NaiveDate,
    pub last_active: Option<chrono::NaiveDate>,
    pub commercial: bool,
    pub weight_class: Option<String>,
    pub notes: Option<String>,
}

impl ForArrow for ConnectionRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("connection_id", DataType::Int32, false),
            field("name", DataType::Utf8, false),
            field("service_address", DataType::Utf8, false),
            field("first_active", DataType::Date32, false),
            field("last_active", DataType::Date32, true),
            field("commercial", DataType::Boolean, false),
            field("weight_class", DataType::Utf8, true),
            field("notes", DataType::Utf8, true),
        ]
    }
}

// ---------------------------------------------------------------------------
// customers.parquet (design doc section 4 + merged_into_customer_id)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomerRow {
    pub customer_id: i32,
    pub name: String,
    pub billing_address: String,
    pub contact: Option<String>,
    pub active: bool,
    pub notes: Option<String>,
    /// Set by `customer merge --from=A --into=B` on customer A.
    /// Reports JOIN through this for rollups.
    pub merged_into_customer_id: Option<i32>,
}

impl ForArrow for CustomerRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("customer_id", DataType::Int32, false),
            field("name", DataType::Utf8, false),
            field("billing_address", DataType::Utf8, false),
            field("contact", DataType::Utf8, true),
            field("active", DataType::Boolean, false),
            field("notes", DataType::Utf8, true),
            field("merged_into_customer_id", DataType::Int32, true),
        ]
    }
}

// ---------------------------------------------------------------------------
// tenancies.parquet (design doc section 4)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenancyRow {
    pub tenancy_id: i32,
    pub connection_id: i32,
    pub customer_id: i32,
    pub start_date: chrono::NaiveDate,
    pub end_date: Option<chrono::NaiveDate>,
    pub notes: Option<String>,
}

impl ForArrow for TenancyRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("tenancy_id", DataType::Int32, false),
            field("connection_id", DataType::Int32, false),
            field("customer_id", DataType::Int32, false),
            field("start_date", DataType::Date32, false),
            field("end_date", DataType::Date32, true),
            field("notes", DataType::Utf8, true),
        ]
    }
}

// ---------------------------------------------------------------------------
// cycles.parquet (design doc section 4 + bill_txn_id)
// ---------------------------------------------------------------------------

/// `cycle.inactive` is a list of connection_ids excluded from this cycle.
/// Stored as `LargeList<Int32>` for compatibility with serde_arrow.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleRow {
    pub cycle_id: i32,
    pub name: String,
    pub period_start: chrono::NaiveDate,
    pub period_end: chrono::NaiveDate,
    pub bill_date: chrono::NaiveDate,
    pub policy_id: i32,
    pub inactive: Vec<i32>,
    pub notes: Option<String>,
    pub issued: bool,
    /// Set by `bills issue` to the journal txn_id; cleared by `bills reverse`.
    pub bill_txn_id: Option<i32>,
}

impl ForArrow for CycleRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("cycle_id", DataType::Int32, false),
            field("name", DataType::Utf8, false),
            field("period_start", DataType::Date32, false),
            field("period_end", DataType::Date32, false),
            field("bill_date", DataType::Date32, false),
            field("policy_id", DataType::Int32, false),
            field(
                "inactive",
                DataType::LargeList(field("item", DataType::Int32, false)),
                false,
            ),
            field("notes", DataType::Utf8, true),
            field("issued", DataType::Boolean, false),
            field("bill_txn_id", DataType::Int32, true),
        ]
    }
}

// ---------------------------------------------------------------------------
// amortization_rules.parquet (new; rubber-duck addition)
// ---------------------------------------------------------------------------

/// Append-only. No edit subcommand. To change a rule, add a new row with a
/// later `effective_from` and an `effective_to` on the old row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AmortizationRuleRow {
    pub rule_id: i32,
    pub account_code: i32,
    pub this_cycle_fraction: f64,
    pub next_cycle_fraction: f64,
    pub effective_from: chrono::NaiveDate,
    pub effective_to: Option<chrono::NaiveDate>,
    pub description: Option<String>,
}

impl ForArrow for AmortizationRuleRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("rule_id", DataType::Int32, false),
            field("account_code", DataType::Int32, false),
            field("this_cycle_fraction", DataType::Float64, false),
            field("next_cycle_fraction", DataType::Float64, false),
            field("effective_from", DataType::Date32, false),
            field("effective_to", DataType::Date32, true),
            field("description", DataType::Utf8, true),
        ]
    }
}

// ---------------------------------------------------------------------------
// bill_breakdowns.parquet (new; rubber-duck addition)
// ---------------------------------------------------------------------------

/// Written by `bills issue` (one row per active connection in the cycle).
/// Deleted by `bills reverse`. Never edited. Records the strategy's
/// per-connection reasoning so the bills view can show breakdown columns
/// without re-running the policy at read time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BillBreakdownRow {
    pub cycle_id: i32,
    pub connection_id: i32,
    pub weight: f64,
    pub share_fraction: f64,
    pub base_cents: i64,
    pub margin_cents: i64,
    pub policy_kind: String,
}

impl ForArrow for BillBreakdownRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("cycle_id", DataType::Int32, false),
            field("connection_id", DataType::Int32, false),
            field("weight", DataType::Float64, false),
            field("share_fraction", DataType::Float64, false),
            field("base_cents", DataType::Int64, false),
            field("margin_cents", DataType::Int64, false),
            field("policy_kind", DataType::Utf8, false),
        ]
    }
}

// ---------------------------------------------------------------------------
// cycle_totals_materialized.parquet (new; output of amortization Rust step)
// ---------------------------------------------------------------------------

/// Computed by `cycle totals refresh` from expenses + amortization_rules.
/// Read by the `cycle_totals` sql-derived-table view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleTotalRow {
    pub cycle_id: i32,
    pub account_code: i32,
    pub amount_cents: i64,
}

impl ForArrow for CycleTotalRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("cycle_id", DataType::Int32, false),
            field("account_code", DataType::Int32, false),
            field("amount_cents", DataType::Int64, false),
        ]
    }
}

// ---------------------------------------------------------------------------
// expenses.series (design doc section 4; time column = paid_date)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpenseRow {
    pub expense_id: i32,
    /// Microseconds since epoch (UTC).
    pub paid_date: i64,
    pub account_code: i32,
    pub vendor: Option<String>,
    pub amount_cents: i64,
    pub memo: Option<String>,
    pub cycle_id: Option<i32>,
    pub void_of: Option<i32>,
}

impl ForArrow for ExpenseRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("expense_id", DataType::Int32, false),
            field("paid_date", ts_us(), false),
            field("account_code", DataType::Int32, false),
            field("vendor", DataType::Utf8, true),
            field("amount_cents", DataType::Int64, false),
            field("memo", DataType::Utf8, true),
            field("cycle_id", DataType::Int32, true),
            field("void_of", DataType::Int32, true),
        ]
    }
}

impl ExpenseRow {
    pub const TIMESTAMP_COLUMN: &'static str = "paid_date";
}

// ---------------------------------------------------------------------------
// payments.series (design doc section 4; time column = received_date)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PaymentRow {
    pub payment_id: i32,
    /// Microseconds since epoch (UTC).
    pub received_date: i64,
    pub customer_id: i32,
    pub amount_cents: i64,
    pub method: Option<String>,
    pub memo: Option<String>,
    pub void_of: Option<i32>,
}

impl ForArrow for PaymentRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("payment_id", DataType::Int32, false),
            field("received_date", ts_us(), false),
            field("customer_id", DataType::Int32, false),
            field("amount_cents", DataType::Int64, false),
            field("method", DataType::Utf8, true),
            field("memo", DataType::Utf8, true),
            field("void_of", DataType::Int32, true),
        ]
    }
}

impl PaymentRow {
    pub const TIMESTAMP_COLUMN: &'static str = "received_date";
}

// ---------------------------------------------------------------------------
// journal.series (design doc section 4; time column = txn_date)
// ---------------------------------------------------------------------------

/// One leg of one transaction. Legs sharing a `txn_id` must satisfy
/// `sum(debit_cents) = sum(credit_cents)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalLegRow {
    pub txn_id: i32,
    /// Microseconds since epoch (UTC). The effective date.
    pub txn_date: i64,
    /// Microseconds since epoch (UTC). When this leg was recorded.
    pub recorded_at: i64,
    pub leg_seq: i32,
    pub account_code: i32,
    pub customer_id: Option<i32>,
    pub connection_id: Option<i32>,
    pub cycle_id: Option<i32>,
    pub debit_cents: i64,
    pub credit_cents: i64,
    /// `bill` | `payment` | `expense` | `adjustment` | `opening` |
    /// `reversal` | `merge`
    pub source: String,
    pub source_ref: Option<String>,
    pub memo: Option<String>,
}

impl ForArrow for JournalLegRow {
    fn for_arrow() -> Vec<FieldRef> {
        vec![
            field("txn_id", DataType::Int32, false),
            field("txn_date", ts_us(), false),
            field("recorded_at", ts_us(), false),
            field("leg_seq", DataType::Int32, false),
            field("account_code", DataType::Int32, false),
            field("customer_id", DataType::Int32, true),
            field("connection_id", DataType::Int32, true),
            field("cycle_id", DataType::Int32, true),
            field("debit_cents", DataType::Int64, false),
            field("credit_cents", DataType::Int64, false),
            field("source", DataType::Utf8, false),
            field("source_ref", DataType::Utf8, true),
            field("memo", DataType::Utf8, true),
        ]
    }
}

impl JournalLegRow {
    pub const TIMESTAMP_COLUMN: &'static str = "txn_date";
}

// ---------------------------------------------------------------------------
// Tests: schema round-trip via WD::create_table_from_items + read_table_as_items
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use tinyfs::EntryType;
    use tinyfs::arrow::ParquetExt;

    /// Round-trip a single row through tinyfs to prove the Arrow schema and
    /// the serde model agree.
    async fn roundtrip<T>(path: &str, items: Vec<T>)
    where
        T: ForArrow
            + Serialize
            + for<'de> Deserialize<'de>
            + Send
            + Sync
            + std::fmt::Debug
            + PartialEq
            + Clone,
    {
        let fs = tinyfs::memory::new_fs().await;
        let wd = fs.root().await.expect("root");
        wd.create_table_from_items(path, &items, EntryType::TablePhysicalVersion)
            .await
            .expect("write");
        let read: Vec<T> = wd.read_table_as_items(path).await.expect("read");
        assert_eq!(items, read);
    }

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).expect("valid date")
    }

    #[tokio::test]
    async fn business_roundtrip() {
        roundtrip(
            "business.parquet",
            vec![BusinessRow {
                name: "Caspar Water Company, LLC".into(),
                address: "200 Example Lane; Anytown, CA 99999".into(),
                contact: "p: 555-555-5555".into(),
                ein: Some("00-0000000".into()),
            }],
        )
        .await;
    }

    #[tokio::test]
    async fn chart_of_accounts_roundtrip() {
        roundtrip(
            "chart.parquet",
            vec![
                ChartOfAccountsRow {
                    code: 1100,
                    name: "Accounts Receivable".into(),
                    kind: "asset".into(),
                    normal_side: "debit".into(),
                    description: None,
                },
                ChartOfAccountsRow {
                    code: 4000,
                    name: "Service Revenue".into(),
                    kind: "income".into(),
                    normal_side: "credit".into(),
                    description: Some("Customer billings".into()),
                },
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn billing_policies_roundtrip() {
        roundtrip(
            "policies.parquet",
            vec![BillingPolicyRow {
                policy_id: 1,
                name: "share-by-weight-2024".into(),
                kind: "share-by-weight".into(),
                config_yaml: "margin: 0.2\n".into(),
                effective_from: d(2023, 10, 1),
                effective_to: None,
                description: Some("Standard 20% margin".into()),
            }],
        )
        .await;
    }

    #[tokio::test]
    async fn connections_roundtrip() {
        roundtrip(
            "connections.parquet",
            vec![
                ConnectionRow {
                    connection_id: 1,
                    name: "Community Center".into(),
                    service_address: "100 Example St".into(),
                    first_active: d(2010, 1, 1),
                    last_active: None,
                    commercial: true,
                    weight_class: None,
                    notes: None,
                },
                ConnectionRow {
                    connection_id: 2,
                    name: "200 Example St".into(),
                    service_address: "200 Example St".into(),
                    first_active: d(2012, 6, 1),
                    last_active: Some(d(2024, 1, 1)),
                    commercial: false,
                    weight_class: Some("residential".into()),
                    notes: Some("Retired 2024".into()),
                },
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn customers_roundtrip() {
        roundtrip(
            "customers.parquet",
            vec![
                CustomerRow {
                    customer_id: 12,
                    name: "Alex Carver".into(),
                    billing_address: "PO Box 1, Anytown CA".into(),
                    contact: Some("alex@example.com".into()),
                    active: true,
                    notes: None,
                    merged_into_customer_id: None,
                },
                CustomerRow {
                    customer_id: 13,
                    name: "Old Owner".into(),
                    billing_address: "(merged)".into(),
                    contact: None,
                    active: false,
                    notes: None,
                    merged_into_customer_id: Some(12),
                },
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn tenancies_roundtrip() {
        roundtrip(
            "tenancies.parquet",
            vec![TenancyRow {
                tenancy_id: 1,
                connection_id: 5,
                customer_id: 12,
                start_date: d(2021, 10, 1),
                end_date: None,
                notes: None,
            }],
        )
        .await;
    }

    #[tokio::test]
    async fn cycles_roundtrip() {
        roundtrip(
            "cycles.parquet",
            vec![
                CycleRow {
                    cycle_id: 1,
                    name: "2024H1".into(),
                    period_start: d(2024, 4, 1),
                    period_end: d(2024, 9, 30),
                    bill_date: d(2024, 10, 1),
                    policy_id: 1,
                    inactive: vec![],
                    notes: None,
                    issued: true,
                    bill_txn_id: Some(87),
                },
                CycleRow {
                    cycle_id: 2,
                    name: "2024H2".into(),
                    period_start: d(2024, 10, 1),
                    period_end: d(2025, 3, 31),
                    bill_date: d(2025, 4, 1),
                    policy_id: 1,
                    inactive: vec![3, 7],
                    notes: Some("Connection 3 vacant; 7 broken meter".into()),
                    issued: false,
                    bill_txn_id: None,
                },
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn amortization_rules_roundtrip() {
        roundtrip(
            "amortization.parquet",
            vec![
                AmortizationRuleRow {
                    rule_id: 1,
                    account_code: 5300,
                    this_cycle_fraction: 0.5,
                    next_cycle_fraction: 0.5,
                    effective_from: d(2020, 1, 1),
                    effective_to: None,
                    description: Some("Insurance amortized 50/50".into()),
                },
                AmortizationRuleRow {
                    rule_id: 2,
                    account_code: 5400,
                    this_cycle_fraction: 0.5,
                    next_cycle_fraction: 0.5,
                    effective_from: d(2020, 1, 1),
                    effective_to: None,
                    description: Some("Taxes amortized 50/50".into()),
                },
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn bill_breakdowns_roundtrip() {
        roundtrip(
            "breakdowns.parquet",
            vec![BillBreakdownRow {
                cycle_id: 1,
                connection_id: 5,
                weight: 1.0,
                share_fraction: 0.0714,
                base_cents: 41667,
                margin_cents: 8333,
                policy_kind: "share-by-weight".into(),
            }],
        )
        .await;
    }

    #[tokio::test]
    async fn cycle_totals_roundtrip() {
        roundtrip(
            "cycle_totals.parquet",
            vec![CycleTotalRow {
                cycle_id: 1,
                account_code: 5400,
                amount_cents: 50000,
            }],
        )
        .await;
    }

    // For series tables we just confirm the schema round-trips as a single
    // version. The actual append-only write path is exercised in later phases.

    #[tokio::test]
    async fn expenses_roundtrip() {
        roundtrip(
            "expenses.parquet",
            vec![ExpenseRow {
                expense_id: 1,
                paid_date: 1_700_000_000_000_000,
                account_code: 5100,
                vendor: Some("ChemCo".into()),
                amount_cents: 12345,
                memo: None,
                cycle_id: Some(1),
                void_of: None,
            }],
        )
        .await;
    }

    #[tokio::test]
    async fn payments_roundtrip() {
        roundtrip(
            "payments.parquet",
            vec![PaymentRow {
                payment_id: 1,
                received_date: 1_700_000_000_000_000,
                customer_id: 12,
                amount_cents: 50000,
                method: Some("check".into()),
                memo: Some("ck #1234".into()),
                void_of: None,
            }],
        )
        .await;
    }

    #[tokio::test]
    async fn journal_roundtrip() {
        roundtrip(
            "journal.parquet",
            vec![
                JournalLegRow {
                    txn_id: 87,
                    txn_date: 1_700_000_000_000_000,
                    recorded_at: 1_700_000_001_000_000,
                    leg_seq: 0,
                    account_code: 1100,
                    customer_id: Some(12),
                    connection_id: Some(5),
                    cycle_id: Some(1),
                    debit_cents: 50000,
                    credit_cents: 0,
                    source: "bill".into(),
                    source_ref: Some("bill:cycle=1,connection=5".into()),
                    memo: None,
                },
                JournalLegRow {
                    txn_id: 87,
                    txn_date: 1_700_000_000_000_000,
                    recorded_at: 1_700_000_001_000_000,
                    leg_seq: 1,
                    account_code: 4000,
                    customer_id: None,
                    connection_id: Some(5),
                    cycle_id: Some(1),
                    debit_cents: 0,
                    credit_cents: 50000,
                    source: "bill".into(),
                    source_ref: Some("bill:cycle=1,connection=5".into()),
                    memo: None,
                },
            ],
        )
        .await;
    }

    /// Sanity: the `TIMESTAMP_COLUMN` constants name fields that actually
    /// exist in the schema with `Timestamp(us, UTC)` type.
    #[test]
    fn series_timestamp_columns_exist() {
        for (name, fields) in [
            (ExpenseRow::TIMESTAMP_COLUMN, ExpenseRow::for_arrow()),
            (PaymentRow::TIMESTAMP_COLUMN, PaymentRow::for_arrow()),
            (JournalLegRow::TIMESTAMP_COLUMN, JournalLegRow::for_arrow()),
        ] {
            let f = fields
                .iter()
                .find(|f| f.name() == name)
                .unwrap_or_else(|| panic!("series timestamp column {name} not in schema"));
            assert_eq!(
                f.data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                "{} must be Timestamp(us, UTC)",
                name
            );
            assert!(!f.is_nullable(), "{name} must be non-null");
        }
    }
}
