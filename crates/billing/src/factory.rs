// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! The `accounts` executable factory.
//!
//! Single CLI entry point for all bookkeeping operations. Operators invoke
//! it via `pond run /system/etc/accounts <subcommand>` (or whatever path
//! the bootstrap mounts it at -- factory name and pond path are independent).

use clap::{Parser, Subcommand};
use provider::registry::{ExecutionContext, ExecutionMode, FactoryCommand};
use provider::{FactoryContext, register_executable_factory};
use serde_json::Value;
use tinyfs::Result as TinyFSResult;
use tlogfs::TLogFSError;

// ---------------------------------------------------------------------------
// Top-level parser
// ---------------------------------------------------------------------------

/// `pond run <accounts-path> <subcommand> ...`
#[derive(Debug, Parser)]
#[command(
    name = "accounts",
    about = "Caspar Water bookkeeping CLI",
    long_about = "Operator-facing double-entry bookkeeping. \
                  Each subcommand maps to a routine task; reports are also \
                  available at /views/* via pond cat."
)]
struct AccountsCli {
    #[command(subcommand)]
    command: AccountsSub,
}

#[derive(Debug, Subcommand)]
enum AccountsSub {
    /// Set the company identity row.
    BusinessSet(BusinessSetArgs),

    /// Connection (physical service hookup) operations.
    #[command(subcommand)]
    Connection(ConnectionSub),

    /// Customer (paying party) operations.
    #[command(subcommand)]
    Customer(CustomerSub),

    /// Tenancy (who is responsible for which connection) operations.
    #[command(subcommand)]
    Tenancy(TenancySub),

    /// Cycle (six-month billing period) operations.
    #[command(subcommand)]
    Cycle(CycleSub),

    /// Expense (line-item cost) operations.
    #[command(subcommand)]
    Expense(ExpenseSub),

    /// Payment (customer payment received) operations.
    #[command(subcommand)]
    Payment(PaymentSub),

    /// Billing policy operations.
    #[command(subcommand)]
    Policy(PolicySub),

    /// Bills (preview, issue, reverse).
    #[command(subcommand)]
    Bills(BillsSub),

    /// Record an opening AR balance for a customer (one-time roll-forward).
    OpeningBalance(OpeningBalanceArgs),

    /// Hand-written balanced transaction.
    Adjust(AdjustArgs),

    /// Show the journal (with filters).
    JournalShow(JournalShowArgs),

    /// Customer AR balance(s).
    Balance(BalanceArgs),

    /// Customers with positive balance, sorted desc.
    WhoOwes(WhoOwesArgs),

    /// AR aging report bucketed 0-30 / 31-60 / 61-90 / 90+ days.
    Aging(AgingArgs),

    /// Trial balance per account_code.
    TrialBalance(TrialBalanceArgs),

    /// Profit and loss (revenue, expenses, reserves contribution).
    Pnl(PnlArgs),

    /// Run all invariant checks; exit non-zero if any violations.
    Verify,
}

// ---------------------------------------------------------------------------
// business set
// ---------------------------------------------------------------------------

#[derive(Debug, clap::Args)]
struct BusinessSetArgs {
    /// Company display name.
    #[arg(long)]
    name: String,
    /// Mailing address (multi-line OK).
    #[arg(long)]
    address: String,
    /// Free-text contact info (phone, email).
    #[arg(long)]
    contact: String,
    /// Tax id (EIN), optional.
    #[arg(long)]
    ein: Option<String>,
}

// ---------------------------------------------------------------------------
// connection ...
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
enum ConnectionSub {
    /// Add a new connection.
    Add {
        #[arg(long)]
        name: String,
        #[arg(long, value_name = "ADDRESS")]
        service: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        first_active: String,
        #[arg(long)]
        commercial: bool,
        #[arg(long)]
        weight_class: Option<String>,
        #[arg(long)]
        notes: Option<String>,
    },
    /// Edit a connection. Only --name and --notes are editable; billing
    /// fields (service, commercial, weight-class) are write-once.
    Edit {
        /// Connection id or name.
        target: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        notes: Option<String>,
    },
    /// Retire a connection (set last_active). Refused if any open tenancy.
    Retire {
        target: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        as_of: String,
    },
    List {
        #[arg(long)]
        active: bool,
        #[arg(long)]
        commercial: bool,
    },
    Show {
        target: String,
    },
}

// ---------------------------------------------------------------------------
// customer ...
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
enum CustomerSub {
    Add {
        #[arg(long)]
        name: String,
        #[arg(long, value_name = "ADDRESS")]
        billing: String,
        #[arg(long)]
        contact: Option<String>,
        #[arg(long)]
        notes: Option<String>,
    },
    Edit {
        target: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        billing: Option<String>,
        #[arg(long)]
        contact: Option<String>,
        #[arg(long)]
        active: Option<bool>,
    },
    /// Re-points open tenancies and posts a transfer transaction
    /// (DR 1100/into + CR 1100/from by from's balance). Marks `from` as
    /// merged_into=into and active=false. Journal AR legs of `from` stay
    /// untouched (audit trail preserved).
    Merge {
        #[arg(long)]
        from: String,
        #[arg(long)]
        into: String,
    },
    List {
        #[arg(long)]
        active: bool,
        #[arg(long)]
        with_balance: bool,
    },
    Show {
        target: String,
    },
    /// Fuzzy lookup by partial name.
    Find {
        query: String,
    },
}

// ---------------------------------------------------------------------------
// tenancy ...
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
enum TenancySub {
    Start {
        #[arg(long)]
        connection: String,
        #[arg(long)]
        customer: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        start: String,
        #[arg(long)]
        notes: Option<String>,
    },
    End {
        #[arg(long)]
        connection: Option<String>,
        #[arg(long)]
        tenancy: Option<i32>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        end: String,
        #[arg(long)]
        notes: Option<String>,
    },
    Edit {
        #[arg(long)]
        tenancy: i32,
        #[arg(long, value_name = "YYYY-MM-DD")]
        start: Option<String>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        end: Option<String>,
        #[arg(long)]
        customer: Option<String>,
        #[arg(long)]
        notes: Option<String>,
    },
    List {
        #[arg(long)]
        connection: Option<String>,
        #[arg(long)]
        customer: Option<String>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        as_of: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// cycle ...
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
enum CycleSub {
    Add {
        #[arg(long)]
        name: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        start: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        bill_date: String,
        #[arg(long)]
        policy: String,
        /// Comma-separated connection ids/names.
        #[arg(long)]
        inactive: Option<String>,
        #[arg(long)]
        notes: Option<String>,
        /// Allow a non-Apr-1/Oct-1 period_start without warning.
        #[arg(long)]
        allow_irregular: bool,
    },
    Edit {
        target: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        bill_date: Option<String>,
        #[arg(long)]
        inactive: Option<String>,
        #[arg(long)]
        notes: Option<String>,
    },
    List,
    Show {
        target: String,
    },
    /// Per-cycle expense rollup (operations / utilities / insurance / taxes).
    Totals {
        #[arg(long)]
        cycle: Option<String>,
    },
    /// Recompute the materialized cycle_totals_materialized.parquet from
    /// expenses + amortization_rules. Idempotent.
    TotalsRefresh,
}

// ---------------------------------------------------------------------------
// expense ...
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
enum ExpenseSub {
    Add {
        #[arg(long, value_name = "YYYY-MM-DD")]
        date: String,
        /// Account code (must be `expense` kind).
        #[arg(long)]
        account: i32,
        /// Dollar amount, e.g. $123.45.
        #[arg(long)]
        amount: String,
        #[arg(long)]
        vendor: Option<String>,
        #[arg(long)]
        memo: Option<String>,
        #[arg(long)]
        cycle: Option<String>,
    },
    Void {
        #[arg(long)]
        id: i32,
        #[arg(long)]
        memo: Option<String>,
    },
    List {
        #[arg(long)]
        cycle: Option<String>,
        #[arg(long)]
        account: Option<i32>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        from: Option<String>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        to: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// payment ...
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
enum PaymentSub {
    Add {
        #[arg(long, value_name = "YYYY-MM-DD")]
        date: String,
        #[arg(long)]
        customer: String,
        #[arg(long)]
        amount: String,
        #[arg(long, default_value = "check")]
        method: String,
        #[arg(long)]
        memo: Option<String>,
    },
    Void {
        #[arg(long)]
        id: i32,
        #[arg(long)]
        memo: Option<String>,
    },
    List {
        #[arg(long)]
        customer: Option<String>,
        #[arg(long)]
        cycle: Option<String>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        from: Option<String>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        to: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// policy ...
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
enum PolicySub {
    /// List available billing policy strategy kinds.
    Kinds,
    /// Register a new billing policy.
    Add {
        #[arg(long)]
        name: String,
        #[arg(long)]
        kind: String,
        /// YAML config path (use `-` for stdin).
        #[arg(long, value_name = "PATH")]
        config_path: String,
        #[arg(long, value_name = "YYYY-MM-DD")]
        effective_from: Option<String>,
        #[arg(long)]
        description: Option<String>,
    },
    List {
        #[arg(long)]
        kind: Option<String>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        effective_as_of: Option<String>,
    },
    Show {
        target: String,
    },
    /// Edit non-effective fields. config_yaml is immutable once any cycle
    /// uses the policy.
    Edit {
        target: String,
        #[arg(long)]
        description: Option<String>,
        #[arg(long, value_name = "YYYY-MM-DD")]
        effective_to: Option<String>,
    },
    /// Amortization rule operations (account_code -> this/next cycle split).
    /// Append-only; no edit subcommand.
    #[command(subcommand)]
    Amortize(AmortizeSub),
}

#[derive(Debug, Subcommand)]
enum AmortizeSub {
    Add {
        #[arg(long)]
        account: i32,
        #[arg(long, default_value = "0.5")]
        this: f64,
        #[arg(long, default_value = "0.5")]
        next: f64,
        #[arg(long, value_name = "YYYY-MM-DD")]
        effective_from: String,
        #[arg(long)]
        description: Option<String>,
    },
    List {
        #[arg(long, value_name = "YYYY-MM-DD")]
        effective_as_of: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// bills ...
// ---------------------------------------------------------------------------

#[derive(Debug, Subcommand)]
enum BillsSub {
    /// Run the strategy against current data and print the result. No writes.
    Preview {
        #[arg(long)]
        cycle: String,
    },
    /// Issue bills for a cycle: write 2N journal legs + N bill_breakdowns
    /// rows + cycle update in one transaction.
    Issue {
        #[arg(long)]
        cycle: String,
    },
    /// Reverse an issued cycle's bills.
    Reverse {
        #[arg(long)]
        cycle: String,
        #[arg(long)]
        reason: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// adjust / opening-balance / journal show / reports
// ---------------------------------------------------------------------------

#[derive(Debug, clap::Args)]
struct OpeningBalanceArgs {
    #[arg(long)]
    customer: String,
    /// Dollar amount; negative for a credit balance.
    #[arg(long)]
    amount: String,
    #[arg(long, value_name = "YYYY-MM-DD")]
    date: Option<String>,
    #[arg(long)]
    memo: Option<String>,
}

#[derive(Debug, clap::Args)]
struct AdjustArgs {
    #[arg(long, value_name = "YYYY-MM-DD")]
    date: String,
    #[arg(long)]
    memo: String,
    /// Repeatable: `--leg=ACCT[:CUSTOMER_ID]:DEBIT_CENTS:CREDIT_CENTS`.
    #[arg(long = "leg", required = true, num_args = 1..)]
    legs: Vec<String>,
}

#[derive(Debug, clap::Args)]
struct JournalShowArgs {
    #[arg(long)]
    cycle: Option<String>,
    #[arg(long)]
    customer: Option<String>,
    #[arg(long)]
    connection: Option<String>,
    #[arg(long)]
    account: Option<i32>,
    #[arg(long, value_name = "YYYY-MM-DD")]
    from: Option<String>,
    #[arg(long, value_name = "YYYY-MM-DD")]
    to: Option<String>,
    #[arg(long, default_value = "200")]
    limit: usize,
}

#[derive(Debug, clap::Args)]
struct BalanceArgs {
    #[arg(long)]
    customer: Option<String>,
    #[arg(long, value_name = "YYYY-MM-DD")]
    as_of: Option<String>,
    #[arg(long, default_value = "table")]
    format: OutputFormat,
}

#[derive(Debug, clap::Args)]
struct WhoOwesArgs {
    #[arg(long, value_name = "YYYY-MM-DD")]
    as_of: Option<String>,
    #[arg(long, default_value = "$0")]
    min: String,
    #[arg(long, default_value = "table")]
    format: OutputFormat,
}

#[derive(Debug, clap::Args)]
struct AgingArgs {
    #[arg(long)]
    customer: Option<String>,
    #[arg(long, value_name = "YYYY-MM-DD")]
    as_of: Option<String>,
    #[arg(long, default_value = "table")]
    format: OutputFormat,
}

#[derive(Debug, clap::Args)]
struct TrialBalanceArgs {
    #[arg(long, value_name = "YYYY-MM-DD")]
    as_of: Option<String>,
    #[arg(long, default_value = "table")]
    format: OutputFormat,
}

#[derive(Debug, clap::Args)]
struct PnlArgs {
    #[arg(long)]
    cycle: Option<String>,
    #[arg(long, value_name = "YYYY-MM-DD")]
    from: Option<String>,
    #[arg(long, value_name = "YYYY-MM-DD")]
    to: Option<String>,
    #[arg(long, default_value = "table")]
    format: OutputFormat,
}

/// Output format for report subcommands.
#[derive(Debug, Clone, clap::ValueEnum)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

// ---------------------------------------------------------------------------
// FactoryCommand impl
// ---------------------------------------------------------------------------

impl FactoryCommand for AccountsCli {
    fn allowed(&self) -> ExecutionMode {
        // Phase 1 -- everything routes through PondReadWriter. When the
        // tlogfs read-only pond mode lands, switch the read subcommands
        // (Balance / WhoOwes / Aging / TrialBalance / Pnl / JournalShow /
        // Verify and the *List / *Show ones) to PondReader.
        ExecutionMode::PondReadWriter
    }
}

// ---------------------------------------------------------------------------
// validate / initialize / execute
// ---------------------------------------------------------------------------

/// The `accounts` factory takes no YAML config in v1 (everything is in the
/// subcommand args). We accept either:
/// - empty bytes / blank document
/// - a multi-doc pond manifest where one document has `kind: mknod` and
///   `spec.factory: accounts` (mirrors how sitegen/hydrovu handle this)
fn validate_accounts_config(config_bytes: &[u8]) -> TinyFSResult<Value> {
    let trimmed = std::str::from_utf8(config_bytes)
        .map_err(|e| tinyfs::Error::Other(format!("non-UTF8 accounts config: {e}")))?
        .trim();
    if trimmed.is_empty() {
        return Ok(Value::Null);
    }
    // For now we accept any well-formed YAML and ignore its content.
    // Future versions may carry default flags (output-format, etc.).
    let _: Value = serde_yaml::from_str(trimmed)
        .map_err(|e| tinyfs::Error::Other(format!("invalid YAML: {e}")))?;
    Ok(Value::Null)
}

/// Called once after `pond apply -f accounts.yaml`. Creates the /data
/// tree and seeds chart_of_accounts. Idempotent (safe on
/// `mknod --overwrite`).
async fn initialize_accounts(_config: Value, context: FactoryContext) -> Result<(), TLogFSError> {
    log::debug!("[ACCOUNTS] initialize: seeding /data + chart_of_accounts + amortization rules");
    let state = tlogfs::extract_state(&context)?;
    let fs = tinyfs::FS::new(state.clone())
        .await
        .map_err(TLogFSError::TinyFS)?;
    let root = fs.root().await.map_err(TLogFSError::TinyFS)?;
    let _ = root
        .create_dir_all("/data")
        .await
        .map_err(TLogFSError::TinyFS)?;
    crate::chart::seed(&root)
        .await
        .map_err(TLogFSError::TinyFS)?;
    crate::cli::policy::seed_default_amortization_rules(&root)
        .await
        .map_err(TLogFSError::TinyFS)?;
    Ok(())
}

/// Dispatch a subcommand. Reads/writes happen against the FS extracted
/// from the `FactoryContext`. The surrounding `pond run` transaction
/// wraps every write; commit happens once when execute returns Ok.
async fn execute_accounts(
    _config: Value,
    context: FactoryContext,
    ctx: ExecutionContext,
) -> Result<(), TLogFSError> {
    let cli: AccountsCli = ctx.to_command::<AccountsCli, TLogFSError>()?;
    let name = subcommand_name(&cli);
    log::info!("[ACCOUNTS] subcommand: {name}");

    let state = tlogfs::extract_state(&context)?;
    let fs = tinyfs::FS::new(state.clone())
        .await
        .map_err(TLogFSError::TinyFS)?;
    let root = fs.root().await.map_err(TLogFSError::TinyFS)?;
    // Ensure /data exists for any subcommand that writes a reference table.
    // create_dir_all is idempotent (mkdir -p semantics).
    let _ = root
        .create_dir_all("/data")
        .await
        .map_err(TLogFSError::TinyFS)?;

    use crate::cli::{
        adjust, bills, business, connection, customer, cycle as cycle_cli, expense, journal,
        opening, payment, policy as policy_cli, reports, tenancy, verify as verify_cli,
    };
    match cli.command {
        AccountsSub::BusinessSet(args) => {
            business::set(&root, args.name, args.address, args.contact, args.ein)
                .await
                .map_err(Into::<TLogFSError>::into)?;
        }
        AccountsSub::Connection(c) => match c {
            ConnectionSub::Add {
                name,
                service,
                first_active,
                commercial,
                weight_class,
                notes,
            } => {
                let _ = connection::add(
                    &root,
                    name,
                    service,
                    &first_active,
                    commercial,
                    weight_class,
                    notes,
                )
                .await
                .map_err(Into::<TLogFSError>::into)?;
            }
            ConnectionSub::Edit {
                target,
                name,
                notes,
            } => connection::edit(&root, &target, name, notes)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            ConnectionSub::Retire { target, as_of } => connection::retire(&root, &target, &as_of)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            ConnectionSub::List { active, commercial } => {
                connection::list(&root, active, commercial)
                    .await
                    .map_err(Into::<TLogFSError>::into)?
            }
            ConnectionSub::Show { target } => connection::show(&root, &target)
                .await
                .map_err(Into::<TLogFSError>::into)?,
        },
        AccountsSub::Customer(c) => match c {
            CustomerSub::Add {
                name,
                billing,
                contact,
                notes,
            } => {
                let _ = customer::add(&root, name, billing, contact, notes)
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
            }
            CustomerSub::Edit {
                target,
                name,
                billing,
                contact,
                active,
            } => customer::edit(&root, &target, name, billing, contact, active)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            CustomerSub::Merge { from, into } => customer::merge(&root, &from, &into)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            CustomerSub::List {
                active,
                with_balance,
            } => customer::list(&root, active, with_balance)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            CustomerSub::Show { target } => customer::show(&root, &target)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            CustomerSub::Find { query } => customer::find(&root, &query)
                .await
                .map_err(Into::<TLogFSError>::into)?,
        },
        AccountsSub::Tenancy(t) => match t {
            TenancySub::Start {
                connection,
                customer,
                start,
                notes,
            } => {
                let _ = tenancy::start(&root, &connection, &customer, &start, notes)
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
            }
            TenancySub::End {
                connection,
                tenancy,
                end,
                notes,
            } => tenancy::end(&root, connection.as_deref(), tenancy, &end, notes)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            TenancySub::Edit {
                tenancy,
                start,
                end,
                customer,
                notes,
            } => tenancy::edit(
                &root,
                tenancy,
                start.as_deref(),
                end.as_deref(),
                customer.as_deref(),
                notes,
            )
            .await
            .map_err(Into::<TLogFSError>::into)?,
            TenancySub::List {
                connection,
                customer,
                as_of,
            } => tenancy::list(
                &root,
                connection.as_deref(),
                customer.as_deref(),
                as_of.as_deref(),
            )
            .await
            .map_err(Into::<TLogFSError>::into)?,
        },
        AccountsSub::Expense(e) => match e {
            ExpenseSub::Add {
                date,
                account,
                amount,
                vendor,
                memo,
                cycle,
            } => {
                let _ = expense::add(
                    &root,
                    &date,
                    account,
                    &amount,
                    vendor,
                    memo,
                    cycle.as_deref(),
                )
                .await
                .map_err(Into::<TLogFSError>::into)?;
            }
            ExpenseSub::Void { id, memo } => {
                let _ = expense::void(&root, id, memo)
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
            }
            ExpenseSub::List {
                cycle,
                account,
                from,
                to,
            } => {
                let cycle_id = cycle.and_then(|s| s.parse::<i32>().ok());
                let from = journal::parse_date_filter(from.as_deref())
                    .map_err(Into::<TLogFSError>::into)?;
                let to =
                    journal::parse_date_filter(to.as_deref()).map_err(Into::<TLogFSError>::into)?;
                expense::list(&root, cycle_id, account, from, to)
                    .await
                    .map_err(Into::<TLogFSError>::into)?
            }
        },
        AccountsSub::Payment(p) => match p {
            PaymentSub::Add {
                date,
                customer,
                amount,
                method,
                memo,
            } => {
                let _ = payment::add(&root, &date, &customer, &amount, method, memo)
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
            }
            PaymentSub::Void { id, memo } => {
                let _ = payment::void(&root, id, memo)
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
            }
            PaymentSub::List {
                customer,
                cycle,
                from,
                to,
            } => {
                let from = journal::parse_date_filter(from.as_deref())
                    .map_err(Into::<TLogFSError>::into)?;
                let to =
                    journal::parse_date_filter(to.as_deref()).map_err(Into::<TLogFSError>::into)?;
                payment::list(&root, customer.as_deref(), cycle.as_deref(), from, to)
                    .await
                    .map_err(Into::<TLogFSError>::into)?
            }
        },
        AccountsSub::OpeningBalance(args) => {
            let _ = opening::run(
                &root,
                &args.customer,
                &args.amount,
                args.date.as_deref(),
                args.memo,
            )
            .await
            .map_err(Into::<TLogFSError>::into)?;
        }
        AccountsSub::Adjust(args) => {
            let _ = adjust::run(&root, &args.date, args.memo, args.legs)
                .await
                .map_err(Into::<TLogFSError>::into)?;
        }
        AccountsSub::JournalShow(args) => {
            let cycle_id = args.cycle.and_then(|s| s.parse::<i32>().ok());
            let customer_id = args.customer.and_then(|s| s.parse::<i32>().ok());
            let connection_id = args.connection.and_then(|s| s.parse::<i32>().ok());
            let from = journal::parse_date_filter(args.from.as_deref())
                .map_err(Into::<TLogFSError>::into)?;
            let to = journal::parse_date_filter(args.to.as_deref())
                .map_err(Into::<TLogFSError>::into)?;
            journal::show(
                &root,
                cycle_id,
                customer_id,
                connection_id,
                args.account,
                from,
                to,
                args.limit,
            )
            .await
            .map_err(Into::<TLogFSError>::into)?
        }
        AccountsSub::Cycle(c) => match c {
            CycleSub::Add {
                name,
                start,
                bill_date,
                policy,
                inactive,
                notes,
                allow_irregular,
            } => {
                let _ = cycle_cli::add(
                    &root,
                    name,
                    &start,
                    &bill_date,
                    &policy,
                    inactive,
                    notes,
                    allow_irregular,
                )
                .await
                .map_err(Into::<TLogFSError>::into)?;
            }
            CycleSub::Edit {
                target,
                bill_date,
                inactive,
                notes,
            } => cycle_cli::edit(&root, &target, bill_date.as_deref(), inactive, notes)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            CycleSub::List => cycle_cli::list(&root)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            CycleSub::Show { target } => cycle_cli::show(&root, &target)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            CycleSub::Totals { cycle } => cycle_cli::totals(&root, cycle.as_deref())
                .await
                .map_err(Into::<TLogFSError>::into)?,
            CycleSub::TotalsRefresh => cycle_cli::refresh(&root)
                .await
                .map_err(Into::<TLogFSError>::into)?,
        },
        AccountsSub::Policy(p) => match p {
            PolicySub::Kinds => policy_cli::kinds()
                .await
                .map_err(Into::<TLogFSError>::into)?,
            PolicySub::Add {
                name,
                kind,
                config_path,
                effective_from,
                description,
            } => {
                let yaml = read_config_path(&config_path).await?;
                let _ = policy_cli::add(&root, name, kind, yaml, effective_from, description)
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
            }
            PolicySub::List {
                kind,
                effective_as_of,
            } => policy_cli::list(&root, kind.as_deref(), effective_as_of.as_deref())
                .await
                .map_err(Into::<TLogFSError>::into)?,
            PolicySub::Show { target } => policy_cli::show(&root, &target)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            PolicySub::Edit {
                target,
                description,
                effective_to,
            } => policy_cli::edit(&root, &target, description, effective_to.as_deref())
                .await
                .map_err(Into::<TLogFSError>::into)?,
            PolicySub::Amortize(a) => match a {
                AmortizeSub::Add {
                    account,
                    this,
                    next,
                    effective_from,
                    description,
                } => {
                    let _ = policy_cli::amortize_add(
                        &root,
                        account,
                        this,
                        next,
                        &effective_from,
                        description,
                    )
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
                }
                AmortizeSub::List { effective_as_of } => {
                    policy_cli::amortize_list(&root, effective_as_of.as_deref())
                        .await
                        .map_err(Into::<TLogFSError>::into)?
                }
            },
        },
        AccountsSub::Bills(b) => match b {
            BillsSub::Preview { cycle } => bills::preview(&root, &cycle)
                .await
                .map_err(Into::<TLogFSError>::into)?,
            BillsSub::Issue { cycle } => {
                let _ = bills::issue(&root, &cycle)
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
            }
            BillsSub::Reverse { cycle, reason } => {
                let _ = bills::reverse(&root, &cycle, reason)
                    .await
                    .map_err(Into::<TLogFSError>::into)?;
            }
        },
        AccountsSub::Balance(args) => {
            reports::balance(&root, args.customer.as_deref(), args.as_of.as_deref())
                .await
                .map_err(Into::<TLogFSError>::into)?
        }
        AccountsSub::WhoOwes(args) => reports::who_owes(&root, args.as_of.as_deref(), &args.min)
            .await
            .map_err(Into::<TLogFSError>::into)?,
        AccountsSub::Aging(args) => {
            reports::aging(&root, args.customer.as_deref(), args.as_of.as_deref())
                .await
                .map_err(Into::<TLogFSError>::into)?
        }
        AccountsSub::TrialBalance(args) => reports::trial_balance(&root, args.as_of.as_deref())
            .await
            .map_err(Into::<TLogFSError>::into)?,
        AccountsSub::Pnl(args) => reports::pnl(
            &root,
            args.cycle.as_deref(),
            args.from.as_deref(),
            args.to.as_deref(),
        )
        .await
        .map_err(Into::<TLogFSError>::into)?,
        AccountsSub::Verify => verify_cli::run(&root)
            .await
            .map_err(Into::<TLogFSError>::into)?,
    }

    Ok(())
}

fn subcommand_name(cli: &AccountsCli) -> &'static str {
    match &cli.command {
        AccountsSub::BusinessSet(_) => "business-set",
        AccountsSub::Connection(c) => match c {
            ConnectionSub::Add { .. } => "connection add",
            ConnectionSub::Edit { .. } => "connection edit",
            ConnectionSub::Retire { .. } => "connection retire",
            ConnectionSub::List { .. } => "connection list",
            ConnectionSub::Show { .. } => "connection show",
        },
        AccountsSub::Customer(c) => match c {
            CustomerSub::Add { .. } => "customer add",
            CustomerSub::Edit { .. } => "customer edit",
            CustomerSub::Merge { .. } => "customer merge",
            CustomerSub::List { .. } => "customer list",
            CustomerSub::Show { .. } => "customer show",
            CustomerSub::Find { .. } => "customer find",
        },
        AccountsSub::Tenancy(t) => match t {
            TenancySub::Start { .. } => "tenancy start",
            TenancySub::End { .. } => "tenancy end",
            TenancySub::Edit { .. } => "tenancy edit",
            TenancySub::List { .. } => "tenancy list",
        },
        AccountsSub::Cycle(c) => match c {
            CycleSub::Add { .. } => "cycle add",
            CycleSub::Edit { .. } => "cycle edit",
            CycleSub::List => "cycle list",
            CycleSub::Show { .. } => "cycle show",
            CycleSub::Totals { .. } => "cycle totals",
            CycleSub::TotalsRefresh => "cycle totals-refresh",
        },
        AccountsSub::Expense(e) => match e {
            ExpenseSub::Add { .. } => "expense add",
            ExpenseSub::Void { .. } => "expense void",
            ExpenseSub::List { .. } => "expense list",
        },
        AccountsSub::Payment(p) => match p {
            PaymentSub::Add { .. } => "payment add",
            PaymentSub::Void { .. } => "payment void",
            PaymentSub::List { .. } => "payment list",
        },
        AccountsSub::Policy(p) => match p {
            PolicySub::Kinds => "policy kinds",
            PolicySub::Add { .. } => "policy add",
            PolicySub::List { .. } => "policy list",
            PolicySub::Show { .. } => "policy show",
            PolicySub::Edit { .. } => "policy edit",
            PolicySub::Amortize(a) => match a {
                AmortizeSub::Add { .. } => "policy amortize add",
                AmortizeSub::List { .. } => "policy amortize list",
            },
        },
        AccountsSub::Bills(b) => match b {
            BillsSub::Preview { .. } => "bills preview",
            BillsSub::Issue { .. } => "bills issue",
            BillsSub::Reverse { .. } => "bills reverse",
        },
        AccountsSub::OpeningBalance(_) => "opening-balance",
        AccountsSub::Adjust(_) => "adjust",
        AccountsSub::JournalShow(_) => "journal-show",
        AccountsSub::Balance(_) => "balance",
        AccountsSub::WhoOwes(_) => "who-owes",
        AccountsSub::Aging(_) => "aging",
        AccountsSub::TrialBalance(_) => "trial-balance",
        AccountsSub::Pnl(_) => "pnl",
        AccountsSub::Verify => "verify",
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read a YAML config from a host path or stdin (`-`). Used by
/// `policy add --config-path=...`.
async fn read_config_path(path: &str) -> Result<String, TLogFSError> {
    use tokio::io::AsyncReadExt;
    if path == "-" {
        let mut buf = String::new();
        let mut stdin = tokio::io::stdin();
        let _ = stdin
            .read_to_string(&mut buf)
            .await
            .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("stdin: {e}"))))?;
        Ok(buf)
    } else {
        tokio::fs::read_to_string(path)
            .await
            .map_err(|e| TLogFSError::TinyFS(tinyfs::Error::Other(format!("read {path}: {e}"))))
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

register_executable_factory!(
    name: "accounts",
    description: "Caspar Water bookkeeping CLI (double-entry, append-only)",
    validate: validate_accounts_config,
    initialize: initialize_accounts,
    execute: execute_accounts
);

// ---------------------------------------------------------------------------
// Tests: clap surface compiles and parses representative invocations.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> AccountsCli {
        AccountsCli::try_parse_from(std::iter::once("accounts").chain(args.iter().copied()))
            .expect("parse")
    }

    #[test]
    fn business_set_parses() {
        let cli = parse(&[
            "business-set",
            "--name=Caspar",
            "--address=Anywhere",
            "--contact=p:555",
        ]);
        assert!(matches!(cli.command, AccountsSub::BusinessSet(_)));
    }

    #[test]
    fn connection_add_parses() {
        let cli = parse(&[
            "connection",
            "add",
            "--name=CommCtr",
            "--service=100 Example St",
            "--first-active=2010-01-01",
            "--commercial",
        ]);
        match cli.command {
            AccountsSub::Connection(ConnectionSub::Add { commercial, .. }) => {
                assert!(commercial);
            }
            _ => panic!("expected connection add"),
        }
    }

    #[test]
    fn connection_edit_only_allows_name_and_notes() {
        // Sanity: --service is NOT a flag on connection edit.
        let result = AccountsCli::try_parse_from([
            "accounts",
            "connection",
            "edit",
            "5",
            "--service=200 Example St",
        ]);
        assert!(result.is_err(), "connection edit must reject --service");
    }

    #[test]
    fn customer_merge_parses() {
        let cli = parse(&["customer", "merge", "--from=12", "--into=15"]);
        match cli.command {
            AccountsSub::Customer(CustomerSub::Merge { from, into }) => {
                assert_eq!(from, "12");
                assert_eq!(into, "15");
            }
            _ => panic!("expected customer merge"),
        }
    }

    #[test]
    fn bills_issue_parses() {
        let cli = parse(&["bills", "issue", "--cycle=2024H1"]);
        match cli.command {
            AccountsSub::Bills(BillsSub::Issue { cycle }) => assert_eq!(cycle, "2024H1"),
            _ => panic!("expected bills issue"),
        }
    }

    #[test]
    fn adjust_requires_at_least_one_leg() {
        let result =
            AccountsCli::try_parse_from(["accounts", "adjust", "--date=2024-01-01", "--memo=test"]);
        assert!(result.is_err(), "adjust must require at least one --leg");
    }

    #[test]
    fn adjust_with_legs_parses() {
        let cli = parse(&[
            "adjust",
            "--date=2024-01-01",
            "--memo=test",
            "--leg=1100:12:5000:0",
            "--leg=4000::0:5000",
        ]);
        match cli.command {
            AccountsSub::Adjust(args) => assert_eq!(args.legs.len(), 2),
            _ => panic!("expected adjust"),
        }
    }

    #[test]
    fn balance_default_format_is_table() {
        let cli = parse(&["balance"]);
        match cli.command {
            AccountsSub::Balance(args) => assert!(matches!(args.format, OutputFormat::Table)),
            _ => panic!("expected balance"),
        }
    }

    #[test]
    fn validate_accepts_empty_config() {
        let v = validate_accounts_config(b"").expect("validate empty");
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn validate_accepts_blank_yaml() {
        let v = validate_accounts_config(b"---\n").expect("validate blank");
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn validate_rejects_invalid_yaml() {
        let result = validate_accounts_config(b": : : not yaml : : :");
        assert!(result.is_err());
    }
}
