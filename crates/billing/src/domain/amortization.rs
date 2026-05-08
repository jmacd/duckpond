// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Amortization logic for tax/insurance/etc.
//!
//! For each expense, look up the matching `amortization_rules` row
//! effective at the cycle's `bill_date` (or paid_date when the expense has
//! no cycle assignment). Default (no rule) is `this_cycle_fraction = 1.0,
//! next_cycle_fraction = 0.0`.
//!
//! Amount distribution uses
//! `this = amount.scale_f64(this_cycle_fraction); next = amount - this`
//! so the per-expense sum is always exact (no penny lost or invented).
//!
//! This mirrors the Go program's `expense.SplitAnnual` for the special
//! case `(this=0.5, next=0.5)`: an annual tax/insurance expense paid in
//! cycle X is split as `[Cents::split(2)[0]` to X, `[1]` to X+1].

use crate::money::Cents;
use crate::schema::{AmortizationRuleRow, CycleRow, CycleTotalRow, ExpenseRow};
use chrono::{DateTime, NaiveDate, Utc};
use std::collections::HashMap;

/// Compute per-(cycle_id, account_code) totals from raw expense rows.
///
/// Returns a deterministically-ordered Vec sorted by `(cycle_id,
/// account_code)`.
///
/// `unassigned` is populated with `(expense_id, reason)` pairs for any
/// expense the algorithm could not place (no covering cycle, no cycle for
/// the next-portion when amortizing the last cycle of the dataset, etc.).
#[must_use]
pub fn compute_totals(
    expenses: &[ExpenseRow],
    cycles: &[CycleRow],
    rules: &[AmortizationRuleRow],
) -> (Vec<CycleTotalRow>, Vec<(i32, String)>) {
    let mut accumulator: HashMap<(i32, i32), i64> = HashMap::new();
    let mut unassigned: Vec<(i32, String)> = Vec::new();

    // Pre-sort cycles by period_start so "next cycle after X" lookup is a
    // simple linear scan.
    let mut sorted_cycles: Vec<&CycleRow> = cycles.iter().collect();
    sorted_cycles.sort_by_key(|c| c.period_start);

    for e in expenses {
        let cycle = match locate_cycle(e, &sorted_cycles) {
            Some(c) => c,
            None => {
                unassigned.push((
                    e.expense_id,
                    format!(
                        "no cycle covers paid_date {} and no --cycle was specified",
                        us_to_naive_date(e.paid_date)
                    ),
                ));
                continue;
            }
        };
        let amount = Cents::from_units(e.amount_cents);
        let rule = active_rule_for(rules, e.account_code, cycle.bill_date);

        let (this_amount, next_amount) = match rule {
            None => (amount, Cents::ZERO),
            Some(r) => {
                let this = amount.scale_f64(r.this_cycle_fraction);
                let next = amount - this;
                (this, next)
            }
        };

        if this_amount.units() != 0 {
            *accumulator
                .entry((cycle.cycle_id, e.account_code))
                .or_insert(0) += this_amount.units();
        }
        if next_amount.units() != 0 {
            match next_cycle(cycle, &sorted_cycles) {
                Some(next_c) => {
                    *accumulator
                        .entry((next_c.cycle_id, e.account_code))
                        .or_insert(0) += next_amount.units();
                }
                None => {
                    unassigned.push((
                        e.expense_id,
                        format!(
                            "amortization rule splits {} of acct {} into next cycle, \
                             but no cycle starts after {}",
                            next_amount.display(),
                            e.account_code,
                            cycle.period_end
                        ),
                    ));
                }
            }
        }
    }

    let mut out: Vec<CycleTotalRow> = accumulator
        .into_iter()
        .filter(|(_, amount)| *amount != 0)
        .map(|((cycle_id, account_code), amount_cents)| CycleTotalRow {
            cycle_id,
            account_code,
            amount_cents,
        })
        .collect();
    out.sort_by_key(|r| (r.cycle_id, r.account_code));
    (out, unassigned)
}

/// Find the cycle this expense is assigned to. Prefers explicit
/// `cycle_id` if set; otherwise the cycle whose `[period_start,
/// period_end]` contains `paid_date`.
fn locate_cycle<'a>(e: &ExpenseRow, sorted_cycles: &[&'a CycleRow]) -> Option<&'a CycleRow> {
    if let Some(id) = e.cycle_id {
        return sorted_cycles.iter().copied().find(|c| c.cycle_id == id);
    }
    let date = us_to_naive_date(e.paid_date);
    sorted_cycles
        .iter()
        .copied()
        .find(|c| c.period_start <= date && date <= c.period_end)
}

/// The cycle whose `period_start` is the smallest date strictly greater
/// than `current.period_end`. Returns None if `current` is the last cycle.
fn next_cycle<'a>(current: &CycleRow, sorted_cycles: &[&'a CycleRow]) -> Option<&'a CycleRow> {
    sorted_cycles
        .iter()
        .copied()
        .find(|c| c.period_start > current.period_end)
}

/// Amortization rule for `account_code` effective on `bill_date`.
/// "Effective" means `effective_from <= bill_date AND (effective_to IS NULL
/// OR bill_date <= effective_to)`. If multiple rules match (operator
/// shouldn't do this, but defense-in-depth), the one with the latest
/// `effective_from` wins.
fn active_rule_for(
    rules: &[AmortizationRuleRow],
    account_code: i32,
    bill_date: NaiveDate,
) -> Option<&AmortizationRuleRow> {
    rules
        .iter()
        .filter(|r| {
            r.account_code == account_code
                && r.effective_from <= bill_date
                && r.effective_to.is_none_or(|to| bill_date <= to)
        })
        .max_by_key(|r| r.effective_from)
}

fn us_to_naive_date(us: i64) -> NaiveDate {
    DateTime::<Utc>::from_timestamp_micros(us)
        .map(|dt| dt.date_naive())
        .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cycle(
        id: i32,
        name: &str,
        start: (i32, u32, u32),
        end: (i32, u32, u32),
        bill: (i32, u32, u32),
    ) -> CycleRow {
        CycleRow {
            cycle_id: id,
            name: name.into(),
            period_start: NaiveDate::from_ymd_opt(start.0, start.1, start.2).unwrap(),
            period_end: NaiveDate::from_ymd_opt(end.0, end.1, end.2).unwrap(),
            bill_date: NaiveDate::from_ymd_opt(bill.0, bill.1, bill.2).unwrap(),
            policy_id: 1,
            inactive: vec![],
            notes: None,
            issued: false,
            bill_txn_id: None,
        }
    }

    fn expense(
        id: i32,
        paid: (i32, u32, u32),
        account: i32,
        cents: i64,
        cycle_id: Option<i32>,
    ) -> ExpenseRow {
        let dt = chrono::NaiveDateTime::new(
            NaiveDate::from_ymd_opt(paid.0, paid.1, paid.2).unwrap(),
            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        );
        ExpenseRow {
            expense_id: id,
            paid_date: dt.and_utc().timestamp_micros(),
            account_code: account,
            vendor: None,
            amount_cents: cents,
            memo: None,
            cycle_id,
            void_of: None,
        }
    }

    fn split_50_rule(account: i32) -> AmortizationRuleRow {
        AmortizationRuleRow {
            rule_id: 1,
            account_code: account,
            this_cycle_fraction: 0.5,
            next_cycle_fraction: 0.5,
            effective_from: NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            effective_to: None,
            description: None,
        }
    }

    #[test]
    fn no_rules_means_all_to_assignment_cycle() {
        let cycles = vec![cycle(
            1,
            "2024H1",
            (2024, 4, 1),
            (2024, 9, 30),
            (2024, 10, 1),
        )];
        let expenses = vec![expense(1, (2024, 8, 15), 5100, 12345, Some(1))];
        let (out, un) = compute_totals(&expenses, &cycles, &[]);
        assert!(un.is_empty());
        assert_eq!(
            out,
            vec![CycleTotalRow {
                cycle_id: 1,
                account_code: 5100,
                amount_cents: 12345
            }]
        );
    }

    #[test]
    fn fifty_fifty_rule_matches_go_split_annual() {
        // Go SplitAnnual: $1000 tax expense in C1 -> $500 to C1, $500 to C2.
        let cycles = vec![
            cycle(1, "2024H1", (2024, 4, 1), (2024, 9, 30), (2024, 10, 1)),
            cycle(2, "2024H2", (2024, 10, 1), (2025, 3, 31), (2025, 4, 1)),
        ];
        let expenses = vec![expense(1, (2024, 8, 15), 5400, 100_000, Some(1))];
        let rules = vec![split_50_rule(5400)];
        let (out, un) = compute_totals(&expenses, &cycles, &rules);
        assert!(un.is_empty());
        assert_eq!(
            out,
            vec![
                CycleTotalRow {
                    cycle_id: 1,
                    account_code: 5400,
                    amount_cents: 50_000
                },
                CycleTotalRow {
                    cycle_id: 2,
                    account_code: 5400,
                    amount_cents: 50_000
                },
            ]
        );
    }

    #[test]
    fn fifty_fifty_with_odd_cents_sums_exactly() {
        // $10.01 = 1001 cents split 50/50 with our algorithm:
        //   this = scale_f64(0.5) = 500
        //   next = 1001 - 500 = 501
        // Sum is exactly 1001, no penny lost.
        let cycles = vec![
            cycle(1, "C1", (2024, 1, 1), (2024, 6, 30), (2024, 7, 1)),
            cycle(2, "C2", (2024, 7, 1), (2024, 12, 31), (2025, 1, 1)),
        ];
        let expenses = vec![expense(1, (2024, 5, 1), 5400, 1001, Some(1))];
        let rules = vec![split_50_rule(5400)];
        let (out, un) = compute_totals(&expenses, &cycles, &rules);
        assert!(un.is_empty());
        let total: i64 = out.iter().map(|r| r.amount_cents).sum();
        assert_eq!(total, 1001);
        // Specifically: this=500, next=501 (floor + remainder to next side).
        let c1 = out.iter().find(|r| r.cycle_id == 1).unwrap();
        let c2 = out.iter().find(|r| r.cycle_id == 2).unwrap();
        assert_eq!(c1.amount_cents, 500);
        assert_eq!(c2.amount_cents, 501);
    }

    #[test]
    fn last_cycle_amortization_unassigned() {
        let cycles = vec![cycle(1, "C1", (2024, 1, 1), (2024, 6, 30), (2024, 7, 1))];
        let expenses = vec![expense(1, (2024, 5, 1), 5400, 100_000, Some(1))];
        let rules = vec![split_50_rule(5400)];
        let (out, un) = compute_totals(&expenses, &cycles, &rules);
        // Half goes to C1; the other half is unassigned (no next cycle).
        assert_eq!(
            out,
            vec![CycleTotalRow {
                cycle_id: 1,
                account_code: 5400,
                amount_cents: 50_000
            }]
        );
        assert_eq!(un.len(), 1);
        assert_eq!(un[0].0, 1);
        assert!(un[0].1.contains("no cycle starts after"));
    }

    #[test]
    fn cycle_inferred_from_paid_date_when_unassigned() {
        let cycles = vec![cycle(1, "C1", (2024, 1, 1), (2024, 6, 30), (2024, 7, 1))];
        let expenses = vec![expense(1, (2024, 3, 1), 5100, 5000, None)];
        let (out, un) = compute_totals(&expenses, &cycles, &[]);
        assert!(un.is_empty());
        assert_eq!(out[0].cycle_id, 1);
    }

    #[test]
    fn expense_with_no_covering_cycle_unassigned() {
        let cycles = vec![cycle(1, "C1", (2024, 7, 1), (2024, 12, 31), (2025, 1, 1))];
        let expenses = vec![expense(1, (2024, 3, 1), 5100, 5000, None)];
        let (out, un) = compute_totals(&expenses, &cycles, &[]);
        assert!(out.is_empty());
        assert_eq!(un.len(), 1);
    }

    #[test]
    fn voided_expense_subtracts_via_negative_amount() {
        let cycles = vec![cycle(1, "C1", (2024, 1, 1), (2024, 6, 30), (2024, 7, 1))];
        let expenses = vec![
            expense(1, (2024, 3, 1), 5100, 5000, Some(1)),
            // void: same account, negative amount
            expense(2, (2024, 3, 1), 5100, -5000, Some(1)),
        ];
        let (out, un) = compute_totals(&expenses, &cycles, &[]);
        assert!(un.is_empty());
        // 5000 + (-5000) = 0, which is omitted from the accumulator output.
        assert!(out.is_empty(), "voided expenses net to zero -> omitted");
    }

    #[test]
    fn rule_effective_dates_respected() {
        let cycles = vec![
            cycle(1, "C1", (2020, 1, 1), (2020, 6, 30), (2020, 7, 1)),
            cycle(2, "C2", (2025, 1, 1), (2025, 6, 30), (2025, 7, 1)),
            cycle(3, "C3", (2025, 7, 1), (2025, 12, 31), (2026, 1, 1)),
        ];
        // Rule: only effective starting 2025-01-01.
        let rules = vec![AmortizationRuleRow {
            rule_id: 1,
            account_code: 5400,
            this_cycle_fraction: 0.5,
            next_cycle_fraction: 0.5,
            effective_from: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            effective_to: None,
            description: None,
        }];
        let expenses = vec![
            expense(1, (2020, 3, 1), 5400, 100_000, Some(1)), // before rule -> all to C1
            expense(2, (2025, 3, 1), 5400, 100_000, Some(2)), // after rule -> 50/50 C2/C3
        ];
        let (out, _un) = compute_totals(&expenses, &cycles, &rules);
        assert_eq!(
            out,
            vec![
                CycleTotalRow {
                    cycle_id: 1,
                    account_code: 5400,
                    amount_cents: 100_000
                },
                CycleTotalRow {
                    cycle_id: 2,
                    account_code: 5400,
                    amount_cents: 50_000
                },
                CycleTotalRow {
                    cycle_id: 3,
                    account_code: 5400,
                    amount_cents: 50_000
                },
            ]
        );
    }
}
