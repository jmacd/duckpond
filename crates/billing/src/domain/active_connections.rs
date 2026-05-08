// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Active-connection filter for billing.
//!
//! A connection is "active for a cycle" iff:
//! - `first_active <= cycle.bill_date`
//! - AND `last_active IS NULL OR cycle.bill_date < last_active`
//! - AND `connection_id NOT IN cycle.inactive`
//!
//! `bills issue` refuses if any active connection lacks a covering
//! tenancy on `cycle.bill_date`.

use crate::schema::{ConnectionRow, CycleRow};

/// Subset of `connections` that should appear on a bill for `cycle`.
#[must_use]
pub fn active_for_cycle<'a>(
    connections: &'a [ConnectionRow],
    cycle: &CycleRow,
) -> Vec<&'a ConnectionRow> {
    let bill_date = cycle.bill_date;
    let inactive: std::collections::HashSet<i32> = cycle.inactive.iter().copied().collect();
    connections
        .iter()
        .filter(|c| {
            c.first_active <= bill_date
                && c.last_active.is_none_or(|la| bill_date < la)
                && !inactive.contains(&c.connection_id)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn c(id: i32, first: (i32, u32, u32), last: Option<(i32, u32, u32)>) -> ConnectionRow {
        ConnectionRow {
            connection_id: id,
            name: format!("c{id}"),
            service_address: "addr".into(),
            first_active: NaiveDate::from_ymd_opt(first.0, first.1, first.2).unwrap(),
            last_active: last.map(|(y, m, d)| NaiveDate::from_ymd_opt(y, m, d).unwrap()),
            commercial: false,
            weight_class: None,
            notes: None,
        }
    }

    fn cycle_with(inactive: Vec<i32>, bill: (i32, u32, u32)) -> CycleRow {
        CycleRow {
            cycle_id: 1,
            name: "T".into(),
            period_start: NaiveDate::from_ymd_opt(bill.0, 1, 1).unwrap(),
            period_end: NaiveDate::from_ymd_opt(bill.0, 12, 31).unwrap(),
            bill_date: NaiveDate::from_ymd_opt(bill.0, bill.1, bill.2).unwrap(),
            policy_id: 1,
            inactive,
            notes: None,
            issued: false,
            bill_txn_id: None,
        }
    }

    #[test]
    fn includes_active_excludes_retired() {
        let conns = vec![
            c(1, (2010, 1, 1), None),
            c(2, (2010, 1, 1), Some((2020, 1, 1))), // retired before bill date
            c(3, (2030, 1, 1), None),               // not yet active on bill date
        ];
        let cycle = cycle_with(vec![], (2024, 10, 1));
        let active = active_for_cycle(&conns, &cycle);
        let ids: Vec<_> = active.iter().map(|c| c.connection_id).collect();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn last_active_is_exclusive_upper_bound() {
        // Connection retired on the cycle's bill_date is INACTIVE on that date.
        let conns = vec![c(1, (2010, 1, 1), Some((2024, 10, 1)))];
        let cycle = cycle_with(vec![], (2024, 10, 1));
        assert!(active_for_cycle(&conns, &cycle).is_empty());
        // But active on the day before.
        let cycle_prev = cycle_with(vec![], (2024, 9, 30));
        assert_eq!(active_for_cycle(&conns, &cycle_prev).len(), 1);
    }

    #[test]
    fn cycle_inactive_excludes() {
        let conns = vec![c(1, (2010, 1, 1), None), c(2, (2010, 1, 1), None)];
        let cycle = cycle_with(vec![2], (2024, 10, 1));
        let active = active_for_cycle(&conns, &cycle);
        let ids: Vec<_> = active.iter().map(|c| c.connection_id).collect();
        assert_eq!(ids, vec![1]);
    }
}
