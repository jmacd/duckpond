// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Name-or-id lookup helpers and standardized error types.
//!
//! Every reference table that has both `name` (Utf8, unique) and an `Int32`
//! primary key allows the operator to specify either form on the CLI. This
//! module enforces:
//!   - exact id match if `query` parses as `i32`
//!   - exact name match otherwise
//!   - **never** a silent partial / case-insensitive match on the
//!     name-or-id path (those are reserved for explicit `find` subcommands)
//!   - clear "ambiguous" / "not found" / "wrong-typed" errors with full
//!     candidate lists when ambiguous

use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum LookupError {
    #[error("no {kind} matches `{query}`")]
    NotFound { kind: &'static str, query: String },

    #[error(
        "ambiguous {kind} `{query}` matches {} rows: {}",
        candidates.len(),
        candidates.join(", ")
    )]
    Ambiguous {
        kind: &'static str,
        query: String,
        candidates: Vec<String>,
    },
}

/// Resolve `query` to exactly one row in `items`. `query` is interpreted
/// as an id if it parses as `i32`; otherwise as an exact name.
///
/// Returns `Ok((idx, &row))` or a clear error.
pub fn find_one<'a, T>(
    kind: &'static str,
    query: &str,
    items: &'a [T],
    id_fn: impl Fn(&T) -> i32,
    name_fn: impl Fn(&T) -> &str,
) -> Result<(usize, &'a T), LookupError> {
    if let Ok(needle_id) = query.parse::<i32>() {
        let mut hits = items
            .iter()
            .enumerate()
            .filter(|(_, t)| id_fn(t) == needle_id);
        match (hits.next(), hits.next()) {
            (Some((idx, row)), None) => Ok((idx, row)),
            (None, _) => Err(LookupError::NotFound {
                kind,
                query: query.to_string(),
            }),
            (Some((_, a)), Some((_, b))) => Err(LookupError::Ambiguous {
                kind,
                query: query.to_string(),
                candidates: vec![
                    format_candidate(a, &id_fn, &name_fn),
                    format_candidate(b, &id_fn, &name_fn),
                ],
            }),
        }
    } else {
        let mut matched: Vec<(usize, &T)> = items
            .iter()
            .enumerate()
            .filter(|(_, t)| name_fn(t) == query)
            .collect();
        match matched.len() {
            1 => Ok(matched.remove(0)),
            0 => Err(LookupError::NotFound {
                kind,
                query: query.to_string(),
            }),
            _ => Err(LookupError::Ambiguous {
                kind,
                query: query.to_string(),
                candidates: matched
                    .into_iter()
                    .map(|(_, t)| format_candidate(t, &id_fn, &name_fn))
                    .collect(),
            }),
        }
    }
}

fn format_candidate<T>(t: &T, id_fn: impl Fn(&T) -> i32, name_fn: impl Fn(&T) -> &str) -> String {
    format!("#{} {}", id_fn(t), name_fn(t))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Row {
        id: i32,
        name: String,
    }
    fn row(id: i32, name: &str) -> Row {
        Row {
            id,
            name: name.into(),
        }
    }

    fn dataset() -> Vec<Row> {
        vec![row(1, "Alice"), row(2, "Bob"), row(3, "Charlie")]
    }

    #[test]
    fn id_match_works() {
        let rows = dataset();
        let (idx, r) = find_one("customer", "2", &rows, |r| r.id, |r| &r.name).expect("found");
        assert_eq!(idx, 1);
        assert_eq!(r.name, "Bob");
    }

    #[test]
    fn name_match_works() {
        let rows = dataset();
        let (idx, r) =
            find_one("customer", "Charlie", &rows, |r| r.id, |r| &r.name).expect("found");
        assert_eq!(idx, 2);
        assert_eq!(r.id, 3);
    }

    #[test]
    fn id_not_found() {
        let rows = dataset();
        let err = find_one("customer", "99", &rows, |r| r.id, |r| &r.name).unwrap_err();
        assert_eq!(
            err,
            LookupError::NotFound {
                kind: "customer",
                query: "99".into()
            }
        );
    }

    #[test]
    fn name_not_found() {
        let rows = dataset();
        let err = find_one("customer", "Zoe", &rows, |r| r.id, |r| &r.name).unwrap_err();
        assert!(matches!(err, LookupError::NotFound { .. }));
    }

    #[test]
    fn name_ambiguous_error_lists_candidates() {
        let rows = vec![row(1, "Alex"), row(2, "Alex"), row(3, "Bob")];
        let err = find_one("customer", "Alex", &rows, |r| r.id, |r| &r.name).unwrap_err();
        match err {
            LookupError::Ambiguous { candidates, .. } => {
                assert_eq!(candidates, vec!["#1 Alex", "#2 Alex"]);
            }
            other => panic!("expected Ambiguous, got {other:?}"),
        }
    }

    #[test]
    fn id_form_takes_precedence_over_name_lookup() {
        let rows = vec![row(1, "2"), row(2, "Bob")];
        // "2" parses as id -> matches row with id=2 (Bob), NOT the row whose
        // name happens to be "2".
        let (_, r) = find_one("customer", "2", &rows, |r| r.id, |r| &r.name).expect("found");
        assert_eq!(r.name, "Bob");
    }
}
