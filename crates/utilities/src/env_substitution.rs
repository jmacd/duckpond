// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Environment variable substitution for configuration files.
//!
//! Performs pre-deserialization expansion of `${env:VAR}` and
//! `${env:VAR:-default}` placeholders in raw config text.
//!
//! # Syntax
//!
//! | Pattern | Behaviour |
//! |---|---|
//! | `${env:VAR}` | Replaced by the value of `$VAR`; error if unset |
//! | `${env:VAR:-default}` | Replaced by `$VAR`; falls back to `default` when unset |
//! | `${env:VAR:-}` | Replaced by `$VAR`; falls back to the empty string when unset |
//! | `$$` | Replaced by a single literal `$` |
//! | `${...}` (no `env:` prefix) | Passed through unchanged (reserved for future providers) |

use std::fmt;

/// Error type for environment variable substitution.
#[derive(Debug)]
pub enum EnvSubstError {
    /// Environment variable is not set and no default was provided.
    NotSet(String),
    /// Environment variable contains invalid Unicode.
    InvalidUnicode(String),
}

impl fmt::Display for EnvSubstError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotSet(var) => write!(
                f,
                "Environment variable '{}' not set and no default provided",
                var
            ),
            Self::InvalidUnicode(var) => {
                write!(f, "Environment variable '{}' contains invalid Unicode", var)
            }
        }
    }
}

impl std::error::Error for EnvSubstError {}

/// Returns true if `input` contains any `${env:` placeholders.
#[must_use]
pub fn has_env_refs(input: &str) -> bool {
    input.contains("${env:")
}

/// Substitute `${env:VAR}` and `${env:VAR:-default}` references in `input`.
///
/// # Errors
///
/// Returns an error when a `${env:VAR}` placeholder is encountered and `VAR`
/// is not set in the process environment and no `:-default` was provided.
pub fn substitute_env_vars(input: &str) -> Result<String, EnvSubstError> {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;

    while let Some(pos) = rest.find('$') {
        // Emit everything before the `$`.
        output.push_str(&rest[..pos]);
        rest = &rest[pos..];

        // `$$` → literal `$`
        if rest.starts_with("$$") {
            output.push('$');
            rest = &rest[2..];
            continue;
        }

        // Possible `${...}` placeholder.
        if rest.starts_with("${")
            && let Some(close) = rest[2..].find('}')
        {
            let inner = &rest[2..2 + close];

            if let Some(spec) = inner.strip_prefix("env:") {
                // Split on the first `:-` to allow an optional default.
                let (var_name, default) = match spec.find(":-") {
                    Some(p) => (&spec[..p], Some(&spec[p + 2..])),
                    None => (spec, None),
                };

                let value = match std::env::var(var_name) {
                    Ok(v) => v,
                    Err(std::env::VarError::NotPresent) => match default {
                        Some(d) => d.to_string(),
                        None => {
                            return Err(EnvSubstError::NotSet(var_name.to_string()));
                        }
                    },
                    Err(std::env::VarError::NotUnicode(_)) => {
                        return Err(EnvSubstError::InvalidUnicode(var_name.to_string()));
                    }
                };

                output.push_str(&value);
                rest = &rest[2 + close + 1..];
            } else {
                // Not an `env:` provider — pass through verbatim
                // (reserved for future providers like file:, vault:).
                output.push_str(&rest[..2 + close + 1]);
                rest = &rest[2 + close + 1..];
            }
            continue;
        }

        // Bare `$` with no recognised pattern — emit and advance.
        output.push('$');
        rest = &rest[1..];
    }

    // Emit any remaining text after the last `$`.
    output.push_str(rest);
    Ok(output)
}

/// Like [`substitute_env_vars`], but lenient: unresolvable `${env:VAR}`
/// references (no default, variable not set) are passed through verbatim
/// instead of causing an error. This is useful when expanding multi-document
/// config files where only a subset of env vars need to resolve.
#[must_use]
pub fn substitute_env_vars_lenient(input: &str) -> String {
    match substitute_env_vars(input) {
        Ok(expanded) => expanded,
        Err(_) => {
            // Re-run with a lenient approach: replace resolvable refs,
            // pass through unresolvable ones.
            let mut output = String::with_capacity(input.len());
            let mut rest = input;

            while let Some(pos) = rest.find('$') {
                output.push_str(&rest[..pos]);
                rest = &rest[pos..];

                if rest.starts_with("$$") {
                    output.push('$');
                    rest = &rest[2..];
                    continue;
                }

                if rest.starts_with("${")
                    && let Some(close) = rest[2..].find('}')
                {
                    let inner = &rest[2..2 + close];

                    if let Some(spec) = inner.strip_prefix("env:") {
                        let (var_name, default) = match spec.find(":-") {
                            Some(p) => (&spec[..p], Some(&spec[p + 2..])),
                            None => (spec, None),
                        };

                        match std::env::var(var_name) {
                            Ok(v) => output.push_str(&v),
                            Err(_) => match default {
                                Some(d) => output.push_str(d),
                                None => {
                                    // Pass through verbatim
                                    output.push_str(&rest[..2 + close + 1]);
                                }
                            },
                        }

                        rest = &rest[2 + close + 1..];
                    } else {
                        output.push_str(&rest[..2 + close + 1]);
                        rest = &rest[2 + close + 1..];
                    }
                    continue;
                }

                output.push('$');
                rest = &rest[1..];
            }

            output.push_str(rest);
            output
        }
    }
}

#[cfg(test)]
#[allow(unsafe_code)]
mod tests {
    use super::*;

    fn env_set_var(key: &str, value: &str) {
        unsafe {
            std::env::set_var(key, value);
        }
    }

    fn env_remove_var(key: &str) {
        unsafe {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn plain_text_unchanged() {
        let result = substitute_env_vars("hello: world").unwrap();
        assert_eq!(result, "hello: world");
    }

    #[test]
    fn basic_substitution() {
        env_set_var("POND_TEST_ENDPOINT", "localhost:4317");
        let result = substitute_env_vars("endpoint: ${env:POND_TEST_ENDPOINT}").unwrap();
        assert_eq!(result, "endpoint: localhost:4317");
        env_remove_var("POND_TEST_ENDPOINT");
    }

    #[test]
    fn default_used_when_unset() {
        env_remove_var("POND_TEST_UNSET");
        let result = substitute_env_vars("port: ${env:POND_TEST_UNSET:-9000}").unwrap();
        assert_eq!(result, "port: 9000");
    }

    #[test]
    fn empty_default_when_unset() {
        env_remove_var("POND_TEST_EMPTY");
        let result = substitute_env_vars("key: ${env:POND_TEST_EMPTY:-}").unwrap();
        assert_eq!(result, "key: ");
    }

    #[test]
    fn set_var_overrides_default() {
        env_set_var("POND_TEST_OVERRIDE", "real-value");
        let result = substitute_env_vars("key: ${env:POND_TEST_OVERRIDE:-fallback}").unwrap();
        assert_eq!(result, "key: real-value");
        env_remove_var("POND_TEST_OVERRIDE");
    }

    #[test]
    fn double_dollar_becomes_literal() {
        let result = substitute_env_vars("price: $$100").unwrap();
        assert_eq!(result, "price: $100");
    }

    #[test]
    fn multiple_substitutions() {
        env_set_var("POND_TEST_HOST", "myhost");
        env_set_var("POND_TEST_PORT", "1234");
        let result =
            substitute_env_vars("endpoint: ${env:POND_TEST_HOST}:${env:POND_TEST_PORT}").unwrap();
        assert_eq!(result, "endpoint: myhost:1234");
        env_remove_var("POND_TEST_HOST");
        env_remove_var("POND_TEST_PORT");
    }

    #[test]
    fn non_env_provider_passed_through() {
        let result = substitute_env_vars("value: ${file:path/to/secret}").unwrap();
        assert_eq!(result, "value: ${file:path/to/secret}");
    }

    #[test]
    fn unset_without_default_errors() {
        env_remove_var("POND_TEST_MISSING");
        let err = substitute_env_vars("key: ${env:POND_TEST_MISSING}");
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("POND_TEST_MISSING"));
    }

    #[test]
    fn unclosed_brace_passed_through() {
        let result = substitute_env_vars("value: ${env:NO_CLOSE").unwrap();
        assert_eq!(result, "value: ${env:NO_CLOSE");
    }

    #[test]
    fn multiline_yaml() {
        env_set_var("POND_TEST_SECRET", "s3cr3t");
        let yaml = "endpoint: \"http://localhost:9000\"\naccess_key: \"${env:POND_TEST_SECRET}\"\n";
        let result = substitute_env_vars(yaml).unwrap();
        assert_eq!(
            result,
            "endpoint: \"http://localhost:9000\"\naccess_key: \"s3cr3t\"\n"
        );
        env_remove_var("POND_TEST_SECRET");
    }

    #[test]
    fn has_env_refs_detection() {
        assert!(has_env_refs("key: ${env:FOO}"));
        assert!(has_env_refs("${env:A:-default}"));
        assert!(!has_env_refs("key: value"));
        assert!(!has_env_refs("key: ${file:path}"));
        assert!(!has_env_refs("key: $FOO"));
    }

    #[test]
    fn bare_dollar_preserved() {
        let result = substitute_env_vars("$FOO and ${notenv} end").unwrap();
        assert_eq!(result, "$FOO and ${notenv} end");
    }
}
