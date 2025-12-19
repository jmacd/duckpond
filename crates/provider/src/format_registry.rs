// SPDX-FileCopyrightText: 2025 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Format Provider Registry
//!
//! Central registry for all format providers (CSV, OtelJson, etc.)
//! Uses linkme for compile-time registration similar to DynamicFactory

use crate::FormatProvider;
use linkme::distributed_slice;
use std::sync::Arc;

/// Format provider entry for distributed slice registration
pub struct FormatProviderEntry {
    /// Format scheme name (e.g., "csv", "oteljson")
    pub scheme: &'static str,
    /// Factory function to create provider instance
    pub create: fn() -> Arc<dyn FormatProvider>,
}

/// Distributed slice containing all registered format providers
/// linkme's distributed_slice uses #[link_section] which is considered unsafe
#[allow(unsafe_code)]
#[allow(clippy::declare_interior_mutable_const)]
#[distributed_slice]
pub static FORMAT_PROVIDERS: [FormatProviderEntry];

/// Format provider registry
pub struct FormatRegistry;

impl FormatRegistry {
    /// Get a format provider by scheme
    #[must_use]
    pub fn get_provider(scheme: &str) -> Option<Arc<dyn FormatProvider>> {
        FORMAT_PROVIDERS
            .iter()
            .find(|entry| entry.scheme == scheme)
            .map(|entry| (entry.create)())
    }

    /// List all available format providers
    #[must_use]
    pub fn list_providers() -> Vec<&'static str> {
        FORMAT_PROVIDERS.iter().map(|entry| entry.scheme).collect()
    }
}

/// Macro to register a format provider
///
/// Usage:
/// ```ignore
/// register_format_provider!(
///     scheme: "csv",
///     provider: CsvProvider::new
/// );
/// ```
#[macro_export]
macro_rules! register_format_provider {
    (scheme: $scheme:expr, provider: $provider:expr) => {
        paste::paste! {
            #[allow(unsafe_code)]
            #[linkme::distributed_slice($crate::format_registry::FORMAT_PROVIDERS)]
            static [<FORMAT_PROVIDER_ $scheme:snake:upper>]: $crate::format_registry::FormatProviderEntry =
                $crate::format_registry::FormatProviderEntry {
                    scheme: $scheme,
                    create: || std::sync::Arc::new($provider()),
                };
        }
    };
}
