// SPDX-License-Identifier: Apache-2.0

//! Register S3-compatible storage (MinIO, AWS S3, Cloudflare R2, etc.)
//! handlers with delta-rs WITHOUT depending on the `deltalake-aws`
//! crate (which has version-pinning issues at deltalake 0.30).
//!
//! Mirrors the registration pattern used by duckpond's
//! `crates/remote/src/s3_registration.rs`.  Call
//! [`register_s3_handlers`] once at process startup BEFORE opening
//! or creating any `s3://` URL.

use deltalake::logstore::{
    LogStore, LogStoreFactory, ObjectStoreFactory, ObjectStoreRef, StorageConfig, default_logstore,
    logstore_factories, object_store_factories,
};
use deltalake::{DeltaResult, DeltaTableError, Path};
use object_store::ObjectStoreScheme;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

#[derive(Clone, Default, Debug)]
struct S3CompatibleStoreFactory {}

impl ObjectStoreFactory for S3CompatibleStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let mut builder = AmazonS3Builder::new().with_url(url.to_string());
        for (key, value) in config.raw.iter() {
            if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                builder = builder.with_config(config_key, value.clone());
            }
        }
        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;
        let store = builder.build().map_err(|e| DeltaTableError::GenericError {
            source: Box::new(e),
        })?;
        Ok((Arc::new(store), prefix))
    }
}

#[derive(Clone, Default, Debug)]
struct S3CompatibleLogStoreFactory {}

impl LogStoreFactory for S3CompatibleLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Register S3-compatible storage handlers for `s3://` and `s3a://`
/// schemes.  Idempotent across multiple calls; safe to call from
/// multiple test fixtures via `std::sync::Once`.
pub fn register_s3_handlers() {
    let object_factory = Arc::new(S3CompatibleStoreFactory::default());
    let log_factory = Arc::new(S3CompatibleLogStoreFactory::default());

    for scheme in ["s3", "s3a"] {
        let url = Url::parse(&format!("{scheme}://")).expect("valid scheme URL");
        object_store_factories().insert(url.clone(), object_factory.clone());
        logstore_factories().insert(url, log_factory.clone());
    }
}
