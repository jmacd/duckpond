// SPDX-FileCopyrightText: 2026 Caspar Water Company
//
// SPDX-License-Identifier: Apache-2.0

//! Register S3-compatible storage (like R2) with Delta Lake without AWS SDK dependencies

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
pub struct S3CompatibleStoreFactory {}

impl ObjectStoreFactory for S3CompatibleStoreFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        config: &StorageConfig,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        log::debug!(
            "S3CompatibleStoreFactory::parse_url_opts called for URL: {}",
            url
        );
        log::debug!("Config options: {:?}", config.raw);

        // Build the S3 client using AmazonS3Builder
        let mut builder = AmazonS3Builder::new().with_url(url.to_string());

        // Apply configuration options from the storage config
        for (key, value) in config.raw.iter() {
            if let Ok(config_key) = AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()) {
                log::debug!("  Setting S3 config: {:?} = {}", config_key, value);
                builder = builder.with_config(config_key, value.clone());
            } else {
                log::debug!("  Ignoring unknown config key: {}", key);
            }
        }

        // Parse the path from the URL
        let (_, path) =
            ObjectStoreScheme::parse(url).map_err(|e| DeltaTableError::GenericError {
                source: Box::new(e),
            })?;
        let prefix = Path::parse(path)?;

        // Build the store
        let store = builder.build().map_err(|e| DeltaTableError::GenericError {
            source: Box::new(e),
        })?;

        log::debug!("Successfully built S3-compatible object store");
        Ok((Arc::new(store), prefix))
    }
}

/// LogStore factory for S3-compatible storage - uses the default Delta Lake logstore
#[derive(Clone, Default, Debug)]
pub struct S3CompatibleLogStoreFactory {}

impl LogStoreFactory for S3CompatibleLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: ObjectStoreRef,
        root_store: ObjectStoreRef,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        log::debug!(
            "S3CompatibleLogStoreFactory::with_options called for URL: {}",
            location
        );
        // Use the default logstore implementation
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

/// Register S3-compatible storage handlers for s3:// and s3a:// schemes
pub fn register_s3_handlers() {
    log::info!("Registering S3-compatible storage handlers for s3:// and s3a:// schemes");
    let object_factory = Arc::new(S3CompatibleStoreFactory::default());
    let log_factory = Arc::new(S3CompatibleLogStoreFactory::default());

    for scheme in ["s3", "s3a"] {
        let url = Url::parse(&format!("{scheme}://")).expect("valid scheme URL");
        log::info!("  Registering scheme: {}", scheme);
        object_store_factories().insert(url.clone(), object_factory.clone());
        logstore_factories().insert(url, log_factory.clone());
    }
    log::info!("S3 handler registration complete");
}
