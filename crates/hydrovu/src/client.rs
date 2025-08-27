use crate::models::{Location, LocationReadings, Names};
use anyhow::{Context, Result, anyhow};
use diagnostics::*;
use oauth2::{
    AuthUrl, ClientId, ClientSecret, Scope, TokenResponse, TokenUrl, basic::BasicClient,
    reqwest::async_http_client,
};
use std::time::Duration;

const BASE_URL: &str = "https://www.hydrovu.com";
const TIMEOUT_SECONDS: u64 = 60;

/// Async HydroVu API client
#[derive(Clone)]
pub struct Client {
    http_client: reqwest::Client,
    token: String,
}

impl Client {
    /// Create a new client with OAuth2 authentication
    pub async fn new(client_id: String, client_secret: String) -> Result<Self> {
        let oauth_client = BasicClient::new(
            ClientId::new(client_id),
            Some(ClientSecret::new(client_secret)),
            AuthUrl::new(Self::auth_url()).with_context(|| "Failed to create authorization URL")?,
            Some(TokenUrl::new(Self::token_url()).with_context(|| "Failed to create token URL")?),
        );

        let token_result = oauth_client
            .exchange_client_credentials()
            .add_scope(Scope::new("read:locations".to_string()))
            .add_scope(Scope::new("read:data".to_string()))
            .request_async(async_http_client)
            .await;

        let token = match token_result {
            Ok(token) => token.access_token().secret().clone(),
            Err(e) => {
                // Provide more detailed error information
                return Err(anyhow!(
                    "OAuth2 authentication failed: {}\n\
                    This could be due to:\n\
                    - Incorrect client_id or client_secret\n\
                    - Network connectivity issues\n\
                    - HydroVu API service unavailable\n\
                    - Invalid OAuth scopes\n\
                    \n\
                    Please verify your credentials at: https://www.hydrovu.com/public-api/docs/",
                    e
                ));
            }
        };

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(TIMEOUT_SECONDS))
            .build()
            .with_context(|| "Failed to create HTTP client")?;

        Ok(Client { http_client, token })
    }

    /// Fetch parameter and unit names/mappings
    pub async fn fetch_names(&self) -> Result<Names> {
        let url = Self::names_url();
        self.fetch_json(&url).await
    }

    /// Test authentication by making a simple API call
    pub async fn test_authentication(&self) -> Result<()> {
        println!("Testing HydroVu API authentication...");

        // Try to fetch names as a simple test
        match self.fetch_names().await {
            Ok(_names) => {
                println!("✓ Authentication successful - API is responding");
                Ok(())
            }
            Err(e) => {
                println!("✗ Authentication test failed");
                Err(e.context("Authentication test failed"))
            }
        }
    }

    /// Fetch list of available locations
    pub async fn fetch_locations(&self) -> Result<Vec<Location>> {
        let url = Self::locations_url();
        self.fetch_json(&url).await
    }

    /// Fetch data for a location with full pagination support and optional record limit
    pub async fn fetch_location_data(
        &self,
        location_id: i64,
        start_time: i64, // @@@ what units?
        stop_at_records: usize,
    ) -> Result<LocationReadings> {
        let base_url = Self::location_data_url(location_id, start_time, None);
        let mut all_data = None;
        let mut next_page_token: Option<String> = None;
        let mut page_count = 0;
        let mut total_records = 0;

        debug!("Starting fetch location {location_id} since timestamp {start_time}");

        loop {
            page_count += 1;

            // Build request with optional pagination token
            let mut request = self.http_client.get(&base_url).bearer_auth(&self.token);

            if let Some(ref token) = next_page_token {
                debug!("Using pagination token for page {page_count}: {token}");
                request = request.header("x-isi-start-page", token);
            }

            let response = request
                .send()
                .await
                .with_context(|| format!("Failed to send request to {}", base_url))?;

            if !response.status().is_success() {
                let status = response.status();
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                return Err(anyhow!(
                    "HTTP {} error from {}: {}",
                    status,
                    base_url,
                    error_text
                ));
            }

            // Check for next page token in response headers
            next_page_token = response
                .headers()
                .get("x-isi-next-page")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let json_text = response
                .text()
                .await
                .with_context(|| "Failed to read response body")?;

            let page_data: LocationReadings = serde_json::from_str(&json_text)
                .with_context(|| format!("Failed to parse JSON response from {}", base_url))?;

            // Count records on this page
            let page_records: usize = page_data
                .parameters
                .iter()
                .map(|param| param.readings.len())
                .sum();

            total_records += page_records;
            info!(
                "Page {page_count} for location {location_id}: {page_records} records (total: {total_records})"
            );

            // Merge this page with accumulated data
            match &mut all_data {
                None => all_data = Some(page_data),
                Some(accumulated) => {
                    // Merge the parameters from this page
                    for param_data in page_data.parameters {
                        // Find existing parameter or add new one
                        match accumulated
                            .parameters
                            .iter_mut()
                            .find(|p| p.parameter_id == param_data.parameter_id)
                        {
                            Some(existing_param) => {
                                existing_param.readings.extend(param_data.readings);
                            }
                            None => {
                                accumulated.parameters.push(param_data);
                            }
                        }
                    }
                }
            }

            // Continue if we have a next page token and haven't hit limit, otherwise break
            if next_page_token.is_none() {
                info!("No more pages available for location {location_id}");
                break;
            } else if total_records >= stop_at_records {
                info!("Reached record limit ({stop_at_records}) for location {location_id}");
                break;
            } else {
                debug!("Continuing to next page for location {location_id}...");
            }
        }

        info!(
            "Completed fetch for location {location_id}: {page_count} pages, {total_records} total records"
        );
        all_data.ok_or_else(|| anyhow!("No data received from API"))
    }

    /// Generic JSON fetch with authentication and pagination support
    async fn fetch_json<T>(&self, url: &str) -> Result<T>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let (data, _next_page) = self.fetch_json_paginated(url, None).await?;
        Ok(data)
    }

    /// Generic JSON fetch with authentication and pagination support
    async fn fetch_json_paginated<T>(
        &self,
        url: &str,
        start_page: Option<&str>,
    ) -> Result<(T, Option<String>)>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let mut request = self.http_client.get(url).bearer_auth(&self.token);

        // Add pagination header if provided
        if let Some(page_token) = start_page {
            request = request.header("x-isi-start-page", page_token);
            debug!("Using pagination token for {url}: {page_token}");
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send request to {}", url))?;

        // Extract next page token from response headers
        let next_page = response
            .headers()
            .get("x-isi-next-page")
            .and_then(|header_value| header_value.to_str().ok())
            .map(|s| {
                debug!("Found next page token for {url}: {s}");
                s.to_string()
            });

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(anyhow!(
                "HTTP {} error from {}: {}",
                status,
                url,
                error_text
            ));
        }

        let json_text = response
            .text()
            .await
            .with_context(|| "Failed to read response body")?;

        let data: T = serde_json::from_str(&json_text)
            .with_context(|| format!("Failed to parse JSON response from {}", url))?;

        Ok((data, next_page))
    }

    // URL construction helpers
    fn combine(base: &str, path: &str) -> String {
        format!("{}/public-api/{}", base, path)
    }

    fn names_url() -> String {
        Self::combine(BASE_URL, "v1/sispec/friendlynames")
    }

    fn locations_url() -> String {
        Self::combine(BASE_URL, "v1/locations/list")
    }

    fn location_data_url(location_id: i64, start_time: i64, end_time: Option<i64>) -> String {
        let mut url = format!("v1/locations/{}/data?startTime={}", location_id, start_time);

        if let Some(end_time) = end_time {
            url.push_str(&format!("&endTime={}", end_time));
        }

        Self::combine(BASE_URL, &url)
    }

    fn auth_url() -> String {
        Self::combine(BASE_URL, "oauth/authorize")
    }

    fn token_url() -> String {
        Self::combine(BASE_URL, "oauth/token")
    }
}

/// Paginated client call for handling large result sets
pub struct PaginatedCall<T> {
    client: Client,
    next_url: Option<String>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> PaginatedCall<T>
where
    T: for<'de> serde::Deserialize<'de>,
{
    /// Create a new paginated call
    pub fn new(client: Client, url: String) -> Self {
        Self {
            client,
            next_url: Some(url),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Fetch the next page of results
    pub async fn next_page(&mut self) -> Result<Option<T>> {
        if let Some(url) = self.next_url.take() {
            let result = self.client.fetch_json(&url).await?;

            // For HydroVu API, pagination is typically handled through
            // URL parameters, but the exact mechanism may vary by endpoint
            // For now, we assume single-page responses
            self.next_url = None;

            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    /// Collect all pages into a single result
    pub async fn collect_all(mut self) -> Result<Vec<T>> {
        let mut results = Vec::new();

        while let Some(page) = self.next_page().await? {
            results.push(page);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_construction() {
        assert_eq!(
            Client::names_url(),
            "https://www.hydrovu.com/public-api/v1/sispec/friendlynames"
        );

        assert_eq!(
            Client::locations_url(),
            "https://www.hydrovu.com/public-api/v1/locations/list"
        );

        assert_eq!(
            Client::location_data_url(123, 1609459200000, None),
            "https://www.hydrovu.com/public-api/v1/locations/123/data?startTime=1609459200000"
        );

        assert_eq!(
            Client::location_data_url(123, 1609459200000, Some(1609545600000)),
            "https://www.hydrovu.com/public-api/v1/locations/123/data?startTime=1609459200000&endTime=1609545600000"
        );
    }
}
