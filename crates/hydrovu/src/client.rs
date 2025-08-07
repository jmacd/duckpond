use crate::models::{Names, Location, LocationReadings};
use anyhow::{Context, Result, anyhow};
use oauth2::{
    AuthUrl, ClientId, ClientSecret, Scope, TokenResponse, TokenUrl,
    basic::BasicClient, reqwest::async_http_client,
};
use std::time::Duration;

const BASE_URL: &str = "https://www.hydrovu.com";
const TIMEOUT_SECONDS: u64 = 60;

/// Async HydroVu API client
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
            AuthUrl::new(Self::auth_url())
                .with_context(|| "Failed to create authorization URL")?,
            Some(TokenUrl::new(Self::token_url())
                .with_context(|| "Failed to create token URL")?),
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

        Ok(Client {
            http_client,
            token,
        })
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

    /// Fetch data for a specific location within a time range
    pub async fn fetch_location_data(
        &self,
        location_id: i64,
        start_time: i64,
        end_time: Option<i64>,
    ) -> Result<LocationReadings> {
        let url = Self::location_data_url(location_id, start_time, end_time);
        self.fetch_json(&url).await
    }

    /// Fetch data for a location since a specific timestamp
    pub async fn fetch_location_data_since(
        &self,
        location_id: i64,
        since_timestamp: i64,
    ) -> Result<LocationReadings> {
        self.fetch_location_data(location_id, since_timestamp, None).await
    }

    /// Generic JSON fetch with authentication
    async fn fetch_json<T>(&self, url: &str) -> Result<T>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .with_context(|| format!("Failed to send request to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(anyhow!(
                "HTTP {} error from {}: {}", 
                status, url, error_text
            ));
        }

        let json_text = response
            .text()
            .await
            .with_context(|| "Failed to read response body")?;

        serde_json::from_str(&json_text)
            .with_context(|| format!("Failed to parse JSON response from {}", url))
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
        let mut url = format!(
            "v1/locations/{}/data?startTime={}", 
            location_id, start_time
        );
        
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
