use std::marker::PhantomData;
use std::rc::Rc;
use super::constant;
use anyhow::{Result,Context};

use oauth2::{
    basic::BasicClient, reqwest::http_client, AuthUrl, ClientId, ClientSecret, Scope,
    TokenResponse, TokenUrl,
};

pub struct Client {
    client: reqwest::blocking::Client,
    token: String,
}

pub struct ClientCall<T: for<'de> serde::Deserialize<'de>> {
    client: Rc<Client>,
    url: String,
    next: Option<String>,
    phan: PhantomData<T>,
}

impl Client {
    pub fn fetch_json<T: for<'de> serde::Deserialize<'de>>(client: Rc<Client>, url: String) -> ClientCall<T> {
	ClientCall::<T> {
            client: client,
            url: url,
            next: Some("".to_string()),
            phan: PhantomData,
	}
    }

    pub fn new((client_id, client_secret): (String, String)) -> Result<Client> {
	let oauth = BasicClient::new(
            ClientId::new(client_id.to_string()),
            Some(ClientSecret::new(client_secret.to_string())),
            AuthUrl::new(constant::auth_url())
		.with_context(|| "authorization failed")?,
            Some(TokenUrl::new(constant::token_url())
		 .with_context(|| "invalid token url")?),
	);
	
	let token_result = oauth
            .exchange_client_credentials()
            .add_scope(Scope::new("read:locations".to_string()))
            .add_scope(Scope::new("read:data".to_string()))
            .request(http_client)
	    .with_context(|| "oauth2 request failed")?;
	
	Ok(Client {
            client: reqwest::blocking::Client::new(),
            token: format!("Bearer {}", token_result.access_token().secret()),
	})
    }

    fn call_api<T: for<'de> serde::Deserialize<'de>>(
        &self,
        url: String,
	prev: &Option<String>,
    ) -> Result<(T, Option<String>)> {
        let mut bldr = self
            .client
            .get(url)
            .header("authorization", &self.token);
	if let Some(hdr) = prev {
	    bldr = bldr.header("x-isi-start-page", hdr)
	}
        let resp = bldr.send()
	    .with_context(|| "api request failed")?;
        let next = next_header(&resp)?;

	// @@@
        // let one = serde_json::from_reader(resp)
	//     .with_context(|| format!("api response parse error {:?}", bytes.as_string()))?;
	
	let text = resp.text()
	    .with_context(|| "api response error")?;
	let one = serde_json::from_str(&text)
	    .with_context(|| format!("api response parse error {:?}", text))?;
        Ok((one, next))
    }
}

impl<T: for<'de> serde::Deserialize<'de>> Iterator for ClientCall<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        if let None = self.next {
            return None;
        }
        match self.client.call_api(self.url.to_string(), &self.next) {
            Ok((value, next)) => {
                self.next = next;
                Some(Ok(value))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

fn next_header(resp: &reqwest::blocking::Response) -> Result<Option<String>> {
    let next = resp.headers().get("x-isi-next-page");
    match next {
        Some(val) => {
            Ok(Some(val.to_str()
		    .with_context(|| "invalid utf-8 in ISI header")?.to_string()))
        }
        None => Ok(None),
    }
}
