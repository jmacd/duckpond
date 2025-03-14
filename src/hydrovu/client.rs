use super::constant;
use anyhow::{anyhow, Context, Result};
use std::marker::PhantomData;
use std::rc::Rc;
use std::time::Duration;
use backon::ExponentialBuilder;
use backon::BlockingRetryable;
use crate::hydrovu::model::{Names,Location,LocationReadings};

use oauth2::{
    basic::BasicClient, reqwest::http_client, AuthUrl, ClientId, ClientSecret, Scope,
    TokenResponse, TokenUrl,
};

pub struct Client {
    client: reqwest::blocking::Client,
    token: String,
}

pub struct ClientCall<T: for<'de> serde::Deserialize<'de>> {
    count: u32,
    client: Rc<Client>,
    url: String,
    next: Option<String>,
    phan: PhantomData<T>,
}

impl Client {
    pub fn fetch_json<T: for<'de> serde::Deserialize<'de>>(
        client: Rc<Client>,
        url: String,
    ) -> ClientCall<T> {
        ClientCall::<T> {
	    count: 0,
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
            AuthUrl::new(constant::auth_url()).with_context(|| "authorization failed")?,
            Some(TokenUrl::new(constant::token_url()).with_context(|| "invalid token url")?),
        );

        let token_result = oauth
            .exchange_client_credentials()
            .add_scope(Scope::new("read:locations".to_string()))
            .add_scope(Scope::new("read:data".to_string()))
            .request(http_client);

        match token_result {
            Ok(token) => Ok(Client {
                client: reqwest::blocking::Client::builder().timeout(Duration::from_secs(60)).build()?,
                token: format!("Bearer {}", token.access_token().secret()),
            }),
            Err(x) => Err(anyhow!("oauth failed: {:?}", x)),
        }
    }

    fn call_api<T: for<'de> serde::Deserialize<'de>>(
        &self,
        url: String,
	_sequence: u32,
        prev: &Option<String>,
    ) -> Result<(T, Option<String>)> {
	let cb = || -> Result<(T, Option<String>)> {
            let mut bldr = self.client.get(&url).header("authorization", &self.token);
            if let Some(hdr) = prev {
		bldr = bldr.header("x-isi-start-page", hdr);
		//eprintln!("{}: fetch data url {} hdr {}", sequence, &url, hdr);
            } else {
		//eprintln!("{}: fetch data url {} first", sequence, &url);
	    }
            let resp = bldr.send().with_context(|| "api request failed")?;
            let next = next_header(&resp)?;

	    let result = resp.error_for_status_ref().map(|_| ());
	    let status = resp.status();
            let text = resp.text().with_context(|| "api response error")?;

	    if let Err(err) = result {
		if status.is_client_error() {
		} else if status.is_server_error() {
		}
		return Err(anyhow!("api response status: {:?}: {}", err, &text));
	    }
 
            let one = serde_json::from_str(&text)
		.with_context(|| format!("api response parse error {:?}", text))?;
	    Ok((one, next))
	};

	cb.retry(ExponentialBuilder::default().without_max_times())
	    .notify(|err: &anyhow::Error, dur: Duration| {
		eprintln!("retrying error {} after sleep sleeping {:?}", err, dur);
	    })
	    .call()
    }
}

impl<T: for<'de> serde::Deserialize<'de>> Iterator for ClientCall<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
	self.count += 1;
	
        if let None = self.next {
            return None;
        }
        match self.client.call_api(self.url.to_string(), self.count, &self.next) {
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

    // let h2 = resp.headers().get("x-isi-requests-this-minute");
    // let h3 = resp.headers().get("x-isi-requests-timeout");
    // eprintln!("h2h3 {:?} {:?}", h2, h3);
    
    match next {
        Some(val) => Ok(Some(
            val.to_str()
                .with_context(|| "invalid utf-8 in ISI header")?
                .to_string(),
        )),
        None => Ok(None),
    }
}

pub fn fetch_names(client: Rc<Client>) -> ClientCall<Names> {
    Client::fetch_json(client, constant::names_url())
}

pub fn fetch_locations(client: Rc<Client>) -> ClientCall<Vec<Location>> {
    Client::fetch_json(client, constant::locations_url())
}

pub fn fetch_data(
    client: Rc<Client>,
    id: i64,
    start: i64,
    end: Option<i64>,
) -> ClientCall<LocationReadings> {
    Client::fetch_json(client, constant::location_url(id, start, end))
}

