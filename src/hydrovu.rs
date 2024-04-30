use oauth2::{
    basic::BasicClient, reqwest::http_client, AuthUrl, ClientId, ClientSecret, Scope,
    TokenResponse, TokenUrl,
};

use std::time;
use std::fs::File;

use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    arrow::ArrowWriter, basic::Compression,
    basic::ZstdLevel, file::properties::WriterProperties,
};

use std::marker::PhantomData;

use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use anyhow::{Context, Result};

use arrow::datatypes::{DataType, Field, FieldRef, Fields};

const BASE_URL: &str = "https://www.hydrovu.com";

fn combine(a: &str, b: &str) -> String {
    return format!("{a}/public-api/{b}");
}

fn names_url() -> String {
    return combine(BASE_URL, "v1/sispec/friendlynames");
}
fn locations_url() -> String {
    return combine(BASE_URL, "v1/locations/list");
}
fn location_url(id: i64, start_time: i64, end_time: i64) -> String {
    return combine(
        BASE_URL,
        format!("v1/locations/{id}/data?startTime={start_time}&endTime={end_time}").as_str(),
    );
}
fn auth_url() -> String {
    return combine(BASE_URL, "oauth/authorize");
}
fn token_url() -> String {
    return combine(BASE_URL, "oauth/token");
}

pub struct Client {
    client: reqwest::blocking::Client,
    token: String,
}

pub struct ClientCall<'a, T: for<'de> serde::Deserialize<'de>> {
    client: &'a Client,
    url: String,
    next: Option<String>,
    phan: PhantomData<T>,
}

// Names is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug)]
pub struct Names {
    parameters: HashMap<String, String>,
    units: HashMap<String, String>,
}

// Location is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Location {
    description: String,
    id: i64,
    name: String,
    gps: LatLong,
}

// LatLong is documented at https://www.hydrovu.com/public-api/docs/index.html
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LatLong {
    latitude: f64,
    longitude: f64,
}

// LocationReadings is a batch of timeseries.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LocationReadings {
    location_id: i64,
    parameters: Vec<ParameterInfo>,
}

// ParameterInfo is a single timeseries.
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ParameterInfo {
    custom_parameter: bool,
    parameter_id: String,
    unit_id: String,
    readings: Vec<Reading>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Reading {
    timestamp: i64,
    value: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Vu {
    pub units: HashMap<String, String>,
    pub params: HashMap<String, String>,
    pub locations: Vec<Location>,
}

fn location_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("description", DataType::Utf8, false)),
        Arc::new(Field::new("id", DataType::UInt64, false)),
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new(
            "gps",
            DataType::Struct(Fields::from(vec![
                Field::new("latitude", DataType::Float64, false),
                Field::new("longitude", DataType::Float64, false),
            ])),
            false,
        )),
    ]
}

#[derive(Serialize, Deserialize, Debug)]
struct Mapping {
    index: String,
    value: String,
}

fn mapping_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("index", DataType::Utf8, false)),
        Arc::new(Field::new("value", DataType::Utf8, false)),
    ]
}

fn evar(name: &str) -> Result<String, Box<dyn Error>> {
    Ok(env::var(name).with_context(|| format!("{name} is not set"))?)
}

impl Client {
    fn fetch_json<T: for<'de> serde::Deserialize<'de>>(&self, url: String) -> ClientCall<T> {
        ClientCall::<T> {
            client: self,
            url: url,
            next: Some("".to_string()),
            phan: PhantomData,
        }
    }
}

const HYDROVU_CLIENT_ID_ENV: &str = "HYDROVU_CLIENT_ID";
const HYDROVU_CLIENT_SECRET_ENV: &str = "HYDROVU_CLIENT_SECRET";

fn creds() -> Result<(String, String), Box<dyn Error>> {
    Ok((
        evar(HYDROVU_CLIENT_ID_ENV)?,
        evar(HYDROVU_CLIENT_SECRET_ENV)?,
    ))
}

pub fn new_client() -> Result<Client, Box<dyn Error>> {
    let (client_id, client_secret) = creds()?;

    let oauth = BasicClient::new(
        ClientId::new(client_id.to_string()),
        Some(ClientSecret::new(client_secret.to_string())),
        AuthUrl::new(auth_url())?,
        Some(TokenUrl::new(token_url())?),
    );

    let token_result = oauth
        .exchange_client_credentials()
        .add_scope(Scope::new("read:locations".to_string()))
        .add_scope(Scope::new("read:data".to_string()))
        .request(http_client)?;

    Ok(Client {
        client: reqwest::blocking::Client::new(),
        token: format!("Bearer {}", token_result.access_token().secret()),
    })
}

impl<T: for<'de> serde::Deserialize<'de>> Iterator for ClientCall<'_, T> {
    type Item = Result<T, Box<dyn Error>>;

    fn next(&mut self) -> Option<Result<T, Box<dyn Error>>> {
        if let None = self.next {
            return None;
        }
        match self.client.call_api(self.url.to_string()) {
            Ok((value, next)) => {
                self.next = next;
                Some(Ok(value))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

impl Client {
    fn call_api<T: for<'de> serde::Deserialize<'de>>(
        &self,
        url: String,
    ) -> Result<(T, Option<String>), Box<dyn Error>> {
        let resp = self
            .client
            .get(url)
            .header("authorization", &self.token)
            .send()?;

        let next = next_header(&resp)?;
        let one = serde_json::from_reader(resp)?;
        Ok((one, next))
    }

    fn fetch_names(&self) -> ClientCall<Names> {
        self.fetch_json(names_url())
    }

    fn fetch_locations(&self) -> ClientCall<Vec<Location>> {
        self.fetch_json(locations_url())
    }

    fn fetch_data(&self, id: i64, start: i64, end: i64) -> ClientCall<LocationReadings> {
        self.fetch_json(location_url(id, start, end))
    }
}

fn next_header(resp: &reqwest::blocking::Response) -> Result<Option<String>, Box<dyn Error>> {
    let next = resp.headers().get("x-isi-next-page");
    match next {
        Some(val) => {
            eprintln!("isi-next {:?}", val);
            Ok(Some(val.to_str()?.to_string()))
        }
        None => Ok(None),
    }
}

fn write_units(mapping: HashMap<String, String>) -> Result<(), Box<dyn Error>> {
    write_mapping("units.parquet", mapping)
}

fn write_parameters(mapping: HashMap<String, String>) -> Result<(), Box<dyn Error>> {
    write_mapping("params.parquet", mapping)
}

fn write_mapping(name: &str, mapping: HashMap<String, String>) -> Result<(), Box<dyn Error>> {
    let result: Result<Vec<Mapping>, _> = mapping
        .iter()
        .map(|(ref x, ref y)| -> Result<Mapping, Box<dyn Error>> {
            Ok(Mapping {
                index: x.to_string(),
                value: y.to_string(),
            })
        })
        .collect();
    let mut records = result?;

    records.sort_unstable_by(|a, b| a.index.cmp(&b.index));

    write_file(name, records, mapping_fields().as_slice())
}

fn write_locations(locations: Vec<Location>) -> Result<(), Box<dyn Error>> {
    let mut sorted = locations.to_vec();

    sorted.sort_unstable_by(|a, b| a.id.cmp(&b.id));

    write_file("locations.parquet", sorted, location_fields().as_slice())
}

fn write_file<T: Serialize>(
    name: &str,
    records: Vec<T>,
    fields: &[Arc<Field>],
) -> Result<(), Box<dyn Error>> {
    let batch = serde_arrow::to_record_batch(fields, &records)?;

    let file = File::create(name)?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(6)?))
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

pub fn sync() -> Result<(), Box<dyn Error>> {
    let client = new_client()?;

    // convert list of results to result of lists
    let names: Result<Vec<Names>, _> = client.fetch_names().collect();
    let (ulist, plist): (Vec<_>, Vec<_>) =
        names?.into_iter().map(|x| (x.units, x.parameters)).unzip();

    let units: HashMap<_, _> = ulist
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .unwrap();
    let params: HashMap<_, _> = plist
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .unwrap();

    let locs: Result<Vec<Vec<Location>>, _> = client.fetch_locations().collect();
    let locations = locs?
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .unwrap();

    write_units(units)?;
    write_parameters(params)?;
    write_locations(locations)?;
    Ok(())
}

pub fn read() -> Result<(), Box<dyn Error>> {
    let client = new_client()?;
    let now = chrono::offset::Utc::now();
    let locs = load_locations()?;
    let vu = load()?;
    for loc in locs {
        for one_data in client.fetch_data(loc.id, 0, now.timestamp()) {
	    let data = one_data?;
            eprintln!("-----");
            eprintln!("loc {:?}", loc.name);
            for param in data.parameters {
                eprintln!(
                    "param {:?}",
                    vu.params
                        .get(&param.parameter_id)
                        .ok_or(format!("paramId {} not found", param.parameter_id))?
                );
                eprintln!(
                    "unit {:?}",
                    vu.units
                        .get(&param.unit_id)
                        .ok_or(format!("unitId {} not found", param.unit_id))?
                );

                for dat in param.readings {
                    let dt = DateTime::<Utc>::from_timestamp(dat.timestamp, 0);

                    eprintln!("data {:?} {:?}", dt, dat.value)
                }
            }
	    let sec = time::Duration::from_millis(1000);
	    std::thread::sleep(sec);
        }
    }
    Ok(())
}

fn load_units() -> Result<HashMap<String, String>, Box<dyn Error>> {
    return load_mapping("units.parquet");
}

fn load_parameters() -> Result<HashMap<String, String>, Box<dyn Error>> {
    return load_mapping("params.parquet");
}

fn load_locations() -> Result<Vec<Location>, Box<dyn Error>> {
    return load_file("locations.parquet");
}

fn load_file<T: for<'a> Deserialize<'a>>(name: &str) -> Result<Vec<T>, Box<dyn Error>> {
    let file = File::open(name)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;
    Ok(serde_arrow::from_record_batch(&reader.next().unwrap()?)?)
}

fn load_mapping(name: &str) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let file = File::open(name)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;

    let items: Vec<Mapping> = serde_arrow::from_record_batch(&reader.next().unwrap()?)?;
    return Ok(items.into_iter().map(|x| (x.index, x.value)).collect())
}

pub fn load() -> Result<Vu, Box<dyn Error>> {
    Ok(Vu {
        units: load_units()?,
        params: load_parameters()?,
        locations: load_locations()?,
    })
}
