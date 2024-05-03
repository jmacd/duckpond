mod client;
mod constant;
pub mod error;
mod load;
mod model;

use std::rc::Rc;

use chrono::offset::Utc;
use chrono::DateTime;
use chrono::SecondsFormat;

use error::Error;
use client::Client;
use client::ClientCall;
use model::Mapping;
use model::Names;
use model::Location;
use model::LocationReadings;

use std::time;
use std::fs::File;

use parquet::{
    arrow::ArrowWriter, basic::Compression,
    basic::ZstdLevel, file::properties::WriterProperties,
};

use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;

use serde::Serialize;

use anyhow::{Context, Result};

use arrow::datatypes::{DataType, Field, FieldRef, Fields};

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

fn mapping_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("index", DataType::Utf8, false)),
        Arc::new(Field::new("value", DataType::Utf8, false)),
    ]
}

fn readings_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("location_id", DataType::UInt64, false)),
	// @@@
    ]
}

fn evar(name: &str) -> Result<String, Error> {
    Ok(env::var(name)
       .with_context(|| format!("{name} is not set"))?)
}

const HYDROVU_CLIENT_ID_ENV: &str = "HYDROVU_CLIENT_ID";
const HYDROVU_CLIENT_SECRET_ENV: &str = "HYDROVU_CLIENT_SECRET";

fn creds() -> Result<(String, String), Error> {
    Ok((
        evar(HYDROVU_CLIENT_ID_ENV)?,
        evar(HYDROVU_CLIENT_SECRET_ENV)?,
    ))
}

fn fetch_names(client: Rc<Client>) -> ClientCall<Names> {
    Client::fetch_json(client, constant::names_url())
}

fn fetch_locations(client: Rc<Client>) -> ClientCall<Vec<Location>> {
    Client::fetch_json(client, constant::locations_url())
}

fn fetch_data(client: Rc<Client>, id: i64, start: i64, end: i64) -> ClientCall<LocationReadings> {
    Client::fetch_json(client, constant::location_url(id, start, end))
}

fn write_units(mapping: BTreeMap<String, String>) -> Result<(), Error> {
    write_mapping("units.parquet", mapping)
}

fn write_parameters(mapping: BTreeMap<String, String>) -> Result<(), Error> {
    write_mapping("params.parquet", mapping)
}

fn write_mapping(name: &str, mapping: BTreeMap<String, String>) -> Result<(), Error> {
    let result = mapping
        .into_iter()
        .map(|(x, y)| -> Mapping {
            Mapping {
                index: x,
                value: y,
            }
        })
        .collect::<Vec<_>>();

    write_file(name, result, mapping_fields().as_slice())
}

fn write_locations(locations: Vec<Location>) -> Result<(), Error> {
    let result = locations.to_vec();

    write_file("locations.parquet", result, location_fields().as_slice())
}

fn write_file<T: Serialize>(
    name: &str,
    records: Vec<T>,
    fields: &[Arc<Field>],
) -> Result<(), Error> {
    let batch = serde_arrow::to_record_batch(fields, &records)
	.with_context(|| "serialize arrow data failed")?;

    let file = File::create(name)
	.with_context(|| format!("open parquet file {}", name))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(6)
					   .with_context(|| "invalid zstd level 6")?))
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
	.with_context(||"new arrow writer failed")?;

    writer.write(&batch)
	.with_context(|| "write parquet data failed")?;
    writer.close()
	.with_context(|| "close parquet file failed")?;

    Ok(())
}

pub fn sync() -> Result<(), Error> {
    let client = Rc::new(Client::new(creds()?)?);

    // convert list of results to result of lists
    let names: Result<Vec<_>, _> = fetch_names(client.clone()).collect();
    let (ulist, plist): (Vec<_>, Vec<_>) =
        names?.into_iter().map(|x| (x.units, x.parameters)).unzip();

    let units = ulist
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .unwrap();
    let params = plist
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .unwrap();

    let locs: Result<Vec<_>, _> = fetch_locations(client.clone()).collect();
    let locations = locs?
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .unwrap();

    write_units(units)?;
    write_parameters(params)?;
    write_locations(locations)?;
    Ok(())
}

fn utc2date(utc: i64) -> String {
    DateTime::from_timestamp(utc, 0).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn read() -> Result<(), Error> {
    let client = Rc::new(Client::new(creds()?)?);
    let now = Utc::now();
    let locs = load::open_locations()?;

    let name = "data.parquet";
    let file = File::create(name)
     	.with_context(|| format!("open parquet file {}", name))?;

    let mut records: Vec<LocationReadings> = Vec::new();
    for loc in locs {
        for one_data in fetch_data(client.clone(), loc.id, 0, now.timestamp()) {
	    let one = one_data?;
	    
	    eprintln!("location {} [{}] {} params {} points {}..{}",
		      loc.name,
		      loc.id,
		      one.parameters.len(),
		      one.parameters.iter().fold(0, |acc, e| acc + e.readings.len()),
		      utc2date(
			  one.parameters.iter().map(|x| &x.readings)
			      .flatten().fold(std::i64::MAX, |a,b| a.min(b.timestamp)),
		      ),
		      utc2date(
			  one.parameters.iter().map(|x| &x.readings)
			      .flatten().fold(std::i64::MIN, |a,b| a.max(b.timestamp)),
		      ),
	    );
	    //eprintln!("{:?}", one);
	    records.push(one);
	    std::thread::sleep(time::Duration::from_millis(100));
	}
    }
    let batch = serde_arrow::to_record_batch(readings_fields().as_slice(), &records)
	.with_context(|| "serialize arrow data failed")?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(6)
					   .with_context(|| "invalid zstd level 6")?))
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
	.with_context(||"new arrow writer failed")?;

    writer.write(&batch)
	.with_context(|| "write parquet data failed")?;
    writer.close()
	.with_context(|| "close parquet file failed")?;

    Ok(())
}
