mod client;
mod constant;
mod load;
mod model;

use std::rc::Rc;

use crate::pond;

use chrono::offset::FixedOffset;
use chrono::DateTime;
use chrono::SecondsFormat;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow_array::array::ArrayRef;
use client::Client;
use client::ClientCall;
use model::Location;
use model::LocationReadings;
use model::Mapping;
use model::Names;

use std::fs::File;
use std::path::Path;
use std::time;

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::env;

use std::sync::Arc;

use anyhow::{Context,Result};

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
        Arc::new(Field::new("index", DataType::Int16, false)),
        Arc::new(Field::new("value", DataType::Utf8, false)),
    ]
}

fn evar(name: &str) -> Result<String> {
    Ok(env::var(name).with_context(|| format!("{name} is not set"))?)
}

const HYDROVU_CLIENT_ID_ENV: &str = "HYDROVU_CLIENT_ID";
const HYDROVU_CLIENT_SECRET_ENV: &str = "HYDROVU_CLIENT_SECRET";

fn creds() -> Result<(String, String)> {
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

fn write_units(pond: &pond::Pond, mapping: BTreeMap<i16, String>) -> Result<()> {
    write_mapping(pond, "units.parquet", mapping)
}

fn write_parameters(pond: &pond::Pond, mapping: BTreeMap<i16, String>) -> Result<()> {
    write_mapping(pond, "params.parquet", mapping)
}

fn write_mapping<P: AsRef<Path>>(pond: &pond::Pond, name: P, mapping: BTreeMap<i16, String>) -> Result<()> {
    let result = mapping
        .into_iter()
        .map(|(x, y)| -> Mapping { Mapping { index: x, value: y } })
        .collect::<Vec<_>>();

    pond.write_file(name, result, mapping_fields().as_slice())
}

fn write_locations(pond: &pond::Pond, locations: Vec<Location>) -> Result<()> {
    let result = locations.to_vec();

    pond.write_file("locations.parquet", result, location_fields().as_slice())
}

fn ss2is(ss: (String, String)) -> Option<(i16, String)> {
    if let Ok(i) = ss.0.parse::<i16>() {
        Some((i, ss.1))
    } else {
        eprintln!("invalid index: {}", ss.0);
        None
    }
}

pub fn sync() -> Result<()> {
    let pond = pond::open()?;
    let client = Rc::new(Client::new(creds()?)?);

    // convert list of results to result of lists
    let names: Result<Vec<_>, _> = fetch_names(client.clone()).collect();
    let (ulist, plist): (Vec<_>, Vec<_>) =
        names?.into_iter().map(|x| (x.units, x.parameters)).unzip();

    let units = ulist
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .context("no units defined")?
        .into_iter()
        .filter_map(ss2is)
        .collect();

    let params = plist
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .context("no parameters defined")?
        .into_iter()
        .filter_map(ss2is)
        .collect();

    let locs: Result<Vec<_>, _> = fetch_locations(client.clone()).collect();
    let locations = locs?
        .into_iter()
        .reduce(|x, y| x.into_iter().chain(y).collect())
        .unwrap();

    write_units(&pond, units)?;
    write_parameters(&pond, params)?;
    write_locations(&pond, locations)?;
    Ok(())
}

pub fn utc2date(utc: i64) -> String {
    DateTime::from_timestamp(utc, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

struct Instrument {
    schema: Schema,
    file: File,
    tsb: Int64Builder,
    fbs: Vec<Float64Builder>,
}

pub fn read(until: &DateTime<FixedOffset>) -> Result<()> {
    let client = Rc::new(Client::new(creds()?)?);
    let vu = load::load()?;

    for loc in &vu.locations {
        let mut insts = BTreeMap::<String, Instrument>::new();

        for one_data in fetch_data(client.clone(), loc.id, 0, until.timestamp()) {
            let one = one_data?;

            eprintln!(
                "location {} [{}] {} params {} points {}..{}",
                loc.name,
                loc.id,
                one.parameters.len(),
                one.parameters
                    .iter()
                    .fold(0, |acc, e| acc + e.readings.len()),
                utc2date(
                    one.parameters
                        .iter()
                        .map(|x| &x.readings)
                        .flatten()
                        .fold(std::i64::MAX, |a, b| a.min(b.timestamp)),
                ),
                utc2date(
                    one.parameters
                        .iter()
                        .map(|x| &x.readings)
                        .flatten()
                        .fold(std::i64::MIN, |a, b| a.max(b.timestamp)),
                ),
            );

            let schema_parts: Vec<String> = one
                .parameters
                .iter()
                .map(|p| -> Result<String> {
                    let pu = vu.lookup_param_unit(p)?;
                    Ok(format!("{},{}", pu.0, pu.1))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let schema_str: String = schema_parts
                .iter()
                .fold(String::new(), |s, t| format!("{}:{}", s, t));

            let inst = insts.get(&schema_str);

            match inst {
                Some(_) => (),
                None => {
                    let fname = format!("data-{}-{}.parquet",
					loc.name.replace(" ", "_"),
					until);
                    let mut fields =
                        vec![Arc::new(Field::new("timestamp", DataType::Int64, false))];

                    // how to avoid the mut below?
                    let mut fvec = one
                        .parameters
                        .iter()
                        .map(|p| -> Result<Arc<Field>> {
                            let pu = vu.lookup_param_unit(p)?;
                            Ok(Arc::new(Field::new(
                                format!("{}.{}", pu.0, pu.1),
                                DataType::Float64,
                                true,
                            )))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    fields.append(&mut fvec);
                    let schema = Schema::new(fields);

                    // one timestamp builder, N float64 builders
                    let tsb = Int64Builder::default();
                    let fbs: Vec<_> = one
                        .parameters
                        .iter()
                        .map(|_| Float64Builder::default())
                        .collect();

                    insts.insert(
                        schema_str.clone(),
                        Instrument {
                            schema: schema,
                            file: File::create(&fname)
                                .with_context(|| format!("open parquet file {}", &fname))?,
                            tsb: tsb,
                            fbs: fbs,
                        },
                    );
                }
            }
            let inst = insts.get_mut(&schema_str).unwrap();

            // compute unique timestamps
            let all_ts = one
                .parameters
                .iter()
                .map(|p| p.readings.iter().map(|r| r.timestamp).collect::<Vec<i64>>())
                .reduce(|x, y| x.into_iter().chain(y).collect())
                .context("readings are empty")?
                .into_iter()
                .collect::<BTreeSet<i64>>();

            // compute an empty vector per timestamp
            let mut all_vecs: BTreeMap<i64, Vec<Option<f64>>> = all_ts
                .into_iter()
                .map(|ts| (ts, vec![None; one.parameters.len()]))
                .collect();

            // fill the vectors, leaving None if not set
            for i in 0..one.parameters.len() {
                for r in one.parameters[i].readings.iter() {
                    all_vecs.get_mut(&r.timestamp).unwrap()[i] = Some(r.value);
                }
            }

            for (ts, values) in all_vecs {
                inst.tsb.append_value(ts);

                for i in 0..values.len() {
                    let it = values[i];
                    inst.fbs[i].append_option(it);
                }
            }

            std::thread::sleep(time::Duration::from_millis(100));
        }

        for (_, mut inst) in insts {
            let mut builders: Vec<ArrayRef> = Vec::new();
            builders.push(Arc::new(inst.tsb.finish()));
            for mut fb in inst.fbs {
                builders.push(Arc::new(fb.finish()));
            }

            let batch = RecordBatch::try_new(Arc::new(inst.schema), builders).unwrap();

            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(
                    ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
                ))
                .build();

            let mut writer = ArrowWriter::try_new(inst.file, batch.schema(), Some(props))
                .with_context(|| "new arrow writer failed")?;

            writer
                .write(&batch)
                .with_context(|| "write parquet data failed")?;
            writer
                .close()
                .with_context(|| "close parquet file failed")?;
        }
    }
    Ok(())
}
