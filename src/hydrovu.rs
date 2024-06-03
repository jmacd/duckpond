mod client;
mod constant;
mod load;
mod model;

use std::rc::Rc;

use crate::pond;
use crate::pond::dir;

use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_array::array::ArrayRef;
use client::Client;
use client::ClientCall;
use model::Location;
use model::LocationReadings;
use model::Mapping;
use model::Names;
use model::Temporal;
use chrono::offset::Utc;
use chrono::DateTime;
use chrono::SecondsFormat;

use std::path::Path;
use std::time;

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::env;

use std::sync::Arc;

use anyhow::{Context, Result};

use arrow::datatypes::{DataType, Field, FieldRef, Fields};

use std::time::Duration;

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

fn temporal_fields() -> Vec<FieldRef> {
    vec![
        Arc::new(Field::new("location_id", DataType::Int64, false)),
        Arc::new(Field::new("min_time", DataType::Int64, false)),
        Arc::new(Field::new("max_time", DataType::Int64, false)),
        Arc::new(Field::new("record_time", DataType::Int64, false)),
        Arc::new(Field::new("num_points", DataType::Int64, false)),
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

fn fetch_data(
    client: Rc<Client>,
    id: i64,
    start: i64,
    end: Option<i64>,
) -> ClientCall<LocationReadings> {
    Client::fetch_json(client, constant::location_url(id, start, end))
}

fn write_units(d: &mut dir::Directory, mapping: BTreeMap<i16, String>) -> Result<()> {
    write_mapping(d, "units", mapping)
}

fn write_parameters(d: &mut dir::Directory, mapping: BTreeMap<i16, String>) -> Result<()> {
    write_mapping(d, "params", mapping)
}

fn write_mapping(d: &mut dir::Directory, name: &str, mapping: BTreeMap<i16, String>) -> Result<()> {
    let result = mapping
        .into_iter()
        .map(|(x, y)| -> Mapping { Mapping { index: x, value: y } })
        .collect::<Vec<_>>();

    d.in_path(Path::new(""), |d: &mut dir::Directory| {
        d.write_file(name, &result, mapping_fields().as_slice())
    })
}

fn write_locations(d: &mut dir::Directory, locations: &Vec<Location>) -> Result<()> {
    let result = locations.to_vec();

    d.in_path(Path::new(""), |d: &mut dir::Directory| {
        d.write_file("locations", &result, location_fields().as_slice())
    })
}

fn write_temporal(d: &mut dir::Directory, locations: &Vec<Location>) -> Result<()> {
    let result = locations
        .iter()
        .map(|x| model::Temporal {
            location_id: x.id,
            min_time: 0,
            max_time: 0,
            record_time: 0,
            num_points: 0,
        })
        .collect::<Vec<Temporal>>();

    d.in_path(Path::new(""), |d: &mut dir::Directory| {
        d.write_file("temporal", &result, temporal_fields().as_slice())
    })
}

fn ss2is(ss: (String, String)) -> Option<(i16, String)> {
    if let Ok(i) = ss.0.parse::<i16>() {
        Some((i, ss.1))
    } else {
        // HydroVu has some garbage data.
        // eprintln!("invalid index: {}", ss.0);
        None
    }
}

pub fn init_func(d: &mut dir::Directory) -> Result<()> {
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

    write_units(d, units)?;
    write_parameters(d, params)?;
    write_locations(d, &locations)?;
    write_temporal(d, &locations)?;
    Ok(())
}

struct Instrument {
    schema: Schema,
    fname: String,
    tsb: Int64Builder,
    fbs: Vec<Float64Builder>,
}

pub fn read(
    dir: &mut dir::Directory,
    vu: &model::Vu,
    temporal: &mut Vec<model::Temporal>,
) -> Result<()> {
    let client = Rc::new(Client::new(creds()?)?);
    
    let now = Utc::now().fixed_offset() - (Duration::from_secs(3600));

    for loc in &vu.locations {
	let mut num_points = 0;
	let mut min_time = std::i64::MAX;
	let mut max_time = std::i64::MIN;

	let loc_last = temporal.iter().filter(|ref x| x.location_id == loc.id).fold(std::i64::MIN, |acc, e| acc.max(e.max_time));
    
	// Calculate a set of instruments from the parameters at this
	// location.
        let mut insts = BTreeMap::<String, Instrument>::new();

	// Iterate over all time for this location.
        for one_data in fetch_data(client.clone(), loc.id, loc_last+1, None) {
            let one = one_data?;

            num_points += one
                .parameters
                .iter()
                .fold(0, |acc, e| acc + e.readings.len());

            let min_one = one
                .parameters
                .iter()
                .map(|x| &x.readings)
                .flatten()
                .fold(std::i64::MAX, |a, b| a.min(b.timestamp));

            let max_one = one
                .parameters
                .iter()
                .map(|x| &x.readings)
                .flatten()
                .fold(std::i64::MIN, |a, b| a.max(b.timestamp));

	    min_time = min_time.min(min_one);
	    max_time = max_time.max(max_one);

	    // Form parts of the instrument schema: map parameter into name and unit.
            let schema_parts: Vec<String> = one
                .parameters
                .iter()
                .map(|p| -> Result<String> {
                    let pu = vu.lookup_param_unit(p)?;
                    Ok(format!("{},{}", pu.0, pu.1))
                })
                .collect::<Result<Vec<_>, _>>()?;
	    // Instrument schema is the concatenation of parts.
            let schema_str: String = schema_parts
                .iter()
                .fold(String::new(), |s, t| format!("{}:{}", s, t));

	    // Get and save the instrument by schema, create Arrow
	    // schema if necessary.
            let inst = insts.get(&schema_str);

            match inst {
                Some(_) => (),
                None => {
		    // Give the instrument a file name.
                    let fname = format!("data-{}", loc.name.replace(" ", "_"));

		    // Build a dynamic Arrow schema.
                    let mut fields =
                        vec![Arc::new(Field::new("timestamp", DataType::Int64, false))];

		    // Map the discovered parameter/unit to an Arrow column.
                    // @@@ how to avoid the mut below?
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

                    // Form a vector of builders, one timestamp and N float64s.
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
                            fname: fname,
                            tsb: tsb,
                            fbs: fbs,
                        },
                    );
                }
            }
            let inst = insts.get_mut(&schema_str).unwrap();

            // Compute unique timestamps.
            let all_ts = one
                .parameters
                .iter()
                .map(|p| p.readings.iter().map(|r| r.timestamp).collect::<Vec<i64>>())
                .reduce(|x, y| x.into_iter().chain(y).collect())
                .context("readings are empty")?
                .into_iter()
                .collect::<BTreeSet<i64>>();

            // Compute an empty vector per timestamp.
            let mut all_vecs: BTreeMap<i64, Vec<Option<f64>>> = all_ts
                .into_iter()
                .map(|ts| (ts, vec![None; one.parameters.len()]))
                .collect();

            // Fill the vectors, leaving None if not set.
            for i in 0..one.parameters.len() {
                for r in one.parameters[i].readings.iter() {
                    all_vecs.get_mut(&r.timestamp).unwrap()[i] = Some(r.value);
                }
            }

	    // Add to the builders
            for (ts, values) in all_vecs {
                inst.tsb.append_value(ts);

                for i in 0..values.len() {
                    let it = values[i];
                    inst.fbs[i].append_option(it);
                }
            }

	    if num_points > 500 {
		break;
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

            dir.create_file(&inst.fname, |f| {
                let mut writer = ArrowWriter::try_new(f, batch.schema(), Some(props))
                    .with_context(|| "new arrow writer failed")?;

                writer
                    .write(&batch)
                    .with_context(|| "write parquet data failed")?;
                writer
                    .close()
                    .with_context(|| "close parquet file failed")?;

                Ok(())
            })?;
        }

        eprintln!(
            "location {} [{}] {}..{} = {} points",
            loc.name,
            loc.id,
            utc2date(min_time),
            utc2date(max_time),
	    num_points,
        );
	
        temporal.push(model::Temporal {
            location_id: loc.id,
            min_time: min_time,
            max_time: max_time,
            record_time: now.timestamp(),
            num_points: num_points as i64,
        });
    }
    Ok(())
}

pub fn run<P: AsRef<Path>>(pond: &mut pond::Pond, path: P) -> Result<()> {
    pond.root
        .in_path(path, |d: &mut dir::Directory| -> Result<()> {
            let vu = load::load(d)?;

            let mut temporal = d.read_file("temporal")?;

            read(d, &vu, &mut temporal)?;

            d.write_file("temporal", &temporal, temporal_fields().as_slice())
        })
}

pub fn utc2date(utc: i64) -> String {
    DateTime::from_timestamp(utc, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}
