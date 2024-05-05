mod client;
mod constant;
pub mod error;
mod load;
mod model;

use std::rc::Rc;

use chrono::offset::Utc;
use chrono::DateTime;
use chrono::SecondsFormat;

use client::Client;
use client::ClientCall;
use error::Error;
use model::Location;
use model::LocationReadings;
use model::Mapping;
use model::Names;

use std::fs::File;
use std::time;

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};

use std::collections::BTreeMap;
use std::collections::BTreeSet;
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
        Arc::new(Field::new("index", DataType::Int16, false)),
        Arc::new(Field::new("value", DataType::Utf8, false)),
    ]
}

fn evar(name: &str) -> Result<String, Error> {
    Ok(env::var(name).with_context(|| format!("{name} is not set"))?)
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

fn write_units(mapping: BTreeMap<i16, String>) -> Result<(), Error> {
    write_mapping("units.parquet", mapping)
}

fn write_parameters(mapping: BTreeMap<i16, String>) -> Result<(), Error> {
    write_mapping("params.parquet", mapping)
}

fn write_mapping(name: &str, mapping: BTreeMap<i16, String>) -> Result<(), Error> {
    let result = mapping
        .into_iter()
        .map(|(x, y)| -> Mapping { Mapping { index: x, value: y } })
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

    let file = File::create(name).with_context(|| format!("open parquet file {}", name))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
        ))
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .with_context(|| "new arrow writer failed")?;

    writer
        .write(&batch)
        .with_context(|| "write parquet data failed")?;
    writer
        .close()
        .with_context(|| "close parquet file failed")?;

    Ok(())
}

fn ss2is(ss: (String, String)) -> Option<(i16, String)> {
    if let Ok(i) = ss.0.parse::<i16>() {
	Some((i, ss.1))
    } else {
	eprintln!("invalid index: {}", ss.0);
	None
    }
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

    write_units(units)?;
    write_parameters(params)?;
    write_locations(locations)?;
    Ok(())
}

fn utc2date(utc: i64) -> String {
    DateTime::from_timestamp(utc, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

struct instrument {
    schema: String,
    file: File,
    fields: Vec<FieldRef>,
}

use arrow::array::Int64Builder;
use arrow::array::Float64Builder;

pub fn read() -> Result<(), Error> {
    let client = Rc::new(Client::new(creds()?)?);
    let now = Utc::now();
    let vu = load::load()?;

    let mut insts = BTree::new::<String, &instrument>();

    for loc in vu.locations {
        for one_data in fetch_data(client.clone(), loc.id, 0, now.timestamp()) {
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

	    let schema =
		one
		.parameters
		.iter()
		.map(|p| {
		    let pu = vu.lookup_param_unit(p);
		    format!("{}-{}", pu.0, pu.1)
		})
		.reduce(|s, t| format!("{}.{}", s, t))
		.context("location has no parameters")?;

	    let mut inst = insts.get(schema);

	    match inst {
		Some(_) => (),
		None => {
		    let fname = format!("data-{}-{}.parquet", loc.location_id, schema);
		    inst = instrument{
			schema: schema,
			file: File::create(fname).with_context(|| format!("open parquet file {}", fname))?,
			fields: vec![
			    Arc::new(Field::new("timestamp", DataType::Int64, false)),
			],
		    };
		}
	    }
	    let inst = inst.unwrap();
	    
	    for p in one.parameters {
		let pu = vu.lookup_param_unit(p);
		inst.fields.push(
		    Arc::new(Field::new(format!("{}.{}", pu.0, pu.1), DataType::Float64, true)),
		);
	    }

	    // one timestamp builder, N float64 builders
	    let mut tsb = Int64Builder::default();
	    let mut vb: Vec<_> = one.parameters.iter().map(|| Float64Builder::default()).collect();

	    // compute unique timestamps
	    let allTs = one.parameters
		.iter()
		.map(|p| p.readings.iter().map(|r| r.timestamp).collect())
		.reduce(|x, y| x.into_iter().chain(y).collect())
		.collect::<BTreeSet<i64>>();

	    // compute an empty vector per timestamp
	    let allVecs: BTreeMap<i64, Vec<Option<f64>>> = allTs.map(|ts| (ts, Vec::new(one.parameters.len)));

	    // fill the vectors, leaving None if not set
	    for i in 0..one.parameters.len {
		for &r in one.parameters[i] {
		    allVecs.get_mut(r.timestamp).unwrap().insert(i, Some(r.value));
		}
	    }

	    for (ts, values) in allVecs {
		tsb.append_value(ts);

		for i in 0..values.len {
		    vb[i].append_option(values[i]);
		}
	    }

	    let mut sa: StructArray::new();

        // let i32 = Arc::new(self.i32.finish()) as ArrayRef;
        // let i32_field = Arc::new(Field::new("i32", DataType::Int32, false));

        // let string = Arc::new(self.string.finish()) as ArrayRef;
        // let string_field = Arc::new(Field::new("i32", DataType::Utf8, false));

        // let i32_list = Arc::new(self.i32_list.finish()) as ArrayRef;
        // let value_field = Arc::new(Field::new("item", DataType::Int32, true));
        // let i32_list_field = Arc::new(Field::new("i32_list", DataType::List(value_field), true));

        // StructArray::from(vec![
        //     (i32_field, i32),
        //     (string_field, string),
        //     (i32_list_field, i32_list),
        // ])
	    
	    RecordBatch::from(&sa)

            std::thread::sleep(time::Duration::from_millis(100));
        }
    }
    let batch = serde_arrow::to_record_batch(readings_fields().as_slice(), &records)
        .with_context(|| "serialize arrow data failed")?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
        ))
        .build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .with_context(|| "new arrow writer failed")?;

    writer
        .write(&batch)
        .with_context(|| "write parquet data failed")?;
    writer
        .close()
        .with_context(|| "close parquet file failed")?;

    Ok(())
}

// /// A custom row representation
// struct MyRow {
//     i32: i32,
//     optional_i32: Option<i32>,
//     string: Option<String>,
//     i32_list: Option<Vec<Option<i32>>>,
// }

// /// Converts `Vec<Row>` into `StructArray`
// #[derive(Debug, Default)]
// struct MyRowBuilder {
//     i32: Int32Builder,
//     string: StringBuilder,
//     i32_list: ListBuilder<Int32Builder>,
// }

// impl MyRowBuilder {
//     fn append(&mut self, row: &MyRow) {
//         self.i32.append_value(row.i32);
//         self.string.append_option(row.string.as_ref());
//         self.i32_list.append_option(row.i32_list.as_ref().map(|x| x.iter().copied()));
//     }

//     /// Note: returns StructArray to allow nesting within another array if desired
//     fn finish(&mut self) -> StructArray {
//         let i32 = Arc::new(self.i32.finish()) as ArrayRef;
//         let i32_field = Arc::new(Field::new("i32", DataType::Int32, false));

//         let string = Arc::new(self.string.finish()) as ArrayRef;
//         let string_field = Arc::new(Field::new("i32", DataType::Utf8, false));

//         let i32_list = Arc::new(self.i32_list.finish()) as ArrayRef;
//         let value_field = Arc::new(Field::new("item", DataType::Int32, true));
//         let i32_list_field = Arc::new(Field::new("i32_list", DataType::List(value_field), true));

//         StructArray::from(vec![
//             (i32_field, i32),
//             (string_field, string),
//             (i32_list_field, i32_list),
//         ])
//     }
// }

// impl<'a> Extend<&'a MyRow> for MyRowBuilder {
//     fn extend<T: IntoIterator<Item = &'a MyRow>>(&mut self, iter: T) {
//         iter.into_iter().for_each(|row| self.append(row));
//     }
// }

// /// Converts a slice of [`MyRow`] to a [`RecordBatch`]
// fn rows_to_batch(rows: &[MyRow]) -> RecordBatch {
//     let mut builder = MyRowBuilder::default();
//     builder.extend(rows);
//     RecordBatch::from(&builder.finish())
// }
