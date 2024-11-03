mod client;
mod constant;
mod load;
mod model;

use crate::pond;
use crate::pond::crd::HydroVuDevice;
use crate::pond::crd::HydroVuSpec;
use crate::pond::dir::FileType;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::Pond;
use crate::pond::UniqueSpec;

use anyhow::{anyhow, Context, Result};
use arrow::array::Float64Builder;
use arrow::array::Int64Builder;
use arrow::datatypes::Schema;
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use arrow_array::array::ArrayRef;
use chrono::offset::Utc;
use chrono::DateTime;
use chrono::SecondsFormat;
use client::Client;
use client::ClientCall;
use model::Location;
use model::LocationReadings;
use model::Mapping;
use model::Names;
use model::ScopedLocation;
use model::Temporal;
use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

pub const MIN_POINTS_PER_READ: usize = 1000;

fn creds(spec: &HydroVuSpec) -> (String, String) {
    (spec.key.clone(), spec.secret.clone())
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

fn write_units(d: &mut WD, mapping: BTreeMap<i16, String>) -> Result<()> {
    write_mapping(d, "units", mapping)
}

fn write_parameters(d: &mut WD, mapping: BTreeMap<i16, String>) -> Result<()> {
    write_mapping(d, "params", mapping)
}

fn write_mapping(d: &mut WD, name: &str, mapping: BTreeMap<i16, String>) -> Result<()> {
    let result = mapping
        .into_iter()
        .map(|(x, y)| -> Mapping { Mapping { index: x, value: y } })
        .collect::<Vec<_>>();

    d.write_whole_file(name, FileType::Table, &result)
}

fn write_locations(d: &mut WD, locations: &Vec<ScopedLocation>) -> Result<()> {
    let result = locations.to_vec();

    d.write_whole_file("locations", FileType::Table, &result)
}

fn write_temporal(d: &mut WD, locations: &Vec<ScopedLocation>) -> Result<()> {
    let result = locations
        .iter()
        .map(|x| model::Temporal {
            location_id: x.location.id,
            min_time: 0,
            max_time: 0,
            record_time: 0,
            num_points: 0,
        })
        .collect::<Vec<Temporal>>();

    // @@@ TODO Table->Series
    d.write_whole_file("temporal", FileType::Table, &result)
}

fn ss2is(ss: (String, String)) -> Option<(i16, String)> {
    if let Ok(i) = ss.0.parse::<i16>() {
        Some((i, ss.1))
    } else {
        // HydroVu has some garbage data.
        None
    }
}

pub fn init_func(
    d: &mut WD,
    spec: &UniqueSpec<HydroVuSpec>,
) -> Result<Option<pond::InitContinuation>> {
    let client = Rc::new(Client::new(creds(spec.inner()))?);

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

    // Mutate locations based on device settings
    let tab: BTreeMap<i64, HydroVuDevice> = spec
        .inner()
        .devices
        .iter()
        .map(|x| (x.id, x.clone()))
        .collect();

    let locations = locations
        .iter()
        .filter_map(|x| -> Option<_> {
            let find = tab.get(&x.id)?;
            Some(ScopedLocation {
                location: Location {
                    description: x.description.clone(),
                    id: x.id,
                    name: find.name.clone(), // replace name field
                    gps: x.gps.clone(),
                },
                scope: find.scope.clone(),
            })
        })
        .collect();

    write_units(d, units)?;
    write_parameters(d, params)?;
    write_locations(d, &locations)?;
    write_temporal(d, &locations)?;
    Ok(None)
}

struct Instrument {
    schema: Schema,
    lid: i64,
    tsb: Int64Builder,
    fbs: Vec<Float64Builder>,
}

pub fn read(
    dir: &mut WD,
    vu: &model::Vu,
    spec: &HydroVuSpec,
    temporal: &mut Vec<model::Temporal>,
) -> Result<()> {
    let client = Rc::new(Client::new(creds(spec))?);

    let now = Utc::now().fixed_offset() - (Duration::from_secs(3600));

    for loc in &vu.locations {
        let mut num_points = 0;
        let mut min_time = std::i64::MAX;
        let mut max_time = std::i64::MIN;

        let loc_last = temporal
            .iter()
            .filter(|ref x| x.location_id == loc.location.id)
            .fold(std::i64::MIN, |acc, e| acc.max(e.max_time));

        // Calculate a set of instruments from the parameters at this
        // location.
        let mut insts = BTreeMap::<String, Instrument>::new();

        for one_data in fetch_data(client.clone(), loc.location.id, loc_last + 1, None) {
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
                    // Build a dynamic Arrow schema.
                    let mut fields = vec![Arc::new(Field::new(
                        "Timestamp",
                        //DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
			DataType::Int64,
                        false,
                    ))];

                    // Map the discovered parameter/unit to an Arrow column.
                    let mut fvec = one
                        .parameters
                        .iter()
                        .map(|p| -> Result<Arc<Field>> {
                            let pu = vu.lookup_param_unit(p)?;
                            Ok(Arc::new(Field::new(
                                format!("{}.{}.{}", loc.scope, pu.0, pu.1),
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
                            lid: loc.location.id,
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

	    std::thread::sleep(std::time::Duration::from_millis(10));

            if num_points > MIN_POINTS_PER_READ {
                break;
            }
        }

        if num_points == 0 {
            eprintln!(
                "     ... location {} no new points at {}",
                loc.location.name,
                utc2date(loc_last)?,
            );
            continue;
        }

        if min_time > max_time {
            return Err(anyhow!(
                "{}: min_time > max_time: {} > {}",
                loc.location.name,
                min_time,
                max_time
            ));
        }
        if min_time <= 0 {
            return Err(anyhow!(
                "{} ({}): min_time is zero",
                loc.location.id,
                loc.location.name
            ));
        }

        for (_, mut inst) in insts {
            let mut builders: Vec<ArrayRef> = Vec::new();
            builders.push(Arc::new(inst.tsb.finish()));
            for mut fb in inst.fbs {
                builders.push(Arc::new(fb.finish()));
            }

            let batch = RecordBatch::try_new(Arc::new(inst.schema), builders)?;

            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(
                    ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
                ))
                .build();

            dir.in_path("data", |wd| {
                wd.create_any_file(
                    format!("{}-{}", loc.location.name, inst.lid).as_str(),
                    FileType::Series,
                    |f| {
                        let mut writer = ArrowWriter::try_new(f, batch.schema(), Some(props))
                            .with_context(|| "new arrow writer failed")?;

                        writer
                            .write(&batch)
                            .with_context(|| "write parquet data failed")?;
                        writer
                            .close()
                            .with_context(|| "close parquet file failed")?;

                        Ok(())
                    },
                )
            })?;
        }

        eprintln!(
            "     ... location {} {}..{} wrote {} points",
            loc.location.name,
            utc2date(min_time)?,
            utc2date(max_time)?,
            num_points,
        );

        temporal.push(model::Temporal {
            location_id: loc.location.id,
            min_time: min_time,
            max_time: max_time,
            record_time: now.timestamp(),
            num_points: num_points as i64,
        });
    }
    Ok(())
}

pub fn run(pond: &mut Pond, spec: &UniqueSpec<HydroVuSpec>) -> Result<()> {
    pond.in_path(spec.dirpath(), |wd| {
        let vu = load::load(wd)?;

        let mut temporal = wd.read_file("temporal")?;

        read(wd, &vu, spec.inner(), &mut temporal)?;

        wd.write_whole_file("temporal", FileType::Table, &temporal)
    })
}

pub fn utc2date(utc: i64) -> Result<String> {
    Ok(DateTime::from_timestamp(utc, 0)
        .ok_or_else(|| anyhow!("cannot get date"))?
        .to_rfc3339_opts(SecondsFormat::Secs, true))
}

pub fn start(
    pond: &mut Pond,
    uspec: &UniqueSpec<HydroVuSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    pond::start_noop(pond, uspec)
}
