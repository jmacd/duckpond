use crate::pond::dir;
use crate::hydrovu::model;

use anyhow::{Context,Result,anyhow};
use arrow_schema::SchemaRef;
use duckdb::Connection;

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};

use std::path::Path;
use std::fs::File;

pub fn export_data(dir: &mut dir::Directory) -> Result<()> {
    
    let locs: Vec<model::Location> = dir.read_file("locations")?;

    let conn = Connection::open_in_memory()?;
    
    for loc in &locs {
	let bname = format!("data-{}", loc.id);
	let files = dir.all_paths_of(&bname);

	let query = format!("CREATE TABLE data_{} AS SELECT * FROM read_parquet({:?});", loc.id, files);

	conn.execute_batch(&query)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
            ))
            .build();

	let place =  format!("./loc.{}.parquet", loc.id);
	let newpath = Path::new(&place);

	let file = File::create_new(&newpath)
	    .with_context(|| format!("create new parquet file {:?}", newpath.display()))?;

	let mut stmt = conn.prepare(format!("SELECT * FROM data_{};", loc.id).as_str())?;

	let mut schema: Option<SchemaRef> = None;
	for rb in stmt.query_arrow([])? {
	    schema = Some(rb.schema());
	    break
	}

	if let None = schema {
	    return Err(anyhow!("empty result has no schema"));
	}
	
        let mut writer = ArrowWriter::try_new(file, schema.unwrap(), Some(props))
            .with_context(|| "new arrow writer failed")?;

	for rb in stmt.query_arrow([])? {
            writer
                .write(&rb)
                .with_context(|| "write parquet data failed")?;
	}

        writer
            .close()
            .with_context(|| "close parquet file failed")?;
    }	
	// print_batches(&rbs)?;

	// while let Some(row) = rows.next()? {
        //     eprintln!("row {:?}", row.get(0).with_context(|| "duckdb row get")?);
	// }
	// match conn.close() {
	//     Ok(_) => {},
	//     Err(e) -> 
	// }

    conn.close().map_err(|x| x.1)?;

    Ok(())
    // match conn.close() {
    // 	Ok(_) => Ok(()),
    // 	Err(e) => Err(e.map_err(x) x.1)
    // }
}
