use crate::pond::dir;
use crate::hydrovu::model;

use anyhow::{Context,Result};

use duckdb::Connection;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::arrow::util::pretty::print_batches;

pub fn export_data(dir: &mut dir::Directory) -> Result<()> {
    
    let locs: Vec<model::Location> = dir.read_file("locations")?;
    
    for loc in &locs {
	let bname = format!("data-{}", loc.id);
	let files = dir.all_paths_of(&bname);

	let conn = Connection::open_in_memory()?;

	let query = format!("CREATE TABLE data AS SELECT * FROM read_parquet({:?});", files);

	conn.execute_batch(&query)?;

	let mut stmt = conn.prepare("SELECT * FROM data;")?;
	let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();
	print_batches(&rbs)?;

	// while let Some(row) = rows.next()? {
        //     eprintln!("row {:?}", row.get(0).with_context(|| "duckdb row get")?);
	// }
    }
    
    Ok(())
}
