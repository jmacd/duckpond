use crate::hydrovu::model;
use crate::pond::wd;

use wd::WD;

use anyhow::{anyhow, Context, Result};
use arrow_schema::SchemaRef;
use duckdb::Connection;

use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};

use std::fs::File;
use std::path::Path;

pub fn export_data(dir: &WD) -> Result<()> {
    let locs: Vec<model::Location> = dir.read_file("locations")?;

    let conn = Connection::open_in_memory()?;

    for loc in &locs {
        let bname = format!("data-{}", loc.id);
        let files = dir.realpath_all(&bname);

        let query = format!(
            "CREATE TABLE data_{} AS SELECT * FROM read_parquet({:?});",
            loc.id, files
        );

        conn.execute_batch(&query)?;

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
            ))
            .build();

        let place = format!("./loc.{}.parquet", loc.name.replace(" ", "_"));
        let newpath = Path::new(&place);

        let file = File::create_new(&newpath)
            .with_context(|| format!("create new parquet file {:?}", newpath.display()))?;

        let mut stmt = conn.prepare(format!("SELECT * FROM data_{};", loc.id).as_str())?;

        let mut schema: Option<SchemaRef> = None;
        for rb in stmt.query_arrow([])? {
            schema = Some(rb.schema());
            break;
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

    conn.close().map_err(|x| x.1)?;

    Ok(())
}
