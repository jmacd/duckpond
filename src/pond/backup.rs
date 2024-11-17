use crate::pond;
use crate::pond::copy::split_path;
use crate::pond::crd::BackupSpec;
use crate::pond::crd::S3Fields;
use crate::pond::dir::read_entries;
use crate::pond::dir::FileType;
use crate::pond::file::sha256_file;
use crate::pond::wd::WD;
use crate::pond::writer::MultiWriter;
use crate::pond::ForArrow;
use crate::pond::ForPond;
use crate::pond::InitContinuation;
use crate::pond::Pond;
use crate::pond::UniqueSpec;
use crate::pond::tmpfile;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::serde_types::Object;
use zstd;
use hex;
use sha2::Digest;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use arrow::datatypes::{DataType, Field, FieldRef};
use anyhow::{anyhow, Context, Result};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::{
    arrow::ArrowWriter, basic::Compression, basic::ZstdLevel, file::properties::WriterProperties,
};
use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum Commands {
    List {
        #[arg(short, long)]
        uuid: String,
    },
    Delete {
        #[arg(short, long)]
        uuid: String,

        #[arg(long)]
        danger: bool,
    },
}

pub struct Common {
    pub bucket: Bucket,
    uuidstr: String,
}

struct Backup {
    common: Common,
    writer_id: usize,
    path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct State {
    pub last: u64,
}

impl ForArrow for State {
    fn for_arrow() -> Vec<FieldRef> {
        vec![Arc::new(Field::new("last", DataType::UInt64, false))]
    }
}

impl Backup {
    fn open_and_put<P: AsRef<Path>>(&mut self, path: P, newpath: &str) -> Result<()> {
        let file = File::open(&path)?;

        let tmp = tmpfile("zstd");
        let zfile = File::create(&tmp)?;

        zstd::stream::copy_encode(&file, &zfile, 6)?;

        let mut zfile = File::open(&tmp)?;

        let status_code = self
            .common
            .bucket
            .put_object_stream(&mut zfile, newpath)
            .with_context(|| "could not put object")?;

        if status_code != 200 {
            Err(anyhow!(
                "put {} status_code {}",
                path.as_ref().display(),
                status_code
            ))
        } else {
            Ok(())
        }
    }

    fn for_each_object<F>(&mut self, f: F) -> Result<()>
    where
        F: Fn(&mut Self, &Object) -> Result<()>,
    {
        let results = self.common.bucket.list(self.common.brootpath(), None)?;

        for res in results {
            for x in res.contents {
                f(self, &x)?;
            }
        }
        Ok(())
    }

    fn write_entries_and_assets(&mut self, pond: &mut Pond, state: &State) -> Result<()> {
        let state = state.clone();

        let path = tmpfile("parquet");

        let writer = pond
            .writer
            .writer_mut(self.writer_id)
            .ok_or(anyhow!("invalid writer"))?;

        writer.commit_to_local_file(&path)?;

        self.open_and_put(
            &path,
            format!("{}{}", self.common.brootpath(), state.last).as_str(),
        )?;

        // We could keep a reference to the arrow record batch
        // used above.  It has 4 columns we need -- name,
        // size, sha256 and content.
        let reread = read_entries(&path)?;

        for ent in &reread {
            if ent.content.is_some() {
                continue;
            }

            let pb = PathBuf::from(&ent.prefix);
            let (dp, bn) = split_path(pb)?;
            pond.in_path(dp, |wd| {
                let mut cent = ent.clone();
                cent.prefix = bn;

                let bpath = self.common.asset_path(&ent.sha256);

                eprintln!("backup {:?} to {}", ent.ftype, bpath);

                let real_path = wd.realpath(&cent).expect("real path here");

                self.open_and_put(&real_path, &bpath)
            })?;
        }

        let statevec = vec![state];

        pond.in_path(&self.path, |wd| -> Result<()> {
            wd.write_whole_file("state", FileType::Table, &statevec)
        })?;

        Ok(())
    }
}

impl Common {
    pub fn new(bucket: Bucket, uuidstr: String) -> Self {
        Self {
            bucket: bucket,
            uuidstr: uuidstr,
        }
    }

    pub fn brootpath(&self) -> String {
        "".to_string() + &self.uuidstr + "/"
    }

    pub fn bpondpath(&self) -> String {
        "".to_string() + &self.uuidstr + "/POND"
    }

    pub fn asset_path(&self, sha: &[u8; 32]) -> String {
        format!("{}asset/{}", self.brootpath(), hex::encode(sha))
    }

    fn read_objects<T: for<'a> Deserialize<'a>>(&mut self, name: &str) -> Result<Vec<T>> {
        let resp_data = self.bucket.get_object(name)?;

        if resp_data.status_code() != 200 {
            return Err(anyhow!(
                "read {}: status code == {}",
                name,
                resp_data.status_code()
            ));
        }

        let cursor = resp_data.bytes().clone();

        let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)
            .with_context(|| format!("open {} failed", name))?;

        let mut reader = builder
            .build()
            .with_context(|| "initialize reader failed")?;
        let input = reader.next();

        match input {
            None => Err(anyhow!("no records")),
            Some(value) => {
                let records = value?;
                Ok(serde_arrow::from_record_batch(&records)?)
            }
        }
    }

    pub fn read_object<T: for<'a> Deserialize<'a>>(&mut self, name: &str) -> Result<T> {
        let mut recs = self.read_objects::<T>(name)?;

        if recs.len() == 1 {
            Ok(recs.remove(0))
        } else {
            Err(anyhow!("expected one record have {}", recs.len()))
        }
    }

    fn write_object<T: Serialize + ForArrow>(&mut self, name: &str, record: &T) -> Result<()> {
        let records = vec![record];
        let batch = serde_arrow::to_record_batch(T::for_arrow().as_slice(), &records)
            .with_context(|| "serialize arrow data failed")?;

        let mut data: Vec<u8> = Vec::new();

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(6).with_context(|| "invalid zstd level 6")?,
            ))
            .build();

        let mut writer = ArrowWriter::try_new(&mut data, batch.schema(), Some(props))
            .with_context(|| "new arrow writer failed")?;

        writer
            .write(&batch)
            .with_context(|| "write parquet data failed")?;
        writer
            .close()
            .with_context(|| "close parquet file failed")?;

        let resp_data = self.bucket.put_object(name, data.as_slice())?;

        if resp_data.status_code() != 200 {
            return Err(anyhow!("status code == {}", resp_data.status_code()));
        }

        Ok(())
    }
}

pub fn new_bucket(s3: &S3Fields) -> Result<Bucket> {
    let region = Region::Custom {
        region: s3.region.clone(),
        endpoint: s3.endpoint.clone(),
    };
    let creds = Credentials::new(
        Some(s3.key.as_str()),
        Some(s3.secret.as_str()),
        None,
        None,
        None,
    )?;

    Ok(Bucket::new(s3.bucket.as_str(), region, creds)?)
}

fn new_backup(uspec: &UniqueSpec<BackupSpec>, writer_id: usize) -> Result<Backup> {
    Ok(Backup {
        common: Common::new(new_bucket(&uspec.spec.s3)?, uspec.uuid.to_string()),
        writer_id: writer_id,
        path: uspec.dirpath(),
    })
}

pub fn init_func(wd: &mut WD, uspec: &UniqueSpec<BackupSpec>, _former: Option<UniqueSpec<BackupSpec>>) -> Result<Option<InitContinuation>> {
    let mut backup = new_backup(
        &uspec,
        wd.multiwriter().add_writer("backup writer".to_string()),
    )?;

    let state = State { last: 1 };

    match backup
        .common
        .read_object::<State>(&backup.common.bpondpath())
    {
        Err(_) => {}
        Ok(_) => return Err(anyhow!("pond backup already exists")),
    }

    Ok(Some(Box::new(|pond| {
        let state = state;
        let mut backup = backup;

        pond.in_path("", |wd| {
            // this will copy an empty state directory belonging to this resource.
            copy_pond(wd, backup.writer_id)
        })?;

        backup.write_entries_and_assets(pond, &state)?;

        backup
            .common
            .write_object(&backup.common.bpondpath(), &state)
    })))
}

fn copy_pond(wd: &mut WD, writer_id: usize) -> Result<()> {
    let ents = wd.entries().clone();
    for ent in &ents {
        if let FileType::Tree = ent.ftype {
            wd.in_path(&ent.prefix, |d| copy_pond(d, writer_id))?;
            continue;
        }

        let mut went = ent.clone();

        went.prefix = wd.pondpath(&ent.prefix).to_string_lossy().to_string();

        // Re-read the file to verify the checksum.
        let real_path = wd
            .realpath_version(&ent.prefix, went.number, went.ftype.ext())
            .expect("real path here");
        let (hasher, size, content_opt) = sha256_file(&real_path)?;

        if size != went.size {
            return Err(anyhow!(
                "local file '{}' has incorrect size {} expected {}",
                real_path.display(),
                size,
                went.size
            ));
        }

        let sha256: [u8; 32] = hasher.finalize().into();
        if sha256 != went.sha256 {
            return Err(anyhow!(
                "local file '{}' has incorrect sha256 {} expected {}",
                &went.prefix,
                hex::encode(sha256),
                hex::encode(went.sha256)
            ));
        }

        went.content = content_opt;

        let writer = wd
            .multiwriter()
            .writer_mut(writer_id)
            .ok_or(anyhow!("missing writer"))?;
        writer.record(&went)?;
    }
    Ok(())
}

pub fn run(_pond: &mut Pond, _spec: &UniqueSpec<BackupSpec>) -> Result<()> {
    Ok(())
}

pub fn start(
    pond: &mut Pond,
    uspec: &UniqueSpec<BackupSpec>,
) -> Result<
    Box<
        dyn for<'a> FnOnce(&'a mut Pond) -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>>,
    >,
> {
    let uspec = uspec.clone();
    let mut backup = new_backup(&uspec, pond.writer.add_writer("backup writer".to_string()))?;

    let s3_state = backup
        .common
        .read_object::<State>(&backup.common.bpondpath())?;

    let dp = uspec.dirpath();
    let local_state = pond.in_path(&dp, |wd| wd.read_file::<State>("state"))?;

    if local_state.len() != 1 {
        return Err(anyhow!("too many entries in local backup state"));
    }

    if *local_state.get(0).unwrap() != s3_state {
        return Err(anyhow!(
            "local and remote states are not equal, repair needed"
        ));
    }

    Ok(Box::new(
        |pond: &mut Pond| -> Result<Box<dyn FnOnce(&mut MultiWriter) -> Result<()>>> {
            let mut state = s3_state;

            state.last += 1;

            backup.write_entries_and_assets(pond, &state)?;

            Ok(Box::new(|_writer| -> Result<()> {
                let mut backup = backup;
                let state = state;

                backup
                    .common
                    .write_object(&backup.common.bpondpath(), &state)
            }))
        },
    ))
}

fn sub_main_cmd<F>(pond: &mut Pond, uuidstr: &str, f: F) -> Result<()>
where
    F: Fn(&mut Pond, &mut Backup) -> Result<()>,
{
    let kind = BackupSpec::spec_kind();
    let specs: Vec<UniqueSpec<BackupSpec>> = pond.in_path(&kind, |wd| wd.read_file(kind))?;
    let mut onespec: Vec<_> = specs
        .iter()
        .filter(|x| x.uuid.to_string() == *uuidstr)
        .collect();

    // TODO: use list_page()

    if onespec.len() == 0 {
        return Err(anyhow!("uuid not found {}", uuidstr));
    }
    let spec = onespec.remove(0);

    let mut backup = new_backup(&spec, pond.writer.add_writer("backup sub-main".to_string()))?;

    f(pond, &mut backup)
}

pub fn sub_main(command: &Commands) -> Result<()> {
    let mut pond = pond::open()?;
    match command {
        Commands::List { uuid } => sub_main_cmd(&mut pond, uuid.as_str(), |_pond, backup| {
            backup.for_each_object(|_backup, x| Ok(eprintln!("{:?}: {} bytes", x.key, x.size)))
        }),
        Commands::Delete { uuid, danger } => {
            if !danger {
                Err(anyhow!(
                    "this will delete backup data; set --danger to proceed"
                ))
            } else {
                sub_main_cmd(&mut pond, uuid.as_str(), |_pond, backup| {
                    backup.for_each_object(|backup, x| {
                        let resp = backup.common.bucket.delete_object(&x.key)?;
                        let code = resp.status_code();
                        if code >= 200 && code < 300 {
                            eprintln!("deleted {:?}", &x.key);
                            Ok(())
                        } else {
                            Err(anyhow!("s3 delete: {:?}: {}", &x.key, resp.status_code()))
                        }
                    })
                })
            }
        }
    }
}
