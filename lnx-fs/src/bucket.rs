//! Virtual File System Bucket
//!
//! This is a way of organising a set of files into completely isolated partitions, similar
//! to that of S3.
//!
//! Buckets are not designed to be created in the hundreds or thousands, or even tens,
//! they should be used in situations where you _must_ have the file system isolation
//! or when the system cannot keep up with a single SQLite metastore.
//!
//! ### File System Structure
//!
//! Buckets are laid out on disk in a consistent structure:
//!
//! ```text
//! base_path/
//! ├── metastore.sqlite
//! └── tablets/
//!     └── 01JCXNCND5Q2ANW5JD8F08DN3V.tablet
//!     └── 01JCXNCND4PG1S3317HA4JC2B6.tablet
//!     └── 01JCXNCNDRT1YGN3X459XTQSCA.tablet
//! ```
//!
//! #### `metastore.sqlite`
//!
//! This contains the metadata of live and active files contained within the
//! `tablets`, along with metadata about the bucket itself, e.g. name, config, etc...
//!
//! #### `tablets/`
//!
//! This is the main data directory where all tablet files are written to.
//!
//! It is possible that some tablets exist within the directory while not being in used
//! anymore, this is because the system only periodically performs a compaction and GC
//! of the dead files.
//!
use std::io;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bon::Builder;
use moka::policy::EvictionPolicy;
use tracing::{info, instrument, trace};

use crate::io::{
    Body,
    RuntimeDispatcher,
    TabletReader,
    TabletReaderOptions,
    TabletWriter,
    TabletWriterOptions,
};
use crate::metastore::Metastore;
use crate::service::FileSystemError;
use crate::{FileMetadata, FileUrl, TabletId};

static TABLET_PATH: &str = "tablets";
static METASTORE_FILE: &str = "metastore.sqlite";

macro_rules! get_config {
    ($metastore:expr, $key:expr) => {{
        $metastore.get_config_value($key).await?.ok_or_else(|| {
            FileSystemError::Corrupted(format!(
                "Bucket required config key {:?} does not exist",
                $key
            ))
        })
    }};
    ($metastore:expr, $key:expr, ty = $t:ty) => {{
        $metastore.get_config_value::<$t>($key).await
    }};
}

macro_rules! set_config {
    ($metastore:expr, $key:expr, $value:expr) => {{
        $metastore.set_config_value($key, $value).await
    }};
}

#[derive(Debug, Builder)]
pub struct BucketOptions {
    #[builder(into)]
    /// The name of the bucket.
    name: String,
    /// The base path for the bucket on disk.
    bucket_path: PathBuf,
    #[builder(default = 512)]
    /// The maximum number of readers allowed to be open at once.
    ///
    /// NOTE: This is a _soft_ limit as some readers may still be in use
    /// after being evicted from the cache.
    max_open_readers: usize,
    #[builder(default = Duration::from_secs(60 * 60))] // 1 hour
    /// The time it takes for a reader to be marked as IDLE in the cache after no uses.
    readers_time_to_idle: Duration,
}

impl BucketOptions {
    async fn save_in_metastore(
        &self,
        metastore: &Metastore,
    ) -> Result<(), FileSystemError> {
        set_config!(metastore, "name", &self.name)?;
        set_config!(metastore, "max_open_readers", &self.max_open_readers)?;
        set_config!(
            metastore,
            "open_readers_time_to_idle_secs",
            &self.readers_time_to_idle.as_secs()
        )?;
        Ok(())
    }

    async fn load_from_metastore(
        base_path: PathBuf,
        metastore: &Metastore,
    ) -> Result<Self, FileSystemError> {
        let name: String = get_config!(metastore, "name")?;
        let max_open_readers: usize = get_config!(metastore, "max_open_readers")?;
        let time_to_idle: u64 =
            get_config!(metastore, "open_readers_time_to_idle_secs")?;

        Ok(Self {
            bucket_path: base_path,
            name,
            max_open_readers,
            readers_time_to_idle: Duration::from_secs(time_to_idle),
        })
    }
}

pub struct Bucket {
    /// The configuration options of the bucket.
    options: BucketOptions,
    /// The paths within the bucket containing various parts of the bucket data.
    paths: BucketPaths,
    /// The metastore for the bucket.
    metastore: Metastore,
    /// The tablet writer for completing new write requests.
    writer: TabletWriter,
    /// An LFU cache of open tablet readers.
    ///
    /// The size of the cache can be configured to hold a variable number
    /// of open files to help minimize file descriptor errors.
    readers: moka::sync::Cache<TabletId, TabletReader, ahash::RandomState>,
    /// The IO runtime for the bucket.
    runtime: RuntimeDispatcher,
}

impl Bucket {
    /// Creates a new bucket with the given runtime.
    ///
    /// If a bucket already exist at the target path a [FileSystemError::BucketAlreadyExists]
    /// is returned.
    pub(crate) async fn create(
        options: BucketOptions,
        runtime: RuntimeDispatcher,
    ) -> Result<Self, FileSystemError> {
        let paths = BucketPaths::from_base(options.bucket_path.clone());

        if paths.metastore_exists()? {
            return Err(FileSystemError::BucketAlreadyExists(options.name));
        }

        paths.ensure_bucket_path_exists()?;
        paths.ensure_tablets_path_exists()?;
        paths.ensure_metastore_file_exists()?;

        let metastore = Metastore::connect(&paths.metastore_sqlite_path()).await?;
        options.save_in_metastore(&metastore).await?;

        Self::open_bucket_inner(options, paths, metastore, runtime).await
    }

    /// Opens an existing bucket with the given runtime.
    ///
    /// If no bucket exists in the given folder a [FileSystemError::BucketNotFound]
    /// error is returned.
    pub(crate) async fn open(
        base_path: PathBuf,
        runtime: RuntimeDispatcher,
    ) -> Result<Self, FileSystemError> {
        let paths = BucketPaths::from_base(base_path.clone());

        if !paths.metastore_exists()? {
            return Err(FileSystemError::BucketNotFound(paths.guess_bucket_name()));
        }

        let metastore = Metastore::connect(&paths.metastore_sqlite_path()).await?;
        let options = BucketOptions::load_from_metastore(base_path, &metastore).await?;

        Self::open_bucket_inner(options, paths, metastore, runtime).await
    }

    async fn open_bucket_inner(
        options: BucketOptions,
        paths: BucketPaths,
        metastore: Metastore,
        runtime: RuntimeDispatcher,
    ) -> Result<Self, FileSystemError> {
        let tablets = metastore.list_tablets().await?;

        // Ensure all the tablets exist, if some are missing, we have an issue.
        for tablet in tablets {
            if !paths.tablet_exists(tablet) {
                return Err(FileSystemError::Corrupted(format!(
                    "Bucket {:?} is missing tablet file {tablet}",
                    options.name
                )));
            }
        }

        let writer_options =
            load_writer_options(&metastore, paths.tablets_path.clone()).await?;
        let writer = TabletWriter::new(writer_options, runtime.clone());

        let readers = moka::sync::CacheBuilder::new(options.max_open_readers as u64)
            .eviction_policy(EvictionPolicy::tiny_lfu())
            .time_to_idle(options.readers_time_to_idle)
            .name(&format!("lnx-fs-readers-{}", options.name))
            .build_with_hasher(ahash::RandomState::new());

        Ok(Self {
            options,
            paths,
            metastore,
            writer,
            readers,
            runtime,
        })
    }

    #[instrument(skip(self, body))]
    /// Write a blob body stream to the store with the given path.
    ///
    /// Once this call completes, the blob is safely persisted to disk.
    pub async fn writer(&self, path: &str, body: Body) -> Result<(), FileSystemError> {
        trace!("Begin writing blob");

        let response = self.writer.write(body).await?;
        trace!("Blob write complete");

        let url = FileUrl::new(path, response.tablet_id);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let metadata = FileMetadata {
            position: response.position,
            created_at: now as i64,
        };

        self.metastore.add_file(url, metadata).await?;
        trace!("Metadata updated");

        Ok(())
    }

    #[instrument(skip(self))]
    /// Creates creates a new read stream for the given file if it exists.
    ///
    /// NOTE:
    /// This operation can be quite expensive if the file is not in the cache,
    /// this is because it will read the metastore for configuration options
    /// before creating the reader.
    ///
    /// To minimise this impact, it is important to have a suitably sized
    /// reader cache allowance.
    pub async fn reader(&self, path: &str) -> Result<Body, FileSystemError> {
        trace!("Begin reading blob");

        let (url, metadata) = self
            .metastore
            .get_file(path)
            .await?
            .ok_or_else(|| FileSystemError::FileNotFound(path.to_string()))?;
        let tablet_id = url.tablet_id();

        if let Some(reader) = self.readers.get(&tablet_id) {
            return reader
                .read(metadata.position)
                .await
                .map_err(FileSystemError::from);
        }

        // This step can be quite expensive
        let options = load_reader_options(
            &self.metastore,
            tablet_id,
            self.paths.tablets_path.clone(),
        )
        .await?;

        let reader = TabletReader::open(options, self.runtime.clone()).await?;
        self.readers.insert(tablet_id, reader.clone());

        reader
            .read(metadata.position)
            .await
            .map_err(FileSystemError::from)
    }

    #[instrument(skip(self))]
    /// Returns the file metadata associated with the given file.
    pub async fn metadata(&self, path: &str) -> Result<FileMetadata, FileSystemError> {
        trace!("Get metadata");
        let url_and_metadata = self.metastore.get_file(path).await?;
        url_and_metadata
            .map(|pair| pair.1)
            .ok_or_else(|| FileSystemError::FileNotFound(path.to_string()))
    }
}

struct BucketPaths {
    metastore_path: PathBuf,
    tablets_path: PathBuf,
    base_path: PathBuf,
}

impl BucketPaths {
    fn from_base(base_path: PathBuf) -> Self {
        Self {
            metastore_path: base_path.join(METASTORE_FILE),
            tablets_path: base_path.join(TABLET_PATH),
            base_path,
        }
    }

    fn metastore_exists(&self) -> io::Result<bool> {
        self.metastore_path.try_exists()
    }

    fn metastore_sqlite_path(&self) -> String {
        format!("sqlite:{}", self.metastore_path.display())
    }

    fn guess_bucket_name(&self) -> String {
        if let Some(dir) = self.base_path.file_name() {
            dir.to_string_lossy().to_string()
        } else {
            self.base_path.display().to_string()
        }
    }

    fn ensure_tablets_path_exists(&self) -> io::Result<()> {
        if self.tablets_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.tablets_path.display(), "Create tablet path");
        std::fs::create_dir(self.tablets_path.as_path())?;

        Ok(())
    }

    fn ensure_metastore_file_exists(&self) -> io::Result<()> {
        if self.metastore_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.metastore_path.display(), "Create metastore");
        std::fs::File::create(self.metastore_path.as_path())?;

        Ok(())
    }

    fn ensure_bucket_path_exists(&self) -> io::Result<()> {
        if self.base_path.try_exists()? {
            return Ok(());
        }

        info!(path = %self.base_path.display(), "Create bucket path");
        std::fs::create_dir(self.base_path.as_path())?;

        Ok(())
    }

    fn tablet_exists(&self, tablet_id: TabletId) -> bool {
        self.tablets_path.join(tablet_id.to_string()).exists()
    }
}

async fn load_writer_options(
    metastore: &Metastore,
    base_path: PathBuf,
) -> Result<TabletWriterOptions, FileSystemError> {
    let max_tablet_size = get_config!(metastore, "max_tablet_size", ty = u64)?;
    let max_active_writers = get_config!(metastore, "max_active_writers", ty = usize)?;

    let options = TabletWriterOptions::builder()
        .base_path(base_path)
        .maybe_max_tablet_size(max_tablet_size)
        .maybe_max_active_writers(max_active_writers)
        .build();

    Ok(options)
}

async fn load_reader_options(
    metastore: &Metastore,
    tablet_id: TabletId,
    base_path: PathBuf,
) -> Result<TabletReaderOptions, FileSystemError> {
    let max_concurrent_reads =
        get_config!(metastore, "max_concurrent_reads", ty = usize)?;
    let sequential_read_threshold =
        get_config!(metastore, "sequential_read_threshold_bytes", ty = usize)?;

    let options = TabletReaderOptions::builder()
        .base_path(base_path)
        .tablet_id(tablet_id)
        .maybe_max_concurrent_reads(max_concurrent_reads)
        .maybe_sequential_read_threshold(sequential_read_threshold)
        .build();

    Ok(options)
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use super::*;
    use crate::io::RuntimeOptions;

    #[test]
    fn test_paths_resolve_to_correct_layout() {
        let base_path = temp_dir().join("test-metastore");
        let paths = BucketPaths::from_base(base_path.clone());
        assert_eq!(paths.metastore_path, base_path.join(METASTORE_FILE));
        assert_eq!(paths.tablets_path, base_path.join(TABLET_PATH));
        assert_eq!(paths.guess_bucket_name(), "test-metastore");
    }

    #[tokio::test]
    async fn test_create_new_bucket() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name)
            .max_open_readers(1)
            .build();

        let _bucket = Bucket::create(options, dispatch)
            .await
            .expect("Create bucket");
    }

    #[tokio::test]
    async fn test_open_existing_bucket() {
        let rt_options = RuntimeOptions::builder().num_threads(1).build();
        let dispatch = crate::io::create_io_runtime(rt_options).unwrap();

        let bucket_name = ulid::Ulid::new().to_string();

        let options = BucketOptions::builder()
            .bucket_path(temp_dir().join(&bucket_name))
            .name(bucket_name.clone())
            .max_open_readers(1)
            .build();

        let bucket = Bucket::create(options, dispatch.clone())
            .await
            .expect("Create bucket");
        drop(bucket);

        let expected_path = temp_dir().join(bucket_name);
        Bucket::open(expected_path, dispatch)
            .await
            .expect("Open existing bucket");
    }
}
