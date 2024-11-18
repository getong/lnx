use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{info, instrument};

use crate::bucket::{BucketCreateOptions, SharedBucket};
use crate::io::{RuntimeDispatcher, RuntimeOptions};
use crate::metastore::MetastoreError;
use crate::Bucket;

/// A [VirtualFileSystem] provides an IO abstraction layer in an LSM-like fashion
/// for efficiently writing blobs to disk.
///
/// Internally blobs are read and written using direct IO and a separate async runtime,
/// it greatly improves the performance of writing small blobs and managing files on disk.
///
/// The system is akin to that of an object storage service like S3.
///
/// ## Blob Caching
///
/// It is important to note that this system does _no caching of blobs_, this includes
/// the file system cache which is bypassed because we use direct IO.
///
/// It is up to the application using this system to cache blobs how it sees fit (for now.)
///
/// ## Buckets
///
/// Like S3, the system has the concept of buckets. This a completely separate
/// folder in the file system with a separate metastore. It is not recommended to have lots
/// of small buckets as it reduces the efficiency of the system, but a couple large
/// buckets may improve performance if there is a _lot_ of activity and SQLite starts
/// to become a limiting factor.
///
pub struct VirtualFileSystem {
    mount_point: Arc<PathBuf>,
    buckets: Arc<RwLock<ahash::HashMap<String, SharedBucket>>>,
    runtime: RuntimeDispatcher,
}

impl VirtualFileSystem {
    #[instrument]
    /// Mount the file system on the given directory.
    ///
    /// Internally it will create the buckets within this directory
    /// and write all its data.
    ///
    /// If any buckets exist when mounting, they will be loaded
    /// and accessible.
    pub async fn mount(
        path: PathBuf,
        runtime_options: RuntimeOptions,
    ) -> Result<Self, FileSystemError> {
        let runtime = crate::io::create_io_runtime(runtime_options)?;

        let mut buckets = ahash::HashMap::default();
        for entry in path.read_dir()? {
            let entry = entry?;

            info!(path = %entry.path().display(), "Attempting to open bucket");
            let bucket = Bucket::open(entry.path(), runtime.clone()).await?;
            buckets.insert(bucket.name().to_string(), SharedBucket::new(bucket));
        }

        Ok(Self {
            mount_point: Arc::new(path),
            buckets: Arc::new(RwLock::new(buckets)),
            runtime,
        })
    }

    /// Returns a clone of the [SharedBucket] if it exists.
    pub fn bucket(&self, bucket: &str) -> Option<SharedBucket> {
        self.buckets.read().get(bucket).cloned()
    }

    /// Creates a new bucket with the given [BucketCreateOptions].
    ///
    /// Returns an error if the bucket already exists (on disk.)
    pub async fn create_bucket(&self, name: &str) -> Result<(), FileSystemError> {
        let options = BucketCreateOptions::builder()
            .name(name)
            .bucket_path(self.mount_point.join(name))
            .build();

        let bucket = Bucket::create(options, self.runtime.clone()).await?;

        let mut lock = self.buckets.write();
        lock.insert(bucket.name().to_string(), SharedBucket::new(bucket));

        Ok(())
    }

    /// Attempts to delete the bucket with the given name.
    ///
    /// WARNING:
    ///
    /// This does _NO_ checking if the bucket is no longer in use or not
    /// and great care should be taken using this method.
    pub async fn delete_bucket(&self, bucket: &str) -> io::Result<()> {
        let maybe_bucket = {
            let mut lock = self.buckets.write();
            lock.remove(bucket)
        };

        if let Some(bucket) = maybe_bucket {
            tokio::fs::remove_dir_all(bucket.path()).await?;
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
/// An error that can occur from the file system service.
pub enum FileSystemError {
    #[error("IO Error: {0}")]
    /// An IO error that occurred while attempting to complete
    /// the operation.
    IoError(#[from] io::Error),
    #[error("Metastore Error: {0}")]
    /// An error that occurred when attempting to access the metastore.
    MetastoreError(#[from] MetastoreError),
    #[error("Bucket not found: {0:?}")]
    /// The target bucket does not exist.
    BucketNotFound(String),
    #[error("File not found: {0:?}")]
    /// No file exists within the bucket with the given name.
    FileNotFound(String),
    #[error("Bucket already exists: {0:?}")]
    /// A bucket with the provided name already exists.
    BucketAlreadyExists(String),
    #[error("Bucket Corrupted: {0}")]
    /// Some part of the bucket data is corrupted.
    Corrupted(String),
}
