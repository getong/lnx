use std::io;
use crate::metastore::MetastoreError;

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