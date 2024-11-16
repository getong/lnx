//! The metastore manages file metadata, offsets and position within
//! the virtual file system. It behaves a bit like S3.
//!
//! Internally it is backed by an SQLite database for each bucket.

mod db;

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
/// An error that can occur when the metastore attempts
/// to track a change in the file system.
pub enum MetastoreError {
    #[error("metastore shutdown")]
    /// The metastore system has shutdown or aborted.
    Shutdown,
    #[error("metastore row corrupted")]
    /// The metastore row was corrupted.
    ///
    /// This should never occur unless manual tampering of the metastore
    /// was performed.
    Corrupted,
    #[error("SQLx Error: {0}")]
    SQLxError(#[from] sqlx::Error),
}

#[derive(Clone)]
/// A metastore instance for a given bucket.
pub struct Metastore {
    bucket: Arc<str>,
}

impl Debug for Metastore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Metastore(bucket={})", self.bucket)
    }
}

impl Metastore {
    #[inline]
    /// Returns the bucket associated with the metastore.
    pub fn bucket(&self) -> &str {
        self.bucket.as_ref()
    }

    /// Attempt to get a file with the given path.
    ///
    /// Returns the full [FileUrl] and [FileMetadata].
    pub(crate) async fn get_file(
        &self,
        path: &str,
    ) -> Result<(FileUrl, FileMetadata), MetastoreError> {
        todo!()
    }

    /// Add a file to be tracked in the metastore.
    pub(crate) async fn add_file(
        &self,
        url: FileUrl,
        metadata: FileMetadata,
    ) -> Result<(), MetastoreError> {
        todo!()
    }

    /// Remove a file from being tracked in the metastore.
    pub(crate) async fn remove_file(&self, path: &str) -> Result<(), MetastoreError> {
        todo!()
    }

    /// Migrates a set of files from one tablet to another tablet.
    pub(crate) async fn migrate_files(
        &self,
        from: TabletId,
        to: TabletId,
    ) -> Result<(), MetastoreError> {
        todo!()
    }

    /// Returns a list of all files currently within the metastore.
    pub async fn list_all_files(
        &self,
    ) -> Result<Vec<(FileUrl, FileMetadata)>, MetastoreError> {
        todo!()
    }

    /// Returns a list of all tablets.
    pub async fn list_tablets(&self) -> Result<Vec<TabletId>, MetastoreError> {
        todo!()
    }

    /// Returns a list of all files within the given tablet.
    pub async fn list_files_in_tablet(
        &self,
        tablet_id: TabletId,
    ) -> Result<Vec<(FileUrl, FileMetadata)>, MetastoreError> {
        todo!()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TabletId(pub(super) ulid::Ulid);

impl TabletId {
    #[allow(clippy::new_without_default)]
    /// Creates a new [TabletId] with a unique ID.
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }
}

impl Display for TabletId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <ulid::Ulid as Display>::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd)]
pub struct FileUrl {
    path: String,
    tablet_id: TabletId,
}

impl FileUrl {
    /// Creates a new [FileUrl] using the given components.
    pub fn new(path: &str, tablet_id: TabletId) -> Self {
        Self {
            path: path.to_string(),
            tablet_id,
        }
    }
}

impl Display for FileUrl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "lnx://{}/{}", self.tablet_id, self.path)
    }
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// The start and stop position of the file in the larger tablet.
    pub position: Range<u64>,
    /// The timestamp when the file was created.
    pub created_at: i64,
    /// The timestamp when the file was last updated.
    pub updated_at: i64,
}
