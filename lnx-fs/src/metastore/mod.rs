//! The metastore manages file metadata, offsets and position within
//! the virtual file system. It behaves a bit like S3.
//!
//! Internally it is backed by an SQLite database for each bucket.

mod db;

use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;

use crate::metastore::db::MetastoreDB;

/// The maximum amount of metadata to cache in memory in bytes.
const MAX_CACHE_CAPACITY: u64 = 8 << 10; // 4KB

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
    /// An LRU cache for accessing file information.
    cache: moka::sync::Cache<String, (TabletId, FileMetadata)>,
    /// THe SQLite DB wrapper for persisting file information.
    db: MetastoreDB,
}

impl Metastore {
    /// Connect to the metastore located at the given path.
    pub async fn connect(path: &str) -> Result<Self, MetastoreError> {
        let db = MetastoreDB::connect(path).await?;
        let cache = moka::sync::CacheBuilder::new(MAX_CACHE_CAPACITY)
            .weigher(|key: &String, _value: &(TabletId, FileMetadata)| {
                let size = key.as_bytes().len()
                    + 16  // size of ulid
                    + FileMetadata::SIZE_IN_CACHE;

                size as u32
            })
            .build();

        Ok(Self { cache, db })
    }

    /// Attempt to get a file with the given path.
    ///
    /// Returns the full [FileUrl] and [FileMetadata].
    pub(crate) async fn get_file(
        &self,
        path: &str,
    ) -> Result<Option<(FileUrl, FileMetadata)>, MetastoreError> {
        if let Some((tablet, metadata)) = self.cache.get(path) {
            return Ok(Some((FileUrl::new(path, tablet), metadata)));
        }

        let maybe_file = self.db.get_file(path).await?;

        if let Some((url, metadata)) = maybe_file.clone() {
            self.cache.insert(url.path, (url.tablet_id, metadata));
        }

        Ok(maybe_file)
    }

    /// Add a file to be tracked in the metastore.
    pub(crate) async fn add_file(
        &self,
        url: FileUrl,
        metadata: FileMetadata,
    ) -> Result<(), MetastoreError> {
        self.db.add_file(url.clone(), metadata.clone()).await?;

        self.cache.insert(url.path, (url.tablet_id, metadata));

        Ok(())
    }

    /// Remove a file from being tracked in the metastore.
    pub(crate) async fn remove_file(&self, path: &str) -> Result<(), MetastoreError> {
        self.db.remove_file(path).await?;

        self.cache.remove(path);

        Ok(())
    }

    /// Deletes all files associated on a tablet.
    pub(crate) async fn delete_tablet_files(
        &self,
        tablet: TabletId,
    ) -> Result<(), MetastoreError> {
        let changed = self.db.delete_tablet_files(tablet).await?;

        for path in changed {
            self.cache.remove(&path);
        }

        Ok(())
    }

    /// Returns a list of all files currently within the metastore.
    pub async fn list_all_files(
        &self,
    ) -> Result<Vec<(FileUrl, FileMetadata)>, MetastoreError> {
        self.db.list_all_files().await
    }

    /// Returns a list of all tablets.
    pub async fn list_tablets(&self) -> Result<Vec<TabletId>, MetastoreError> {
        self.db.list_tablets().await
    }

    /// Returns a list of all files within the given tablet.
    pub async fn list_files_in_tablet(
        &self,
        tablet_id: TabletId,
    ) -> Result<Vec<(FileUrl, FileMetadata)>, MetastoreError> {
        self.db.list_files_in_tablet(tablet_id).await
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

impl FileMetadata {
    const SIZE_IN_CACHE: usize = size_of::<Self>();
}