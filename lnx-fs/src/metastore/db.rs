use std::str::FromStr;
use std::time::Duration;

use sqlx::FromRow;
use tracing::warn;

use crate::metastore::{FileUrl, MetastoreError, TabletId};
use crate::FileMetadata;

const POOL_SIZE: u32 = if cfg!(test) { 1 } else { 5 };

#[derive(Clone)]
pub struct MetastoreDB {
    pool: sqlx::SqlitePool,
}

impl MetastoreDB {
    /// Attempts to connect to the SQLite database at the given path.
    pub(crate) async fn connect(path: &str) -> Result<Self, MetastoreError> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .acquire_timeout(Duration::from_secs(10))
            .acquire_slow_threshold(Duration::from_secs(1))
            .max_connections(POOL_SIZE)
            .connect(path)
            .await?;

        let slf = Self { pool };

        slf.setup_tables().await?;

        Ok(slf)
    }

    async fn setup_tables(&self) -> Result<(), MetastoreError> {
        let query = r#"
        CREATE TABLE IF NOT EXISTS lnx__active_files (
            path TEXT NOT NULL PRIMARY KEY,
            extension TEXT,
            tablet_id TEXT NOT NULL,
            range_start BIGINT NOT NULL,
            range_end BIGINT NOT NULL,
            created_at BIGINT NOT NULL
        );
        
        CREATE UNIQUE INDEX IF NOT EXISTS path_lookup ON lnx__active_files (path);
        CREATE INDEX IF NOT EXISTS tablet_lookup ON lnx__active_files (tablet_id);
        CREATE INDEX IF NOT EXISTS extension_lookup ON lnx__active_files (extension);
        
        CREATE TABLE IF NOT EXISTS lnx__bucket_config (
            key TEXT NOT NULL PRIMARY KEY,
            value TEXT NOT NULL
        );
        "#;

        sqlx::query(query).execute(&self.pool).await?;

        Ok(())
    }

    /// Attempt to get a file with the given path.
    ///
    /// Returns the full [FileUrl] and [FileMetadata].
    pub(crate) async fn get_file(
        &self,
        path: &str,
    ) -> Result<Option<(FileUrl, FileMetadata)>, MetastoreError> {
        let query = r#"
            SELECT
                path,
                tablet_id,
                range_start,
                range_end,
                created_at
            FROM lnx__active_files
            WHERE path = ?;
        "#;

        let row: Option<FileRow> = sqlx::query_as(query)
            .bind(path)
            .fetch_optional(&self.pool)
            .await?;

        let Some(row) = row else { return Ok(None) };

        let tablet_id = ulid::Ulid::from_str(&row.tablet_id)
            .map_err(|e| {
                warn!(error = ?e, row = ?row, "Metastore row contains corrupted tablet_id, has the store be edited?");
                MetastoreError::Corrupted
            })?;

        let file_url = FileUrl {
            path: row.path,
            tablet_id: TabletId(tablet_id),
        };

        let metadata = FileMetadata {
            position: row.range_start as u64..row.range_end as u64,
            created_at: row.created_at,
        };

        Ok(Some((file_url, metadata)))
    }

    /// Add a file to be tracked in the metastore.
    pub(crate) async fn add_file(
        &self,
        url: FileUrl,
        metadata: FileMetadata,
    ) -> Result<(), MetastoreError> {
        let extension = url.path.rsplit_once('.').map(|parts| parts.1);

        let query = r#"
            INSERT INTO lnx__active_files (
                path,
                extension,
                tablet_id,
                range_start,
                range_end,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (path) 
            DO UPDATE SET             
                tablet_id = excluded.tablet_id,
                range_start = excluded.range_start,
                range_end = excluded.range_end,
                created_at = excluded.created_at;
        "#;

        sqlx::query(query)
            .bind(&url.path)
            .bind(extension)
            .bind(url.tablet_id.to_string())
            .bind(metadata.position.start as i64)
            .bind(metadata.position.end as i64)
            .bind(metadata.created_at)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Remove a file from being tracked in the metastore.
    pub(crate) async fn remove_file(&self, path: &str) -> Result<(), MetastoreError> {
        let query = r#"
            DELETE FROM lnx__active_files WHERE path = ?;
        "#;

        sqlx::query(query).bind(path).execute(&self.pool).await?;

        Ok(())
    }

    #[allow(unused)] // TODO: Add GC system
    /// Delete all files for a given tablet.
    pub(crate) async fn delete_tablet_files(
        &self,
        tablet: TabletId,
    ) -> Result<Vec<String>, MetastoreError> {
        let query = r#"
            DELETE lnx__active_files 
            WHERE tablet_id = ? 
            RETURNING path;
        "#;

        let file_paths: Vec<String> = sqlx::query_scalar(query)
            .bind(tablet.to_string())
            .fetch_all(&self.pool)
            .await?;

        Ok(file_paths)
    }

    /// Returns a list of all files currently within the metastore.
    pub async fn list_all_files(
        &self,
    ) -> Result<Vec<(FileUrl, FileMetadata)>, MetastoreError> {
        let query = r#"
            SELECT
                path,
                tablet_id,
                range_start,
                range_end,
                created_at
            FROM lnx__active_files;
        "#;

        let rows: Vec<FileRow> = sqlx::query_as(query).fetch_all(&self.pool).await?;

        let files = map_rows_to_files(rows);

        Ok(files)
    }

    /// Returns a list of all tablets.
    pub async fn list_tablets(&self) -> Result<Vec<TabletId>, MetastoreError> {
        let query = r#"
            SELECT DISTINCT
                tablet_id
            FROM lnx__active_files;
        "#;

        let rows: Vec<String> = sqlx::query_scalar(query).fetch_all(&self.pool).await?;

        let tablets = rows
            .into_iter()
            .filter_map(|id| {
                match ulid::Ulid::from_str(&id) {
                    Ok(id) => Some(id),
                    Err(e) => {
                        warn!(error = ?e, tablet_id = ?id, "Metastore row contains corrupted tablet_id, has the store be edited?");
                        None
                    }
                }
            })
            .map(TabletId)
            .collect();

        Ok(tablets)
    }

    #[allow(unused)] // TODO: Add GC system
    /// Returns a list of all files within the given tablet.
    pub async fn list_files_in_tablet(
        &self,
        tablet_id: TabletId,
    ) -> Result<Vec<(FileUrl, FileMetadata)>, MetastoreError> {
        let query = r#"
            SELECT
                path,
                tablet_id,
                range_start,
                range_end,
                created_at
            FROM lnx__active_files
            WHERE tablet_id = ?;
        "#;

        let rows: Vec<FileRow> = sqlx::query_as(query)
            .bind(tablet_id.to_string())
            .fetch_all(&self.pool)
            .await?;

        let files = map_rows_to_files(rows);

        Ok(files)
    }

    /// Returns a list of all files which have the given extension.
    pub async fn list_files_with_ext(
        &self,
        extension: &str,
    ) -> Result<Vec<(FileUrl, FileMetadata)>, MetastoreError> {
        let query = r#"
            SELECT
                path,
                tablet_id,
                range_start,
                range_end,
                created_at
            FROM lnx__active_files
            WHERE extension = ?;
        "#;

        let rows: Vec<FileRow> = sqlx::query_as(query)
            .bind(extension)
            .fetch_all(&self.pool)
            .await?;

        let files = map_rows_to_files(rows);

        Ok(files)
    }

    /// Attempts to retrieve a configuration value with the given key.
    pub async fn get_config_value<V>(
        &self,
        key: &str,
    ) -> Result<Option<V>, MetastoreError>
    where
        V: serde::de::DeserializeOwned,
    {
        let query = r#"
            SELECT value
            FROM lnx__bucket_config
            WHERE key = ?;
        "#;

        let value: Option<String> = sqlx::query_scalar(query)
            .bind(key)
            .fetch_optional(&self.pool)
            .await?;

        value
            .map(|v| serde_json::from_str(&v).map_err(MetastoreError::ConfigSerdeError))
            .transpose()
    }

    /// Attempts to set a config value with the given key.
    ///
    /// This is implemented as an UPSERT.
    pub async fn set_config_value<V>(
        &self,
        key: &str,
        value: &V,
    ) -> Result<(), MetastoreError>
    where
        V: serde::Serialize + ?Sized,
    {
        let query = r#"
            INSERT INTO lnx__bucket_config (key, value)
            VALUES (?, ?) 
            ON CONFLICT (key) 
            DO UPDATE SET value = excluded.value;
        "#;

        let value =
            serde_json::to_string(&value).map_err(MetastoreError::ConfigSerdeError)?;
        sqlx::query(query)
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Deletes a config value with a given key.
    pub async fn del_config_value(&self, key: &str) -> Result<(), MetastoreError> {
        let query = r#"
            DELETE FROM lnx__bucket_config WHERE key = ?;
        "#;

        sqlx::query(query).bind(key).execute(&self.pool).await?;

        Ok(())
    }
}

#[derive(Debug, FromRow)]
struct FileRow {
    path: String,
    tablet_id: String,
    range_start: i64,
    range_end: i64,
    created_at: i64,
}

fn map_rows_to_files(rows: Vec<FileRow>) -> Vec<(FileUrl, FileMetadata)> {
    rows
        .into_iter()
        .filter_map(|row| {
            let tablet_id = match ulid::Ulid::from_str(&row.tablet_id) {
                Ok(id) => id,
                Err(e) => {
                    warn!(error = ?e, row = ?row, "Metastore row contains corrupted tablet_id, has the store be edited?");
                    return None
                }
            };

            let file_url = FileUrl {
                path: row.path,
                tablet_id: TabletId(tablet_id),
            };

            let metadata = FileMetadata {
                position: row.range_start as u64..row.range_end as u64,
                created_at: row.created_at,
            };

            Some((file_url, metadata))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_table_setup() {
        let _metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");
    }

    #[tokio::test]
    async fn test_add_and_get_files() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        metastore
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            )
            .await
            .expect("Add file");
        metastore
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            )
            .await
            .expect("Add file");

        let (url, _metadata) = metastore
            .get_file("foo/sample.gzip")
            .await
            .expect("Get file from metastore")
            .expect("File should exist");
        assert_eq!(url.path, "foo/sample.gzip");
        assert_eq!(url.tablet_id, tablet);

        let (url, metadata) = metastore
            .get_file("foo/bar/example.txt")
            .await
            .expect("Get file from metastore")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(url.tablet_id, tablet);
        assert_eq!(metadata.position, 0..128);
        assert_eq!(metadata.created_at, 12314);
    }

    #[tokio::test]
    async fn test_add_and_list_all_files() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        metastore
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            )
            .await
            .expect("Add file");
        metastore
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            )
            .await
            .expect("Add file");

        let files = metastore.list_all_files().await.expect("List all files");
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_add_and_list_tablets() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        metastore
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            )
            .await
            .expect("Add file");
        metastore
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            )
            .await
            .expect("Add file");

        let files = metastore.list_tablets().await.expect("List all tablets");
        assert_eq!(files.len(), 1);

        metastore
            .add_file(
                FileUrl::new("foo/sample2.gzip", TabletId::new()),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            )
            .await
            .expect("Add file");

        let files = metastore.list_tablets().await.expect("List all tablets");
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_add_and_list_tablet_files() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        metastore
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            )
            .await
            .expect("Add file");
        metastore
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            )
            .await
            .expect("Add file");
        metastore
            .add_file(
                FileUrl::new("foo/sample2.gzip", TabletId::new()),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            )
            .await
            .expect("Add file");

        let files = metastore
            .list_files_in_tablet(tablet)
            .await
            .expect("List all files");
        assert_eq!(files.len(), 2);
    }

    #[tokio::test]
    async fn test_add_and_list_extension_files() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        metastore
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            )
            .await
            .expect("Add file");
        metastore
            .add_file(
                FileUrl::new("foo/sample.gzip", tablet),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            )
            .await
            .expect("Add file");
        metastore
            .add_file(
                FileUrl::new("foo/sample2.gzip", TabletId::new()),
                FileMetadata {
                    position: 42..422,
                    created_at: 234243234,
                },
            )
            .await
            .expect("Add file");

        let files = metastore
            .list_files_with_ext("gzip")
            .await
            .expect("List files");
        assert_eq!(files.len(), 2);
        let files = metastore
            .list_files_with_ext("txt")
            .await
            .expect("List files");
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn test_add_and_remove_files() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        let tablet = TabletId::new();

        metastore
            .add_file(
                FileUrl::new("foo/bar/example.txt", tablet),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            )
            .await
            .expect("Add file");

        let (url, _metadata) = metastore
            .get_file("foo/bar/example.txt")
            .await
            .expect("Get file from metastore")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(url.tablet_id, tablet);

        metastore
            .remove_file("foo/bar/example.txt")
            .await
            .expect("Remove file");

        let maybe_file = metastore
            .get_file("foo/bar/example.txt")
            .await
            .expect("Get file from metastore");
        assert!(maybe_file.is_none(), "File should be deleted");
    }

    #[tokio::test]
    async fn test_remove_missing_file() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        metastore
            .remove_file("foo/sample.gzip")
            .await
            .expect("Missing files should be ignored");
    }

    #[tokio::test]
    async fn test_add_duplicate_file() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        metastore
            .add_file(
                FileUrl::new("foo/bar/example.txt", TabletId::new()),
                FileMetadata {
                    position: 0..128,
                    created_at: 12314,
                },
            )
            .await
            .expect("Add file");

        metastore
            .add_file(
                FileUrl::new("foo/bar/example.txt", TabletId::new()),
                FileMetadata {
                    position: 0..128,
                    created_at: 123,
                },
            )
            .await
            .expect("Metastore should allow duplicate path keys and update");

        let (url, metadata) = metastore
            .get_file("foo/bar/example.txt")
            .await
            .expect("Get file from metastore")
            .expect("File should exist");
        assert_eq!(url.path, "foo/bar/example.txt");
        assert_eq!(metadata.created_at, 123);
    }

    #[tokio::test]
    async fn test_config_kv() {
        let metastore = MetastoreDB::connect(":memory:")
            .await
            .expect("Create metastore SQLite table");

        metastore
            .set_config_value("name", "demo")
            .await
            .expect("Set config name");
        metastore
            .set_config_value("age", &1234)
            .await
            .expect("Set config name");

        let name: String = metastore
            .get_config_value("name")
            .await
            .expect("Get config value")
            .expect("Config value should exist");
        assert_eq!(name, "demo");

        let age: usize = metastore
            .get_config_value("age")
            .await
            .expect("Get config value")
            .expect("Config value should exist");
        assert_eq!(age, 1234);

        let missing: Option<()> = metastore
            .get_config_value("missing")
            .await
            .expect("Get config value");
        assert!(missing.is_none());

        let invalid = metastore
            .get_config_value::<usize>("name")
            .await
            .expect_err("Serde should raise an error");
        assert!(matches!(invalid, MetastoreError::ConfigSerdeError(_)));
    }
}
