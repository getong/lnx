use bon::Builder;
use tracing::info;

use crate::metastore::{Metastore, MetastoreError};

macro_rules! set_config {
    ($slf:ident, $metastore:expr, $key:ident) => {{
        match &$slf.$key {
            MaybeUnset::Unset => Ok(()),
            MaybeUnset::None => {
                info!(key = stringify!($key), "Removing config value");
                $metastore.del_config_value(stringify!($key)).await
            },
            MaybeUnset::Some(v) => {
                info!(key = stringify!($key), value = v, "Setting config value");
                $metastore.set_config_value(stringify!($key), v).await
            },
        }
    }};
}

macro_rules! get_config {
    ($slf:ident, $metastore:expr, $key:ident) => {{
        $slf.$key = $metastore
            .get_config_value(stringify!($key))
            .await?
            .map(MaybeUnset::Some)
            .unwrap_or(MaybeUnset::Unset)
    }};
}

macro_rules! getters_with_option {
    ($key:ident, ty = $t:ty) => {
        pub(crate) fn $key(&self) -> Option<$t> {
            match self.$key {
                MaybeUnset::Unset => None,
                MaybeUnset::None => None,
                MaybeUnset::Some(v) => Some(v),
            }
        }
    };
}

#[derive(Debug, Default)]
#[cfg_attr(test, derive(Eq, PartialEq))]
/// Represents a config option which can be:
///
/// - `None` To remove the current set value.
/// - `Unset` To leave the value as it is currently set.
/// - `Some(T)` To update the value with a new value.
pub enum MaybeUnset<T> {
    #[default]
    Unset,
    None,
    Some(T),
}

impl<T> From<T> for MaybeUnset<T> {
    fn from(value: T) -> Self {
        Self::Some(value)
    }
}

#[derive(Debug, Default, Builder)]
#[cfg_attr(test, derive(Eq, PartialEq))]
/// Configuration options that can be adjusted at runtime on the bucket.
///
/// These are not "core" configs, they have sane defaults but may want to
/// be adjusted depending on your use case.
pub struct BucketConfig {
    #[builder(into, default, setters(vis = "pub(crate)"))]
    /// The name of the bucket.
    pub(crate) name: MaybeUnset<String>,
    #[builder(default, into)]
    /// Sets the maximum number of concurrent reads
    /// _on the same tablet_.
    pub max_concurrent_tablet_reads: MaybeUnset<usize>,
    #[builder(default, into)]
    /// The threshold from where a read will be changed from
    /// a single random read to a scan which will stream
    /// the data read.
    pub sequential_read_threshold_bytes: MaybeUnset<usize>,
    #[builder(default, into)]
    /// The maximum target size of a tablet.
    ///
    /// The system will always go slightly _over_ this limit, but it should
    /// be within some margin of error.
    pub max_tablet_size_bytes: MaybeUnset<u64>,
    #[builder(default, into)]
    /// The maximum number of concurrent tablet writers.
    ///
    /// More writers can increase the write throughput, but if your throughput
    /// is too low for the number of writers, you will have a lot of small files
    /// which increases the work for the vacuum step.
    pub max_active_writers: MaybeUnset<usize>,
    #[builder(default, into)]
    /// The maximum number of tablet readers that can be open at once and cached.
    ///
    /// This helps to limit the number of open files, but if this value is too low
    /// it can cause performance issues from having to constantly open and close files.
    pub max_open_readers: MaybeUnset<usize>,
    #[builder(default, into)]
    /// The time it takes in seconds, for a reader to be marked as IDLE following
    /// no usage activity.
    pub readers_time_to_idle_secs: MaybeUnset<u64>,
}

impl BucketConfig {
    pub(crate) async fn store_in_metastore(
        &self,
        metastore: &Metastore,
    ) -> Result<(), MetastoreError> {
        info!("Persisting bucket config changes");

        set_config!(self, metastore, name)?;
        set_config!(self, metastore, max_concurrent_tablet_reads)?;
        set_config!(self, metastore, sequential_read_threshold_bytes)?;
        set_config!(self, metastore, max_tablet_size_bytes)?;
        set_config!(self, metastore, max_active_writers)?;
        set_config!(self, metastore, max_open_readers)?;
        set_config!(self, metastore, readers_time_to_idle_secs)?;

        Ok(())
    }

    pub(crate) async fn load_from_metastore(
        &mut self,
        metastore: &Metastore,
    ) -> Result<(), MetastoreError> {
        info!("Loading bucket config changes");

        get_config!(self, metastore, name);
        get_config!(self, metastore, max_concurrent_tablet_reads);
        get_config!(self, metastore, sequential_read_threshold_bytes);
        get_config!(self, metastore, max_tablet_size_bytes);
        get_config!(self, metastore, max_active_writers);
        get_config!(self, metastore, max_open_readers);
        get_config!(self, metastore, readers_time_to_idle_secs);

        Ok(())
    }

    getters_with_option!(max_concurrent_tablet_reads, ty = usize);
    getters_with_option!(sequential_read_threshold_bytes, ty = usize);
    getters_with_option!(max_tablet_size_bytes, ty = u64);
    getters_with_option!(max_active_writers, ty = usize);
    getters_with_option!(max_open_readers, ty = usize);
    getters_with_option!(readers_time_to_idle_secs, ty = u64);
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_bucket_config_metastore_interactions_set_all() {
        let metastore = Metastore::connect(":memory:").await.unwrap();
        
        let cfg = BucketConfig::builder()
            .name("demo".to_string())
            .sequential_read_threshold_bytes(100)
            .max_tablet_size_bytes(100)
            .max_active_writers(10)
            .max_concurrent_tablet_reads(10)
            .max_open_readers(10)
            .build();
        cfg.store_in_metastore(&metastore).await.unwrap();
        
        let mut loaded = BucketConfig::default();
        loaded.load_from_metastore(&metastore).await.unwrap();
        assert_eq!(cfg, loaded, "Configs should match");
    }
    
    #[tokio::test]
    async fn test_bucket_config_metastore_interactions_update() {
        let metastore = Metastore::connect(":memory:").await.unwrap();

        let cfg = BucketConfig::builder()
            .name("demo".to_string())
            .sequential_read_threshold_bytes(100)
            .build();
        cfg.store_in_metastore(&metastore).await.unwrap();

        let mut loaded = BucketConfig::default();
        loaded.load_from_metastore(&metastore).await.unwrap();
        assert_eq!(cfg, loaded, "Configs should match");

        let cfg = BucketConfig::builder()
            .sequential_read_threshold_bytes(20)
            .build();
        cfg.store_in_metastore(&metastore).await.unwrap();

        let mut loaded = BucketConfig::default();
        loaded.load_from_metastore(&metastore).await.unwrap();
        assert_eq!(cfg.sequential_read_threshold_bytes, MaybeUnset::Some(20));
    }

    #[tokio::test]
    async fn test_bucket_config_metastore_interactions_unset() {
        let metastore = Metastore::connect(":memory:").await.unwrap();

        let cfg = BucketConfig::builder()
            .name("demo".to_string())
            .sequential_read_threshold_bytes(100)
            .build();
        cfg.store_in_metastore(&metastore).await.unwrap();

        let mut loaded = BucketConfig::default();
        loaded.load_from_metastore(&metastore).await.unwrap();
        assert_eq!(cfg, loaded, "Configs should match");

        let cfg = BucketConfig::builder()
            .sequential_read_threshold_bytes(MaybeUnset::None)
            .build();
        cfg.store_in_metastore(&metastore).await.unwrap();

        let mut loaded = BucketConfig::default();
        loaded.load_from_metastore(&metastore).await.unwrap();
        assert_eq!(loaded.sequential_read_threshold_bytes, MaybeUnset::Unset);
    }    
}