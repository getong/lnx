use std::io;
use std::path::{Path, PathBuf};
use async_trait::async_trait;
use crate::TabletId;

mod writer;
mod reader;

pub use self::writer::{TabletWriter, TabletWriterOptions, WriteResponse};
pub use self::reader::{TabletReader, TabletReaderOptions};

#[async_trait(?Send)]
/// A factory that creates actor tasks from within the context
/// of a glommio runtime.
pub trait ActorFactory: Send {
    /// Spawns an actor instance with the pre-configured state in factory.
    async fn spawn_actor(self) -> io::Result<()>;
}


pub(super) fn get_tablet_file_path(base: &Path, tablet_id: TabletId) -> PathBuf {
    base.join(tablet_id.to_string()).with_extension("tablet")
}