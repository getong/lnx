use std::io;
use async_trait::async_trait;

mod writer;

pub use self::writer::{TableWriter, TabletWriterOptions, WriteResponse};

#[async_trait(?Send)]
/// A factory that creates actor tasks from within the context
/// of a glommio runtime.
pub trait ActorFactory: Send {
    /// Spawns an actor instance with the pre-configured state in factory.
    async fn spawn_actor(self) -> io::Result<()>;
}
