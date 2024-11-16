use std::io;
use tracing::{debug, error, info, instrument};
use std::io::Result;
use std::path::PathBuf;
use anyhow::Context;
use async_trait::async_trait;
use futures_util::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder};
use tokio::sync::oneshot;

use crate::io::actors::ActorFactory;
use crate::io::body::Body;
use crate::TabletId;

/// A factory that creates a new [TabletWriterActor] for a given file path.
pub struct TabletWriterActorFactory {
    file_path: PathBuf,
    events: flume::Receiver<WriteEvent>,
}

#[async_trait(?Send)]
impl ActorFactory for TabletWriterActorFactory {
    async fn spawn_actor(&self) -> anyhow::Result<()> {
        let tablet_id = TabletId::new();
        
        let file = DmaFile::create(self.file_path.as_path())
            .await
            .map_err(io::Error::from)
            .context("Create DMA file for tablet")?;
        
        let writer = DmaStreamWriterBuilder::new(file)
            .with_write_behind(10)
            .with_buffer_size(256 << 10)
            .build();
        
        let actor = TabletWriterActor {
            tablet_id,
            events: self.events.clone(),
            writer,
            should_exit: false,
        };
        
        glommio::spawn_local(actor.run())
            .detach();
        
        Ok(())
    }
}

/// An actor that processes disk-IO operations and executes them.
///
/// This is primarily designed to run with [glommio] and perform
/// asynchronous, Direct IO disk access.
///
/// The actor is only designed to live for as long as the tablet it
/// is writing to, once the tablet has reached its maximum size it
/// will flush all buffer and exit.
pub struct TabletWriterActor {
    tablet_id: TabletId,
    events: flume::Receiver<WriteEvent>,
    writer: DmaStreamWriter,
    should_exit: bool,
}

impl TabletWriterActor {
    #[instrument("tablet-writer", skip(self), fields(tablet_id = %self.tablet_id))]
    async fn run(mut self) {
        info!("Writer is ready to process events");
        while let Ok(event) = self.events.recv_async().await {
            self.handle_event(event).await;

            if self.should_exit {
                break;
            }
        }

        info!("Writer is flushing and closing file");
        if let Err(e) = self.flush_and_close().await {
            error!(error = ?e, "Failed to flush and sync file data");
        }
        debug!("Writer has exited");
    }

    async fn flush_and_close(&mut self) -> Result<()> {
        self.writer.sync().await?;
        Ok(())
    }

    async fn handle_event(&mut self, event: WriteEvent) {
        if let Err(e) = self.copy_data_from_event(&event).await {
            let _ = event.ack.send(Err(e));
            return
        }
        
        let result = self
            .writer
            .sync()
            .await
            .map_err(io::Error::from)
            .map(|_| ());
        
        let _ = event.ack.send(result);
    }
    
    async fn copy_data_from_event(&mut self, event: &WriteEvent) -> Result<()> {
        loop {
            let Some(chunk) = event.body.next().await? else { return Ok(()) };
            self.writer.write_all(&chunk).await?;            
        }
    }
}


struct WriteEvent {
    /// The incoming body to write to the file.
    body: Body,
    /// The channel sender for acknowledging the write op
    /// has been completed.
    /// 
    /// This is only triggered once the body is written and flushed,
    /// or there is an error.
    ack: oneshot::Sender<Result<()>>,
}