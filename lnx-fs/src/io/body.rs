use std::io;
use std::io::ErrorKind;

use bytes::Bytes;
use flume::RecvError;
use tracing::warn;

/// A body is a stream of incoming bytes.
pub struct Body {
    incoming: flume::Receiver<Option<Bytes>>,
}

impl Body {
    /// Creates a new [Body] which can stream chunks via a channel.
    pub fn channel() -> (flume::Sender<Option<Bytes>>, Self) {
        let (tx, rx) = flume::bounded(2);
        (tx, Self { incoming: rx })
    }

    /// Creates a body that is empty.
    pub fn empty() -> Self {
        let (tx, slf) = Self::channel();
        let _ = tx.try_send(None);
        slf
    }

    /// Creates a body that is complete as a single [Bytes] object.
    pub fn complete(body: Bytes) -> Self {
        let (tx, slf) = Self::channel();
        let _ = tx.try_send(Some(body));
        let _ = tx.try_send(None);
        slf
    }

    /// Attempts to read the next chunk of the available body.
    ///
    /// Returns `Ok(None)` if the body reading is complete.
    pub async fn next(&self) -> io::Result<Option<Bytes>> {
        self.incoming.recv_async().await.map_err(|e| {
            if matches!(e, RecvError::Disconnected) {
                warn!(
                    "IO body channel was disconnected before reading could be completed"
                );
            }
            io::Error::new(ErrorKind::Interrupted, e.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_body_empty() {
        let body = Body::empty();
        let chunk = body.next().await.expect("Body read should be OK");
        assert!(chunk.is_none());
    }

    #[tokio::test]
    async fn test_body_complete() {
        let msg = Bytes::from_static(b"Hello, World!");
        let body = Body::complete(msg.clone());
        let chunk = body.next().await.expect("Body read should be OK");
        assert_eq!(chunk, Some(msg));
        let chunk = body.next().await.expect("Body read should be OK");
        assert!(chunk.is_none());
    }
}
