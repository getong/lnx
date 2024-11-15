use std::future::Future;

use anyhow::Result;
use tokio::select;

/// Waits for the OS shutdown signal to be received.
pub fn wait_shutdown_signal() -> Result<impl Future<Output = ()>> {
    use tokio::signal::unix::SignalKind;

    let mut quit_signal = tokio::signal::unix::signal(SignalKind::quit())?;
    let mut interrupt_signal = tokio::signal::unix::signal(SignalKind::interrupt())?;
    let mut terminate_signal = tokio::signal::unix::signal(SignalKind::terminate())?;

    Ok(async move {
        select! {
            _ = quit_signal.recv() => {},
            _ = interrupt_signal.recv() => {},
            _ = terminate_signal.recv() => {},
        }
    })
}