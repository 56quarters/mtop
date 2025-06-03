use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Handle;

/// Spawn a future that waits for a CTRL-C interrupt (SIGINT on Unix) and
/// sets the `interrupted` boolean to `true`.
pub async fn wait_for_interrupt(handle: Handle, interrupted: Arc<AtomicBool>) {
    handle.spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                interrupted.store(true, Ordering::Release);
            }
        }
    });
}
