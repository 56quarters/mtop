use crate::MtopError;
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// `Timeout` can be used to add a timeout to any future emitted by mtop.
pub trait Timeout: Sized {
    fn timeout<S>(self, t: Duration, operation: S) -> Timed<Self>
    where
        S: Into<String>;
}

impl<F, V> Timeout for F
where
    F: Future<Output = Result<V, MtopError>>,
{
    fn timeout<S>(self, t: Duration, operation: S) -> Timed<F>
    where
        S: Into<String>,
    {
        Timed {
            operation: operation.into(),
            time: t,
            inner: tokio::time::timeout(t, self),
        }
    }
}

pin_project! {
    #[derive(Debug)]
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Timed<T> {
        operation: String,
        time: Duration,
        #[pin]
        inner: tokio::time::Timeout<T>,
    }
}

impl<F, V> Future for Timed<F>
where
    F: Future<Output = Result<V, MtopError>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        // Poll the inner Timeout future and unwrap one layer of Result it adds,
        // converting a timeout into specific form of an MtopError
        this.inner.poll(cx).map(|res| match res {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(e),
            Err(_e) => Err(MtopError::timeout(*this.time, this.operation)),
        })
    }
}
