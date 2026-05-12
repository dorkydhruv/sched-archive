use std::{pin::Pin, task::{Context, Poll}};

use futures::Stream;

pub struct IntervalStream<Fut> 
where Fut: Unpin
{
    interval: tokio::time::Interval,
    fut: Pin<Box<dyn Fn()-> Fut + Send>>,
    in_progress: Option<Fut>,
}

impl <Fut, Output> IntervalStream<Fut>
where Fut: Future<Output = Output> + Unpin{
    pub fn new(interval: tokio::time::Interval, fut: Pin<Box<dyn Fn()-> Fut + Send>>) -> Self {
        Self {
            interval,
            fut,
            in_progress: None,
        }
    }
}

impl <Fut, Output> Stream for IntervalStream<Fut>
where Fut: Future<Output = Output> + Unpin{
    type Item = Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(in_progress) = &mut self.in_progress {
            match Pin::new(in_progress).poll(cx) {
                Poll::Ready(output) => {
                    self.in_progress = None;
                    Poll::Ready(Some(output))
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            match Pin::new(&mut self.interval).poll_tick(cx) {
                Poll::Ready(_) => {
                    let fut = (self.fut)();
                    self.in_progress = Some(fut);
                    Poll::Pending
                }
                Poll::Pending => Poll::Pending,
            }
        }
    }
}