use tokio::sync::mpsc::Sender;
use tokio_stream::Stream;

pub type Filter<T> = fn(&T) -> bool;
pub struct BufferedAsyncStream<T> {
    capacity: usize,
    buffer: Vec<T>,
    consumers: Vec<(Sender<T>, Filter<T>)>,
}

impl<T> BufferedAsyncStream<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            buffer: Vec::with_capacity(capacity),
            consumers: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    pub async fn push(&mut self, element: T) {
        let mut maybe_element = Some(element);

        for (ref mut consumer, filter) in self.consumers.iter_mut() {
            match maybe_element {
                Some(element) => {
                    if (filter)(&element) {
                        match consumer.send(element).await {
                            Ok(_) => return,
                            Err(err) => maybe_element = Some(err.0),
                        }
                    } else {
                        maybe_element = Some(element)
                    }
                }
                None => break,
            }
        }

        self.consumers.retain(|(consumer, _)| !consumer.is_closed());
        if let Some(element) = maybe_element {
            self.buffer.push(element);
        }
    }

    pub async fn get_consumer(&mut self, filter: Filter<T>) -> impl Stream<Item = T> {
        let (tx, rx) = tokio::sync::mpsc::channel(self.capacity);

        for element in self.buffer.drain_filter(|element| (filter)(element)) {
            let _ = tx.send(element).await;
        }

        self.consumers.push((tx, filter));

        tokio_stream::wrappers::ReceiverStream::from(rx)
        // return rx;
    }
}

// pub struct BufferedAsyncStreamConsumer<T> {
//     filter_fn: Filter<T>,
//     inner_stream: Mutex<BufferedAsyncStream<T>>,
// }
//
// impl<T> tokio_stream::Stream for BufferedAsyncStreamConsumer<T> {
//     type Item = T;
//
//     fn poll_next(
//         self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> std::task::Poll<Option<Self::Item>> {
//         let mut guard_fut = Box::pin(self.inner_stream.lock());
//         match guard_fut.as_mut().poll(cx) {
//             Poll::Ready(mut inner_stream) => {
//                 let mut peek_fut = Box::pin(inner_stream.next(&self.filter_fn));
//                 peek_fut.as_mut().poll(cx)
//             }
//             Poll::Pending => Poll::Pending,
//         }
//     }
// }
