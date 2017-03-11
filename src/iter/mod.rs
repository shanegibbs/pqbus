//! Queue iterators.

use std::marker::PhantomData;
use super::{Message, FromMessageBody};
use {Queue, BusResult, PopError};

/// Iterator condition.
pub trait NextMessage<B, E> {
    /// Returns the next message. Or `None` if the iterator is complete.
    fn next(&self, queue: &Queue<B>) -> Option<Result<B, PopError<E>>> where B: FromMessageBody<E>;
}

/// Generic type for iterating through a queue.
pub struct MessageIter<'bus, 'queue, N, B, E>
    where N: NextMessage<B, E>,
          B: FromMessageBody<E> + 'queue,
          'bus: 'queue
{
    next_message: N,
    queue: &'queue Queue<'bus, B>,
    phantom: PhantomData<(B, E)>,
}

impl<'bus, 'queue, N, B, E> MessageIter<'bus, 'queue, N, B, E>
    where N: NextMessage<B, E>,
        B: FromMessageBody<E> + 'queue,
        'bus: 'queue
{
    /// Constructs new `MessageIter` given the iterator condition `N`.
    pub fn new(queue: &'queue Queue<'bus, B>, n: N) -> Self {
        MessageIter {
            next_message: n,
            queue: queue,
            phantom: PhantomData,
        }
    }
}

impl<'bus, 'queue, N, B, E> Iterator for MessageIter<'bus, 'queue, N, B, E>
    where N: NextMessage<B, E>,
          B: FromMessageBody<E>
{
    type Item = Result<B, PopError<E>>;

    fn next(&mut self) -> Option<Result<B, PopError<E>>> {
        self.next_message.next(self.queue)
    }
}

/// Iterate forever, blocking when the queue is empty.
pub struct NextMessageBlocking;
impl<B, E> NextMessage<B, E> for NextMessageBlocking {
    fn next(&self, q: &Queue<B>) -> Option<Result<B, PopError<E>>>
        where B: FromMessageBody<E>
    {
        Some(q.pop_blocking())
    }
}

/// Iterate until queue is empty.
pub struct NextMessagePending;
impl<B, E> NextMessage<B, E> for NextMessagePending {
    fn next(&self, q: &Queue<B>) -> Option<Result<B, PopError<E>>>
        where B: FromMessageBody<E>
    {
        match q.pop() {
            Ok(Some(m)) => Some(Ok(m)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
