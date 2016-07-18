//! Queue iterators.

use std::marker::PhantomData;

use {Queue, Result};

/// Iterator condition.
pub trait NextMessage<T> {
    /// Returns the next message. Or `None` if the iterator is complete.
    fn next(&self, &Queue<T>) -> Option<Result<T>> where T: From<Vec<u8>> + Into<Vec<u8>>;
}

/// Generic type for iterating through a queue.
pub struct MessageIter<'a, N, T>
    where N: NextMessage<T>,
          T: 'a + From<Vec<u8>> + Into<Vec<u8>>
{
    next_message: N,
    queue: &'a Queue<'a, T>,
    phantom: PhantomData<T>,
}

impl<'a, N, T> MessageIter<'a, N, T>
    where N: NextMessage<T>,
          T: 'a + From<Vec<u8>> + Into<Vec<u8>>
{
    /// Constructs new `MessageIter` given the iterator condition `N`.
    pub fn new(queue: &'a Queue<'a, T>, n: N) -> Self {
        MessageIter {
            next_message: n,
            queue: queue,
            phantom: PhantomData,
        }
    }
}

impl<'a, N, T> Iterator for MessageIter<'a, N, T>
    where N: NextMessage<T>,
          T: From<Vec<u8>> + Into<Vec<u8>>
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        self.next_message.next(self.queue)
    }
}

/// Iterate forever, blocking when the queue is empty.
pub struct NextMessageBlocking;
impl<T> NextMessage<T> for NextMessageBlocking {
    fn next(&self, q: &Queue<T>) -> Option<Result<T>>
        where T: From<Vec<u8>> + Into<Vec<u8>>
    {
        Some(q.pop_blocking())
    }
}

/// Iterate until queue is empty.
pub struct NextMessagePending;
impl<T> NextMessage<T> for NextMessagePending {
    fn next(&self, q: &Queue<T>) -> Option<Result<T>>
        where T: From<Vec<u8>> + Into<Vec<u8>>
    {
        match q.pop() {
            Ok(Some(m)) => Some(Ok(m)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
