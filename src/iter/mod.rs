//! Queue iterators.

use {Queue, Result};

/// Iterator condition.
pub trait NextMessage {
    /// Returns the next message. Or `None` if the iterator is complete.
    fn next(&self, &Queue) -> Option<Result<String>>;
}

/// Generic type for iterating through a queue.
pub struct MessageIter<'a, N>
    where N: NextMessage
{
    next_message: N,
    queue: &'a Queue<'a>,
}

impl<'a, N> MessageIter<'a, N>
    where N: NextMessage
{
    /// Constructs new `MessageIter` given the iterator condition `N`.
    pub fn new(queue: &'a Queue<'a>, n: N) -> Self {
        MessageIter {
            next_message: n,
            queue: queue,
        }
    }
}

impl<'a, N> Iterator for MessageIter<'a, N>
    where N: NextMessage
{
    type Item = Result<String>;

    fn next(&mut self) -> Option<Result<String>> {
        self.next_message.next(self.queue)
    }
}

/// Iterate forever, blocking when the queue is empty.
pub struct NextMessageBlocking;
impl NextMessage for NextMessageBlocking {
    fn next(&self, q: &Queue) -> Option<Result<String>> {
        Some(q.pop_blocking())
    }
}

/// Iterate until queue is empty.
pub struct NextMessagePending;
impl NextMessage for NextMessagePending {
    fn next(&self, q: &Queue) -> Option<Result<String>> {
        match q.pop() {
            Ok(Some(m)) => Some(Ok(m)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
