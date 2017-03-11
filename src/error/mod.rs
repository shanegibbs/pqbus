//! Error types

// use postgres::error::ConnectError;
use postgres::error::Error as PostgresError;
use retry::RetryError;
use std::fmt;

/// PqBus error types
#[derive(Debug)]
pub enum BusError {
    /// Failed to push message.
    Push(PostgresError),
    /// Failed to pop message.
    Pop(PostgresError),
    /// Failed to notify message available for consumption.
    Notify(PostgresError),
    /// Failed register a listener for the queue.
    Listen(PostgresError),
    /// Failed receive notification from queue.
    ReceiveNotification(PostgresError),
    /// Failed to create queue.
    Create(PostgresError),
    /// Failed query the size of the queue.
    Size(PostgresError),
    /// Connection failed.
    Connection(String, RetryError),
    /// SQL query failure.
    Sql(PostgresError),
    /// Name of bus does not match regex
    InvalidBusName(String),
    /// Name of queue does not match regex
    InvalidQueueName(String),
    Generic(String),
}

/// Queue push errors
#[derive(Debug)]
pub enum PushError<E> {
    Substrate(PostgresError),
    BodySeralize(E),
    Generic(String),
}

/// Queue pop errors
#[derive(Debug)]
pub enum PopError<E> {
    Pop(PostgresError),
    /// Failed to pop message.
    BodyDeseralize(E),
    Generic(String),
}

impl<E> From<BusError> for PopError<E> {
    fn from(e: BusError) -> Self {
        PopError::Generic(format!("{}", e))
    }
}

impl<E> From<PushError<E>> for BusError
    where PushError<E>: fmt::Display
{
    fn from(e: PushError<E>) -> Self {
        BusError::Generic(format!("{}", e))
    }
}

impl<E> From<PopError<E>> for BusError
    where PopError<E>: fmt::Display
{
    fn from(e: PopError<E>) -> Self {
        BusError::Generic(format!("{}", e))
    }
}

impl From<PostgresError> for BusError {
    fn from(err: PostgresError) -> BusError {
        BusError::Sql(err)
    }
}

impl<E> fmt::Display for PopError<E>
    where E: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::PopError::*;
        match *self {
            Pop(ref e) => write!(f, "{}", e),
            BodyDeseralize(ref e) => write!(f, "{}", e),
            Generic(ref e) => write!(f, "{}", e),
        }
    }
}

impl fmt::Display for BusError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::BusError::*;
        match *self {
            Push(ref e) => write!(f, "Message push failed: {}", e),
            Pop(ref e) => write!(f, "Message pop failed: {}", e),
            Notify(ref e) => write!(f, "Queue push notification failed: {}", e),
            Listen(ref e) => write!(f, "Failed to register listener form queue updates: {}", e),
            ReceiveNotification(ref e) => write!(f, "Failed to receive notification: {}", e),
            Create(ref e) => write!(f, "Failed to create queue: {}", e),
            Size(ref e) => write!(f, "Unable to get size of queue: {}", e),
            Connection(ref uri, ref e) => write!(f, "Failed to connect to bus {}: {}", uri, e),
            Sql(ref e) => write!(f, "SQL query failed: {}", e),
            InvalidBusName(ref e) => write!(f, "Invalid bus name: {}", e),
            InvalidQueueName(ref e) => write!(f, "Invalid queue name: {}", e),
            Generic(ref e) => write!(f, "{}", e),
        }
    }
}
