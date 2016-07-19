//! Error types

// use postgres::error::ConnectError;
use postgres::error::Error as PostgresError;
use retry::RetryError;
use std::fmt;

/// PqBus error types
#[derive(Debug)]
pub enum Error {
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
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Push(ref e) => write!(f, "Message push failed: {}", e),
            Error::Pop(ref e) => write!(f, "Message pop failed: {}", e),
            Error::Notify(ref e) => write!(f, "Queue push notification failed: {}", e),
            Error::Listen(ref e) => {
                write!(f, "Failed to register listener form queue updates: {}", e)
            }
            Error::ReceiveNotification(ref e) => write!(f, "Failed to receive notification: {}", e),
            Error::Create(ref e) => write!(f, "Failed to create queue: {}", e),
            Error::Size(ref e) => write!(f, "Unable to get size of queue: {}", e),
            Error::Connection(ref uri, ref e) => {
                write!(f, "Failed to connect to bus {}: {}", uri, e)
            }
            Error::Sql(ref e) => write!(f, "SQL query failed: {}", e),
            Error::InvalidBusName(ref e) => write!(f, "Invalid bus name: {}", e),
            Error::InvalidQueueName(ref e) => write!(f, "Invalid queue name: {}", e),
        }
    }
}

impl From<PostgresError> for Error {
    fn from(err: PostgresError) -> Error {
        Error::Sql(err)
    }
}
