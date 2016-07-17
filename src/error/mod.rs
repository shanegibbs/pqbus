//! Error types

// use postgres::error::ConnectError;
use postgres::error::Error as PostgresError;
use retry::RetryError;

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
    /// Failed to create queue.
    Create(PostgresError),
    /// Failed query the size of the queue.
    Size(PostgresError),
    /// Connection failed.
    Connection(String, RetryError),
    /// SQL query failure.
    Sql(PostgresError),
}

impl From<PostgresError> for Error {
    fn from(err: PostgresError) -> Error {
        Error::Sql(err)
    }
}
