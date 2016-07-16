// use postgres::error::ConnectError;
use postgres::error::Error as PostgresError;
use retry::RetryError;

// An error encountered in pqbus
#[derive(Debug)]
pub enum Error {
    Push(PostgresError),
    Pop(PostgresError),
    Notify(PostgresError),
    Listen(PostgresError),
    Create(PostgresError),
    Size(PostgresError),
    Connection(String, RetryError),
    Sql(PostgresError),
}

impl From<PostgresError> for Error {
    fn from(err: PostgresError) -> Error {
        Error::Sql(err)
    }
}
