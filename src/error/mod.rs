use postgres::error::ConnectError;
use postgres::error::Error as PostgresError;

// An error encountered in pqbus
#[derive(Debug)]
pub enum Error {
    Connection(ConnectError),
    Sql(PostgresError),
}

impl From<ConnectError> for Error {
    fn from(err: ConnectError) -> Error {
        Error::Connection(err)
    }
}

impl From<PostgresError> for Error {
    fn from(err: PostgresError) -> Error {
        Error::Sql(err)
    }
}
