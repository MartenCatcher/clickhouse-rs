use std::{borrow::Cow, io, mem, result, str::Utf8Error, string::FromUtf8Error};

use failure::*;
use tokio::prelude::*;
use tokio_timer::timeout::Error as TimeoutError;
use tokio_timer::Error as TimerError;
use url::ParseError;

use crate::types::Packet;

/// Result type alias for this library.
pub type Result<T> = result::Result<T, Error>;

/// This type enumerates library errors.
#[derive(Debug)]
pub enum Error {
    Driver(DriverError),

    Io(io::Error),

    Connection(ConnectionError),

    Other(failure::Error),

    Server(ServerError),

    Url(UrlError),

    FromSql(FromSqlError),
}

/// This type represents Clickhouse server error.
#[derive(Debug, Clone)]
pub struct ServerError {
    pub code: u32,
    pub name: String,
    pub message: String,
    pub stack_trace: String,
}

/// This type enumerates connection errors.
#[derive(Debug)]
pub enum ConnectionError {
    TlsHostNotProvided,

    IoError(io::Error),

    #[cfg(feature = "tls")]
    TlsError(native_tls::Error),
}

/// This type enumerates connection URL errors.
#[derive(Debug)]
pub enum UrlError {
    Invalid,

    InvalidParamValue { param: String, value: String },

    Parse(ParseError),

    UnknownParameter { param: String },

    UnsupportedScheme { scheme: String },
}

/// This type enumerates driver errors.
#[derive(Debug)]
pub enum DriverError {
    Overflow,

    UnknownPacket { packet: u64 },

    UnexpectedPacket,

    Timeout,

    Utf8Error(Utf8Error),
}

/// This type enumerates cast from sql type errors.
#[derive(Debug)]
pub enum FromSqlError {
    InvalidType {
        src: Cow<'static, str>,
        dst: Cow<'static, str>,
    },

    OutOfRange,

    UnsupportedOperation,
}

impl From<DriverError> for Error {
    fn from(err: DriverError) -> Self {
        Error::Driver(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<ServerError> for Error {
    fn from(err: ServerError) -> Self {
        Error::Server(err)
    }
}

impl From<UrlError> for Error {
    fn from(err: UrlError) -> Self {
        Error::Url(err)
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::Other(failure::Context::new(err).into())
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Error::Other(failure::Context::new(err.to_string()).into())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::Other(failure::Context::new(err).into())
    }
}

impl From<TimerError> for Error {
    fn from(err: TimerError) -> Self {
        Error::Other(failure::Context::new(err).into())
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Self {
        Error::Url(UrlError::Parse(err))
    }
}

impl From<TimeoutError<Error>> for Error {
    fn from(err: TimeoutError<Self>) -> Self {
        match err.into_inner() {
            None => Error::Driver(DriverError::Timeout),
            Some(inner) => inner,
        }
    }
}

impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Error::Driver(DriverError::Utf8Error(err))
    }
}

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Self {
        Error::Connection(err)
    }
}

impl<S> Into<Poll<Option<Packet<S>>, Error>> for Error {
    fn into(self) -> Poll<Option<Packet<S>>, Error> {
        let mut this = self;

        if let Error::Io(ref mut e) = &mut this {
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(Async::NotReady);
            }

            let me = mem::replace(e, io::Error::from(io::ErrorKind::Other));
            return Err(Error::Io(me));
        }

        warn!("ERROR: {:?}", this);
        Err(this)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Io(error) => error,
            e => io::Error::new(io::ErrorKind::Other, format!("{:?}", e)),
        }
    }
}
