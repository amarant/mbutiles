use std::{convert, io};
use rusqlite;
use std::num;
use walkdir;
use std::fmt::{self, Debug, Display};

#[derive(Debug)]
pub enum InnerError {
    None,
    IO(io::Error),
    Rusqlite(rusqlite::Error),
    ParseInt(num::ParseIntError),
    WalkDir(walkdir::Error),
}

pub struct MBTileError {
    pub message: String,
    pub inner_error: InnerError,
}

impl Display for MBTileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.message, self.inner_error)
    }
}

impl Debug for MBTileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[macro_export]
macro_rules! try_desc {
    ($expr:expr, $arg:expr) => (match $expr {
        Ok(val) => val,
        Err(err) => {
            return Err(convert::From::from((err, $arg)));
        }
    });
}

impl convert::From<(io::Error, String)> for MBTileError {
    fn from((kind, message): (io::Error, String)) -> MBTileError {
        MBTileError { message: message.to_owned(), inner_error: convert::From::from(kind) }
    }
}

impl convert::From<(io::Error, &'static str)> for MBTileError {
    fn from((kind, message): (io::Error, &'static str)) -> MBTileError {
        MBTileError { message: message.to_owned(), inner_error: convert::From::from(kind) }
    }
}

impl convert::From<(io::Error)> for InnerError {
    fn from(error: io::Error) -> InnerError {
        InnerError::IO(error)
    }
}

impl convert::From<(rusqlite::Error, &'static str)> for MBTileError {
    fn from((kind, message): (rusqlite::Error, &'static str)) -> MBTileError {
        MBTileError { message: message.to_owned(), inner_error: convert::From::from(kind) }
    }
}

impl convert::From<(rusqlite::Error)> for InnerError {
    fn from(error: rusqlite::Error) -> InnerError {
        InnerError::Rusqlite(error)
    }
}

pub trait ToMBTilesResult<T: Sized, E: Sized> {
    fn to_mbtiles_result(self, message: String) -> Result<T, MBTileError>;
}

impl<T: Sized> ToMBTilesResult<T, io::Error> for Result<T, io::Error> {
    fn to_mbtiles_result(self, message: String) -> Result<T, MBTileError> {
        self.map_err(|err| {
            MBTileError {
                message: message,
                inner_error: InnerError::IO(err),
            }
        })
    }
}

impl<T: Sized> ToMBTilesResult<T, rusqlite::Error> for Result<T, rusqlite::Error> {
    fn to_mbtiles_result(self, message: String) -> Result<T, MBTileError> {
        self.map_err(|err| {
            MBTileError {
                message: message,
                inner_error: InnerError::Rusqlite(err),
            }
        })
    }
}

impl<T: Sized> ToMBTilesResult<T, num::ParseIntError> for Result<T, num::ParseIntError> {
    fn to_mbtiles_result(self, message: String) -> Result<T, MBTileError> {
        self.map_err(|err| {
            MBTileError {
                message: message,
                inner_error: InnerError::ParseInt(err),
            }
        })
    }
}

impl<T: Sized> ToMBTilesResult<T, walkdir::Error> for Result<T, walkdir::Error> {
    fn to_mbtiles_result(self, message: String) -> Result<T, MBTileError> {
        self.map_err(|err| {
            MBTileError {
                message: message,
                inner_error: InnerError::WalkDir(err),
            }
        })
    }
}

impl Display for InnerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            InnerError::None => write!(f, ""),
            InnerError::IO(ref err) => write!(f, ", IO error: {}", err),
            InnerError::Rusqlite(ref err) => write!(f, ", SQLite error: {}", err),
            InnerError::ParseInt(ref err) => write!(f, ", Parse integer error: {}", err),
            InnerError::WalkDir(ref err) => write!(f, ", Directory Walker error: {}", err),
        }
    }
}
