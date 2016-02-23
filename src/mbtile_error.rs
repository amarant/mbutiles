use std::io;
use rusqlite;
use std::num;
use walkdir;
use std::fmt::{self, Display, Debug};

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
