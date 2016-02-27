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

macro_rules! from_MBTileError {
    ($source_error:ty, $message_type:ty) => (
        impl convert::From<($source_error, $message_type)> for MBTileError {
            fn from((kind, message): ($source_error, $message_type)) -> MBTileError {
                MBTileError { message: message.to_owned(), inner_error: convert::From::from(kind) }
            }
        }
    )
}

from_MBTileError!(io::Error, String);
from_MBTileError!(io::Error, &'static str);
from_MBTileError!(walkdir::Error, String);
from_MBTileError!(walkdir::Error, &'static str);
from_MBTileError!(rusqlite::Error, String);
from_MBTileError!(rusqlite::Error, &'static str);
from_MBTileError!(num::ParseIntError, String);
from_MBTileError!(num::ParseIntError, &'static str);

macro_rules! from_InnerError {
    ($source_error:ty, $selector:ident) => (
        impl convert::From<($source_error)> for InnerError {
            fn from(error: $source_error) -> InnerError {
                InnerError::$selector(error)
            }
        }
    )
}

from_InnerError!(io::Error, IO);
from_InnerError!(walkdir::Error, WalkDir);
from_InnerError!(rusqlite::Error, Rusqlite);
from_InnerError!(num::ParseIntError, ParseInt);

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
