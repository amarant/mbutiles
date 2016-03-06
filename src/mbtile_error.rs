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
    pub message: Option<String>,
    pub inner_error: InnerError,
}

impl MBTileError {
    pub fn new_static(message: &'static str) -> MBTileError {
        MBTileError {
            message: Some(message.to_owned()),
            inner_error: InnerError::None,
        }
    }

    pub fn new(message: String) -> MBTileError {
        MBTileError {
            message: Some(message),
            inner_error: InnerError::None,
        }
    }
}

impl Display for MBTileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}{}", self.message, self.inner_error)
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

macro_rules! MBTileError_from_Tuple{
    ($source_error:ty, $message_type:ty) => (
        impl convert::From<($source_error, $message_type)> for MBTileError {
            fn from((kind, message): ($source_error, $message_type)) -> MBTileError {
                MBTileError { message: Some(message.to_owned()), inner_error: convert::From::from(kind) }
            }
        }
    )
}

MBTileError_from_Tuple!(io::Error, String);
MBTileError_from_Tuple!(io::Error, &'static str);
MBTileError_from_Tuple!(walkdir::Error, String);
MBTileError_from_Tuple!(walkdir::Error, &'static str);
MBTileError_from_Tuple!(rusqlite::Error, String);
MBTileError_from_Tuple!(rusqlite::Error, &'static str);
MBTileError_from_Tuple!(num::ParseIntError, String);
MBTileError_from_Tuple!(num::ParseIntError, &'static str);

macro_rules! MBTileError_from_Error {
    ($source_error:ty) => (
        impl convert::From<$source_error> for MBTileError {
            fn from(kind: $source_error) -> MBTileError {
                MBTileError { message: None, inner_error: convert::From::from(kind) }
            }
        }
    )
}

MBTileError_from_Error!(rusqlite::Error);

macro_rules! InnerError_from_Error {
    ($source_error:ty, $selector:ident) => (
        impl convert::From<($source_error)> for InnerError {
            fn from(error: $source_error) -> InnerError {
                InnerError::$selector(error)
            }
        }
    )
}

InnerError_from_Error!(io::Error, IO);
InnerError_from_Error!(walkdir::Error, WalkDir);
InnerError_from_Error!(rusqlite::Error, Rusqlite);
InnerError_from_Error!(num::ParseIntError, ParseInt);

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
