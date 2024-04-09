use std::{convert, io};

use std::num;

use std::fmt::{self, Debug, Display};

use std::str;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum InnerError {
    #[error("")]
    None,
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
    #[error("SQLite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("Parse integer error: {0}")]
    ParseInt(#[from] num::ParseIntError),
    #[error("Directory Walker error: {0}")]
    WalkDir(#[from] walkdir::Error),
    #[error("Json parser error: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Zip error: {0}")]
    DataError(#[from] flate2::CompressError),
    #[error("Utf8 error: {0}")]
    Utf8Error(#[from] str::Utf8Error),
    #[error("regex error: {0}")]
    Regex(#[from] regex::Error),
}

#[derive(Debug)]
pub struct MBTileError {
    pub message: Option<String>,
    pub source: InnerError,
}

impl MBTileError {
    pub fn new_static(message: &'static str) -> MBTileError {
        MBTileError {
            message: Some(message.to_owned()),
            source: InnerError::None,
        }
    }

    pub fn new(message: String) -> MBTileError {
        MBTileError {
            message: Some(message),
            source: InnerError::None,
        }
    }
}

impl Display for MBTileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.message {
            Some(ref msg) => write!(f, "{:?}{}", msg, self.source),
            None => write!(f, "{}", self.source),
        }
    }
}

// impl Debug for MBTileError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{}", self)
//     }
// }

pub type MBTypeResult<U> = Result<U, MBTileError>;

pub trait ToMBTileResult<U, E> {
    fn desc<S: Into<String>>(self, description: S) -> Result<U, MBTileError>;
}

macro_rules! to_MBTileResult {
    ($($p:ty,)*) => (
        $(
            impl<U> ToMBTileResult<U, $p> for Result<U, $p>     {
                fn desc<S: Into<String>>(self, description: S) -> MBTypeResult<U> {
                    self.map_err(|err|
                        MBTileError {
                            message: Some(description.into()),
                            source: convert::From::from(err)
                        }
                    )
                }
            }
        )*
    )
}

to_MBTileResult!(
    io::Error,
    walkdir::Error,
    rusqlite::Error,
    num::ParseIntError,
    serde_json::Error,
    flate2::CompressError,
    str::Utf8Error,
    regex::Error,
);

macro_rules! MBTileError_from_Error {
    ($source_error:ty) => {
        impl convert::From<$source_error> for MBTileError {
            fn from(kind: $source_error) -> MBTileError {
                MBTileError {
                    message: None,
                    source: convert::From::from(kind),
                }
            }
        }
    };
}

MBTileError_from_Error!(rusqlite::Error);
MBTileError_from_Error!(io::Error);
MBTileError_from_Error!(flate2::CompressError);
MBTileError_from_Error!(serde_json::Error);
MBTileError_from_Error!(str::Utf8Error);
MBTileError_from_Error!(regex::Error);

// macro_rules! InnerError_from_Error {
//     ($source_error:ty, $selector:ident) => (
//         impl convert::From<$source_error> for InnerError {
//             fn from(error: $source_error) -> InnerError {
//                 InnerError::$selector(error)
//             }
//         }
//     )
// }
//
// InnerError_from_Error!(io::Error, IO);
// InnerError_from_Error!(walkdir::Error, WalkDir);
// InnerError_from_Error!(rusqlite::Error, Rusqlite);
// InnerError_from_Error!(num::ParseIntError, ParseInt);
// InnerError_from_Error!(serde::de::Error, ParserError);
// InnerError_from_Error!(flate2::DataError, DataError);
// InnerError_from_Error!(serde::ser::Error, EncoderError);
// InnerError_from_Error!(str::Utf8Error, Utf8Error);
// InnerError_from_Error!(regex::Error, Regex);
//
// impl Display for InnerError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match *self {
//             InnerError::None => write!(f, ""),
//             InnerError::IO(ref err) => write!(f, ", IO error: {}", err),
//             InnerError::Rusqlite(ref err) => write!(f, ", SQLite error: {}", err),
//             InnerError::ParseInt(ref err) => write!(f, ", Parse integer error: {}", err),
//             InnerError::WalkDir(ref err) => write!(f, ", Directory Walker error: {}", err),
//             InnerError::ParserError(ref err) => write!(f, ", Json parser error: {}", err),
//             InnerError::DataError(ref err) => write!(f, ", Zip error: {}", err),
//             InnerError::EncoderError(ref err) => write!(f, ", Json Encoder error: {}", err),
//             InnerError::Utf8Error(ref err) => write!(f, ", Utf8 error: {}", err),
//             InnerError::Regex(ref err) => write!(f, ", regex error: {}", err),
//         }
//     }
// }
