#![feature(plugin)]
#![plugin(clippy)]
#![plugin(regex_macros)]
#![feature(trace_macros, log_syntax)]
// #[warn(unused_variable)]

extern crate rustc_serialize;
extern crate rusqlite;
extern crate walkdir;
extern crate regex;
#[macro_use(log, info, debug, error, warn)]
extern crate log;
extern crate stdio_logger;
extern crate zip;

#[macro_use]
mod mbtile_error;
mod mbtiles;

pub use mbtiles::{export, import};
