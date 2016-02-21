#![feature(plugin)]
#![plugin(clippy)]

extern crate rustc_serialize;
extern crate docopt;
extern crate rusqlite;
extern crate walkdir;
extern crate regex;
#[macro_use(log, info, debug, error)]
extern crate log;
extern crate stdio_logger;

use docopt::Docopt;
use rusqlite::Connection;
use std::iter::Iterator;
use walkdir::{DirEntry, WalkDir, WalkDirIterator};
use std::path::{Path, Component};
use std::fs::File;
use std::io;
use std::io::prelude::*;
use log::{Log, LogLevel};
use stdio_logger::Logger;
use std::num;
use std::io::Error;
use std::fmt;

const USAGE: &'static str = "
MBTiles utils.

Usage:
    mbutiles <command> [options] <input> \
                             [<output>]
    mbutiles -h | --help
    mbutiles --version

Options:
  -h --help                   Show this help message and exit.
  --verbose                   Show log info.
  --version                   Show version.
  --scheme=<scheme>           Tiling scheme of the tiles. Default is \"xyz\" (z/x/y),\
 other options are \"tms\" which is also z/x/y but uses a flipped y coordinate,\
 and \"wms\" which replicates the MapServer WMS TileCache directory structure\
 \"z/000/000/x/000/000/y.png\". [default: xyz]
  --image-format=<format>     The format of the image tiles, either png, jpg, webp or pbf.
  --grid-callback=<callback>  Option to control JSONP callback for UTFGrid tiles.\
 If grids are not used as JSONP, you can remove callbacks specifying --grid_callback=\"\".

 Commands:
    import
    export
    metadata
";

#[derive(RustcDecodable, Debug)]
enum Command {
    Import,
    Export,
    Metadata,
}

#[derive(RustcDecodable, Debug)]
enum Scheme {
    Xyz,
    Tms,
    Wms,
}

#[derive(RustcDecodable, Debug, Clone, Copy)]
enum ImageFormat {
    Png,
    Jpg,
    Webp,
    Pbf,
}

#[derive(RustcDecodable, Debug)]
struct Args {
    arg_command: Command,
    flag_verbose: bool,
    flag_scheme: Scheme,
    flag_image_format: Option<ImageFormat>,
    flag_grid_callback: Option<String>,
    arg_input: String,
    arg_output: Option<String>,
}

#[derive(Debug)]
enum InnerError {
    None,
    IO(io::Error),
    Rusqlite(rusqlite::Error),
    ParseInt(num::ParseIntError),
    WalkDir(walkdir::Error),
}

struct MBTileError {
    message: String,
    inner_error: InnerError,
}

impl std::fmt::Debug for MBTileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl std::fmt::Display for InnerError {
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

impl std::fmt::Display for MBTileError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.message, self.inner_error)
    }
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                         .and_then(|d| d.decode())
                         .unwrap_or_else(|e| e.exit());
    stdio_logger::init(if args.flag_verbose {
        LogLevel::Info
    } else {
        LogLevel::Error
    })
        .expect("Could not initialize logging");
    info!("{:?}", args);
    match args.arg_command {
        Command::Import => {
            // import tiles dir into mbtiles
            let input = args.arg_input.clone();
            let output = args.arg_output.unwrap_or_else(
                || format!("{}.mbtiles", input));
            if let Err(err) = import(&Path::new(&args.arg_input), &Path::new(&output),
                args.flag_scheme, args.flag_image_format, args.flag_grid_callback) {
                error!("{:?}", err);
            }
        },
        Command::Export =>
            // export mbtiles to a dir
            export(args.arg_input, args.arg_output,
                args.flag_scheme, args.flag_image_format, args.flag_grid_callback),
        Command::Metadata =>
            // dumps metadata
            metadata(args.arg_input, args.arg_output,
                args.flag_scheme, args.flag_image_format, args.flag_grid_callback),
    }
}

fn mbtiles_connect(mbtiles_file: &Path) -> Result<Connection, MBTileError> {
    Connection::open(mbtiles_file).to_mbtiles_result(format!("Can't connect to {:?}", mbtiles_file))
}

macro_rules! log_err {
    ($e:expr) => {
        match $e {
            Ok(_) => (),
            Err(e) => {
                error!("{:?}", e);
            }
        }
    }
}

fn optimize_connection(connection: &Connection) -> Result<(), MBTileError> {
    connection.execute_batch("
        PRAGMA synchronous=0;
        PRAGMA locking_mode=EXCLUSIVE;
        PRAGMA journal_mode=DELETE;
        ")
              .to_mbtiles_result("Cannot execute sqlite optimization query".to_owned())
}

fn optimize_database(connection: &Connection) -> Result<(), MBTileError> {
    info!("SQLite analyse");
    try!(connection.execute_batch("ANALYZE;").to_mbtiles_result("Can't analyze sqlite".to_owned()));
    info!("SQLite vacuum");
    try!(connection.execute_batch("VACUUM;").to_mbtiles_result("Can't vacuum sqlite".to_owned()));
    Ok(())
}

trait ToMBTilesResult<T: Sized, E: Sized> {
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

fn mbtiles_setup(connection: &Connection) -> Result<(), MBTileError> {
    connection.execute_batch("
        CREATE TABLE tiles (
                zoom_level INTEGER,
                tile_column INTEGER,
                tile_row INTEGER,
                tile_data BLOB);
        CREATE TABLE metadata
            (name TEXT, value TEXT);
        CREATE TABLE grids (zoom_level INTEGER, tile_column INTEGER,
            tile_row INTEGER, grid BLOB);
        CREATE TABLE grid_data (zoom_level INTEGER, tile_column
            INTEGER, tile_row INTEGER, key_name TEXT, key_json TEXT);
        CREATE UNIQUE INDEX name ON metadata (name);
        CREATE UNIQUE INDEX tile_index ON tiles
            (zoom_level, tile_column, tile_row);
    ")
              .to_mbtiles_result("Can't create schema".to_owned())
}

fn is_visible(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map_or(false, |s| !s.starts_with('.'))
}

fn get_extension(image_format: ImageFormat) -> String {
    match image_format {
        ImageFormat::Jpg => "jpg".to_owned(),
        ImageFormat::Pbf => "pbf".to_owned(),
        ImageFormat::Png => "png".to_owned(),
        ImageFormat::Webp => "webp".to_owned(),
    }
}

fn parse_component(component: Component,
                   parse_file: Option<ImageFormat>)
                   -> Result<u32, MBTileError> {
    if let Component::Normal(zoom_dir) = component {
        let s = try!(zoom_dir.to_str()
                             .ok_or(MBTileError {
                                 message: format!("Unvalid unicode path: {:?}", zoom_dir),
                                 inner_error: InnerError::None,
                             }));
        if let Some(image_format) = parse_file {
            let parts: Vec<&str> = s.split('.').collect();
            let filtered_extension = get_extension(image_format);
            if parts[1] == filtered_extension {
                parts[0].parse::<u32>().to_mbtiles_result("".to_owned())
            } else {
                Err(MBTileError {
                    message: format!("The filtered extention {} is different than the path's \
                             extention {}",
                                     filtered_extension,
                                     parts[1]),
                    inner_error: InnerError::None,
                })
            }
        } else {
            s.parse::<u32>().to_mbtiles_result("".to_owned())
        }
    } else {
        Err(MBTileError {
            message: format!("Can't read path component {:?}", component),
            inner_error: InnerError::None,
        })
    }
}

fn import(input: &Path,
          output: &Path,
          flag_scheme: Scheme,
          flag_image_format: Option<ImageFormat>,
          flag_grid_callback: Option<String>)
          -> Result<(), MBTileError> {
    info!("Importing disk to MBTiles");
    debug!("{:?} --> {:?}", &input, &output);
    if !input.is_dir() {
        return Err(MBTileError {
            message: "Can only import from a directory".to_owned(),
            inner_error: InnerError::None,
        });
    }
    let connection = try!(mbtiles_connect(output));
    try!(optimize_connection(&connection));
    try!(mbtiles_setup(&connection));
    let base_components_length = input.components().count();
    let dir_walker = WalkDir::new(input)
                         .follow_links(true)
                         .min_depth(1)
                         .max_depth(3)
                         .into_iter()
                         .filter_entry(is_visible);
    for entry_res in dir_walker {
        let entry = try!(entry_res.to_mbtiles_result("".to_owned()));
        let entry_path = entry.path();
        if entry_path.is_dir() {
            // ignore directories
            continue;
        }
        let end_comp: Vec<Component> = entry_path.components()
                                                 .skip(base_components_length)
                                                 .collect();
        if end_comp.len() == 3 {
            parse_component(end_comp[0], None)
                .and_then(|zoom| {
                    parse_component(end_comp[1], None).and_then(|row| {
                        parse_component(end_comp[2], flag_image_format).and_then(|col| {
                            info!("Zoom: {}, Row: {}, Col {}", zoom, row, col);
                            insert_image_sqlite(entry_path, zoom, col, row, &connection)
                        })
                    })
                })
                .unwrap_or_else(|err| error!("{}", err))
        }
        info!("{}", entry.path().display());
    }
    try!(optimize_database(&connection));
    Ok(())
}

fn insert_image_sqlite(image_path: &Path,
                       zoom: u32,
                       column: u32,
                       row: u32,
                       connection: &Connection)
                       -> Result<(), MBTileError> {
    let mut image_file = try!(File::open(image_path)
                                  .to_mbtiles_result(format!("Can't open {:?}", image_path)));
    let mut buffer = Vec::new();
    try!(image_file.read_to_end(&mut buffer)
                   .to_mbtiles_result(format!("Can't read file {:?}", image_path)));
    try!(connection.execute("insert into tiles (zoom_level,
                    tile_column, tile_row, tile_data) values
                    ($1, $2, $3, $4);",
                            &[&(zoom as i64), &(column as i64), &(row as i64), &buffer])
                   .to_mbtiles_result(format!("Can't insert {:?}", image_path)));
    Ok(())
}

fn export(input: String,
          output: Option<String>,
          flag_scheme: Scheme,
          flag_image_format: Option<ImageFormat>,
          flag_grid_callback: Option<String>) {
    let input_path = Path::new(&input);
    if input_path.is_file() {
    } else {
        error!("Can only export from a file")
    }
}

fn metadata(input: String,
            output: Option<String>,
            flag_scheme: Scheme,
            flag_image_format: Option<ImageFormat>,
            flag_grid_callback: Option<String>) {
    let input_path = Path::new(&input);
    if input_path.is_file() {
    } else {
        error!("Can only dumps from a file")
    }
}
