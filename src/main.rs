#![feature(plugin)]
#![plugin(clippy)]

extern crate rustc_serialize;
extern crate docopt;
extern crate rusqlite;
extern crate walkdir;
extern crate regex;
#[macro_use(log, info, error)]
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
  --scheme=<scheme>           Tiling scheme of the tiles. Default is \"xyz\" (z/x/y), other options are \"tms\" which is also z/x/y but uses a flipped y coordinate, and \"wms\" which replicates the MapServer WMS TileCache directory structure \"z/000/000/x/000/000/y.png\". [default: xyz]
  --image-format=<format>     The format of the image tiles, either png, jpg, webp or pbf.
  --grid-callback=<callback>  Option to control JSONP callback for UTFGrid tiles. If grids are not used as JSONP, you can remove callbacks specifying --grid_callback=\"\".

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

fn main() {
    let args: Args = Docopt::new(USAGE)
                         .and_then(|d| d.decode())
                         .unwrap_or_else(|e| e.exit());
    stdio_logger::init(if args.flag_verbose {LogLevel::Info} else {LogLevel::Error})
        .expect("Could not initialize logging");
    info!("{:?}", args);
    match args.arg_command {
        Command::Import => {
            // import tiles dir into mbtiles
            let input = args.arg_input.clone();
            let output = args.arg_output.unwrap_or_else(
                || format!("{}.mbtiles", input));
            import(&Path::new(&args.arg_input), &Path::new(&output),
                args.flag_scheme, args.flag_image_format, args.flag_grid_callback)
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

fn mbtiles_connect(mbtiles_file: &Path) -> Connection {
    Connection::open(mbtiles_file).unwrap()
}

fn optimize_connection(connection: &Connection) {
    if let Err(err) = connection.execute_batch("
        PRAGMA synchronous=0;
        PRAGMA locking_mode=EXCLUSIVE;
        PRAGMA journal_mode=DELETE;
        ") {
        error!("Cannot execute sqlite optimization query {:?}", err);
    }
}

struct Tile {
    zoom_level: Option<i32>,
    tile_column: Option<i32>,
    tile_row: Option<i32>,
    tile_data: Option<Vec<u8>>,
}

fn mbtiles_setup(connection: &Connection) {
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
    ");
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with("."))
         .unwrap_or(false)
}

fn get_extension(image_format: ImageFormat) -> String {
    match image_format {
        ImageFormat::Jpg => "jpg".to_owned(),
        ImageFormat::Pbf => "pbf".to_owned(),
        ImageFormat::Png => "png".to_owned(),
        ImageFormat::Webp => "webp".to_owned(),
    }
}

fn parse_component(component: Component, parse_file: Option<ImageFormat>) -> Option<u32> {
    if let Component::Normal(zoom_dir) = component {
        zoom_dir.to_str()
            .ok_or("no component".to_owned())
            .and_then(|s| {
                if let Some(image_format) = parse_file {
                    let parts: Vec<&str> = s.split('.').collect();
                    let filtered_extension = get_extension(image_format);
                    if parts[1] == filtered_extension {
                        parts[0].parse::<u32>().map_err(|err| err.to_string())
                    } else {
                        Err(format!("The filtered extention {} is different \
than the path\'s extention {}", filtered_extension, parts[1]).to_owned())
                    }
                } else {
                    s.parse::<u32>().map_err(|err| err.to_string())
                }
            })
            .map_err(|err| error!("{:?}", err))
            .ok()
    } else {
        error!("Can't read path component {:?}", component);
        None
    }
}

fn import(input: &Path,
          output: &Path,
          flag_scheme: Scheme,
          flag_image_format: Option<ImageFormat>,
          flag_grid_callback: Option<String>) {
    let input_path = Path::new(&input);
    if input_path.is_dir() {
        let connection = mbtiles_connect(output);
        optimize_connection(&connection);
        mbtiles_setup(&connection);
        let base_components_length = input_path.components().count();
        let dir_walker = WalkDir::new(input_path)
            .follow_links(true)
            .min_depth(1)
            .max_depth(3)
            .into_iter()
            .filter_entry(|e| !is_hidden(e));
        for entry in dir_walker {
            if let Ok(entry) = entry {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    let end_comp : Vec<Component> = entry_path
                        .components()
                        .skip(base_components_length)
                        .collect();
                    if end_comp.len() == 3 {
                        if let Some(zoom) = parse_component(end_comp[0], None) {
                            if let Some(row) = parse_component(end_comp[1], None) {
                                if let Some(col) = parse_component(end_comp[2], flag_image_format) {
                                    info!("Zoom: {}, Row: {}, Col {}", zoom, row, col);
                                    insert_image_sqlite(entry_path, zoom, col, row, &connection);
                                }
                            }
                        }
                    }
                }
                info!("{}", entry.path().display());

            }
        }
    } else {
        error!("Can only import from a directory")
    }
}

fn insert_image_sqlite(image_path: &Path, zoom: u32, column: u32, row: u32, connection: &Connection) {
    match File::open(image_path) {
        Ok(mut image_file) =>  {
            let mut buffer = Vec::new();
            if image_file.read_to_end(&mut buffer).is_ok() {
                if let Err(err) = connection.execute("insert into tiles (zoom_level,
                            tile_column, tile_row, tile_data) values
                            ($1, $2, $3, $4);",
                            &[&(zoom as i64), &(column as i64), &(row as i64), &buffer]) {
                                error!("Can't insert {:?}, {}", image_path, err);
                }
            } else {
                error!("Can't read file {:?}", image_path);
            }
        },
        Err(err) => {
            error!("Can't open {:?} =>  {:?}", image_path, err);
        }
    }
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
