use rusqlite::Connection;
use std::iter::Iterator;
use walkdir::{DirEntry, WalkDir, WalkDirIterator};
use std::path::{Component, Path};
use std::fs::File;
use std::io::prelude::*;
use mbtile_error::{InnerError, MBTileError};
use rustc_serialize::json::Json;
use std::convert;

#[derive(RustcDecodable, Debug)]
pub enum Command {
    Import,
    Export,
    Metadata,
}

#[derive(RustcDecodable, Debug)]
pub enum Scheme {
    Xyz,
    Tms,
    Wms,
}

#[derive(RustcDecodable, Debug, Clone, Copy)]
pub enum ImageFormat {
    Png,
    Jpg,
    Webp,
    Pbf,
}

fn mbtiles_connect(mbtiles_file: &Path) -> Result<Connection, MBTileError> {
    Ok(try_desc!(Connection::open(mbtiles_file), format!("Can't connect to {:?}", mbtiles_file)))
}

fn optimize_connection(connection: &Connection) -> Result<(), MBTileError> {
    Ok(try_desc!(connection.execute_batch("
        PRAGMA synchronous=0;
        PRAGMA locking_mode=EXCLUSIVE;
        PRAGMA journal_mode=DELETE;
        "), "Cannot execute sqlite optimization query"))
}

fn optimize_database(connection: &Connection) -> Result<(), MBTileError> {
    info!("SQLite analyse");
    try_desc!(connection.execute_batch("ANALYZE;"), "Can't analyze sqlite");
    info!("SQLite vacuum");
    try_desc!(connection.execute_batch("VACUUM;"), "Can't vacuum sqlite");
    Ok(())
}

fn mbtiles_setup(connection: &Connection) -> Result<(), MBTileError> {
    Ok(try_desc!(connection.execute_batch("
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
    "), "Can't create schema"))
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
                Ok(try_desc!(parts[0].parse::<u32>(), "Can't parse component in integer format"))
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
            Ok(try_desc!(s.parse::<u32>(), "Can't parse component in integer format"))
        }
    } else {
        Err(MBTileError {
            message: format!("Can't read path component {:?}", component),
            inner_error: InnerError::None,
        })
    }
}

fn insert_metadata(input: &Path, connection: &Connection) -> Result<(), MBTileError> {
    if input.is_file() {
        info!("metadata.json was not found");
        return Ok(());
    }
    let mut metadata_file = try_desc!(File::open(input.join("metadata.json")),
        "Can't open metadata.json");
    let mut buffer = String::new();
    try_desc!(metadata_file.read_to_string(&mut buffer),
                      "metadata.json wasn't readable");
    // TODO: use try! add error type
    if let Ok(data) = Json::from_str(buffer.as_str()) {
        if data.is_object() {
            let obj = data.as_object().unwrap();
            for (key, value) in obj.iter() {
                try_desc!(connection.execute("insert into metadata (name, value) values ($1, $2)",
                                        &[key, &value.as_string().unwrap()]),
                               "Can't insert medata in database");
            }
        }
    }
    info!("metadata.json was restored");
    Ok(())
}

pub fn import(input: &Path,
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
    insert_metadata(&input, &connection);
    let base_components_length = input.components().count();
    let dir_walker = WalkDir::new(input)
                         .follow_links(true)
                         .min_depth(1)
                         .max_depth(3)
                         .into_iter()
                         .filter_entry(is_visible);
    for entry_res in dir_walker {
        let entry = try_desc!(entry_res, "invalid entry");
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
    let mut image_file = try_desc!(File::open(image_path),
                                  format!("Can't open {:?}", image_path));
    let mut buffer = Vec::new();
    try_desc!(image_file.read_to_end(&mut buffer), format!("Can't read file {:?}", image_path));
    try_desc!(connection.execute("insert into tiles (zoom_level,
                    tile_column, tile_row, tile_data) values
                    ($1, $2, $3, $4);",
                            &[&(zoom as i64), &(column as i64), &(row as i64), &buffer]),
                   format!("Can't insert {:?}", image_path));
    Ok(())
}

pub fn export(input: String,
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

pub fn metadata(input: String,
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
