use rusqlite::Connection;
use std::iter::Iterator;
use walkdir::{DirEntry, WalkDir, WalkDirIterator};
use std::path::{Component, Path};
use std::fs::{self, File};
use std::io::prelude::*;
use mbtile_error::{InnerError, MBTileError};
use rustc_serialize::json::Json;
use std::convert;
use std::fmt;
use std::collections::BTreeMap;

#[derive(RustcDecodable, Debug)]
pub enum Command {
    Import,
    Export,
    Metadata,
}

#[derive(RustcDecodable, Debug, Clone, Copy)]
pub enum Scheme {
    Xyz,
    Tms,
    Wms,
    Ags,
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let res = match *self {
            Scheme::Xyz => "xyz",
            Scheme::Tms => "tms",
            Scheme::Wms => "wms",
            Scheme::Ags => "ags",
        };
        write!(f, "{}", res)
    }
}

#[derive(RustcDecodable, Debug, Clone, Copy)]
pub enum ImageFormat {
    Png,
    Jpg,
    Webp,
    Pbf,
}

fn mbtiles_connect(mbtiles_file: &Path) -> Result<Connection, MBTileError> {
    Ok(try_desc!(Connection::open(mbtiles_file),
                 format!("Can't connect to {:?}", mbtiles_file)))
}

fn optimize_connection(connection: &Connection) -> Result<(), MBTileError> {
    Ok(try_desc!(connection.execute_batch("
        PRAGMA synchronous=0;
        PRAGMA locking_mode=EXCLUSIVE;
        PRAGMA journal_mode=DELETE;
        "),
                 "Cannot execute sqlite optimization query"))
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
    "),
                 "Can't create schema"))
}

fn is_visible(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map_or(false, |s| !s.starts_with('.'))
}

fn get_extension(image_format: ImageFormat) -> &'static str {
    match image_format {
        ImageFormat::Jpg => "jpg",
        ImageFormat::Pbf => "pbf",
        ImageFormat::Png => "png",
        ImageFormat::Webp => "webp",
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
              flag_image_format: ImageFormat,
              flag_grid_callback: String)
              -> Result<(), MBTileError> {
    info!("Importing disk to MBTiles");
    debug!("{:?} --> {:?}", &input, &output);
    if !input.is_dir() {
        return Err(MBTileError::new_static("Can only import from a directory"));
    }
    let connection = try!(mbtiles_connect(output));
    try!(optimize_connection(&connection));
    try!(mbtiles_setup(&connection));
    try!(insert_metadata(&input, &connection));
    try!(walk_dir_image(&input,
                        flag_scheme,
                        flag_image_format,
                        flag_grid_callback,
                        &connection));
    debug!("tiles (and grids) inserted.");
    try!(optimize_database(&connection));
    Ok(())
}

fn flip_y(zoom: u32, y: u32) -> u32 {
    2u32.pow(zoom) - 1 - y
}

fn walk_dir_image(input: &Path,
                  flag_scheme: Scheme,
                  flag_image_format: ImageFormat,
                  flag_grid_callback: String,
                  connection: &Connection)
                  -> Result<(), MBTileError> {
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
            parse_zoom_dir(end_comp[0], flag_scheme)
                .and_then(|zoom| {
                    parse_image_dir(end_comp[1], flag_scheme).and_then(|image_dir| {
                        parse_image_filename(end_comp[2], flag_scheme, flag_image_format)
                            .and_then(|image_filename| {
                                let (col, row) = match flag_scheme {
                                    Scheme::Ags => (image_filename, flip_y(zoom, image_dir)),
                                    Scheme::Xyz => (image_dir, flip_y(zoom, image_filename)),
                                    _ => (image_dir, image_filename),
                                };
                                info!("Zoom: {}, Col: {}, Row {}", zoom, col, row);
                                insert_image_sqlite(entry_path, zoom, col, row, &connection)
                            })
                    })
                })
                .unwrap_or_else(|err| error!("{}", err))
        }
        info!("{}", entry.path().display());
    }
    Ok(())
}

fn parse_comp(component: Component) -> Result<String, MBTileError> {
    if let Component::Normal(os_str) = component {
        os_str.to_str()
              .ok_or(MBTileError::new(
                  format!("Unvalid unicode path: {:?}", os_str)))
              .map(|s| s.to_owned())
    } else {
        Err(MBTileError::new(format!("Can't read path component {:?}", component)))
    }
}

fn parse_zoom_dir(component: Component, flag_scheme: Scheme) -> Result<u32, MBTileError> {
    let mut zoom_string = try!(parse_comp(component));
    if let Scheme::Ags = flag_scheme {
        if !zoom_string.contains('L') {
            warn!("You appear to be using an ags scheme on an non-arcgis Server cache.");
        }
        zoom_string = zoom_string.replace("L", "");
    }
    Ok(try_desc!(zoom_string.parse::<u32>(),
                 "Can't parse component in integer format"))
}

fn parse_image_dir(component: Component, flag_scheme: Scheme) -> Result<u32, MBTileError> {
    let mut radix = 10u32;
    let mut x_string = try!(parse_comp(component));
    if let Scheme::Ags = flag_scheme {
        x_string = x_string.replace("R", "");
        radix = 16;
    }
    Ok(try_desc!(u32::from_str_radix(x_string.as_str(), radix),
                 "Can't parse component in integer format"))
}

fn parse_image_filename(component: Component,
                        flag_scheme: Scheme,
                        image_format: ImageFormat)
                        -> Result<u32, MBTileError> {
    let mut radix = 10u32;
    let mut x_string = try!(parse_comp(component));
    let mut x_part: String; //escape E0506 for s
    {
        let parts: Vec<&str> = x_string.split('.').collect();
        let filtered_extension = get_extension(image_format);
        if parts[1] == filtered_extension {
            x_part = parts[0].to_owned();
        } else {
            return Err(MBTileError::new(format!("The filtered extention {} is different than the path's \
                         extention {}",
                                                filtered_extension,
                                                parts[1])));
        }
        if let Scheme::Ags = flag_scheme {
            x_part = x_string.replace("C", "");
            radix = 16;
        }
    }
    x_string = x_part;
    Ok(try_desc!(u32::from_str_radix(x_string.as_str(), radix),
                 "Can't parse component in integer format"))
}

fn insert_grid_json(connection: &Connection, grid_path: &Path) -> Result<(), MBTileError> {
    let mut grid_file = try_desc!(File::open(grid_path), format!("Can't open {:?}", grid_path));
    let mut buffer = Vec::new();
    try_desc!(grid_file.read_to_end(&mut buffer),
              format!("Can't read file {:?}", grid_path));
    // let re = regex!(r"[\w\s=+-\/]+\(({(.|\n)*})\);?");
    // for capture in re.captures(buffer) {
    // buffer = capture;
    // }
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
    try_desc!(image_file.read_to_end(&mut buffer),
              format!("Can't read file {:?}", image_path));
    try_desc!(connection.execute("insert into tiles (zoom_level,
                    tile_column, tile_row, tile_data) values
                    ($1, $2, $3, $4);",
                                 &[&(zoom as i64), &(column as i64), &(row as i64), &buffer]),
              format!("Can't insert {:?}", image_path));
    Ok(())
}

pub fn export(input: String,
              opt_output: Option<String>,
              flag_scheme: Scheme,
              flag_image_format: ImageFormat,
              flag_grid_callback: String)
              -> Result<(), MBTileError> {
    let input_path = Path::new(&input);
    if !input_path.is_file() {
        error!("Can only export from a file")
    }
    let output =
        try!(opt_output.or_else(|| {
                           input_path.file_stem()
                                     .and_then(|stem| stem.to_str())
                                     .map(|stem_str| stem_str.to_owned())
                       })
                       .ok_or(MBTileError::new_static("Cannot identify an output directory")));
    debug!("Exporting MBTiles to disk");
    debug!("{:?} --> {:?}", &input, &output);
    let output_path = Path::new(&output);
    if output_path.exists() {
        return Err(MBTileError::new_static("Directory already exists"));
    }
    try_desc!(fs::create_dir_all(&output_path),
              "Can't create the output directory");
    let connection = try!(mbtiles_connect(&input_path));
    let mut metadata_statement = try!(connection.prepare("select name, value from metadata;"));
    let metadata_rows = try!(metadata_statement.query(&[]));
    let mut metadata_map: BTreeMap<String, Json> = BTreeMap::new();
    for res_row in metadata_rows {
        let row = try!(res_row);
        metadata_map.insert(row.get(0), Json::from_str(&row.get::<String>(1)).unwrap());
    }
    let json_obj = Json::Object(metadata_map);
    let json_str = json_obj.to_string();
    let metadata_path = output_path.join("metadata.json");
    let mut metadata_file = try_desc!(File::create(metadata_path), "Can't create metadata file");
    try_desc!(metadata_file.write(json_str.as_bytes()), "Can't write metadata file");
    Ok(())
}

pub fn metadata(input: String,
                output: Option<String>,
                flag_scheme: Scheme,
                flag_image_format: ImageFormat,
                flag_grid_callback: String) {
    let input_path = Path::new(&input);
    if input_path.is_file() {
    } else {
        error!("Can only dumps from a file")
    }
}
