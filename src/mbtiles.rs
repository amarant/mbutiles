use rusqlite::{Connection, Row};
use std::iter::Iterator;
use walkdir::{DirEntry, WalkDir, WalkDirIterator};
use std::path::{Component, Path};
use std::fs::{self, File};
use std::io::prelude::*;
use mbtile_error::{MBTileError, ToMBTileResult};
use rustc_serialize::json::{self, Json};
use std::fmt;
use std::collections::BTreeMap;
use zip::ZipArchive;
use std::io::Cursor;
use rusqlite::types::ToSql;
use rusqlite::Statement;

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
    Ok(try!(Connection::open(mbtiles_file).desc(format!("Can't connect to {:?}", mbtiles_file))))
}

fn optimize_connection(connection: &Connection) -> Result<(), MBTileError> {
    Ok(try!(connection.execute_batch("
        PRAGMA synchronous=0;
        PRAGMA locking_mode=EXCLUSIVE;
        PRAGMA journal_mode=DELETE;
        ")
                      .desc("Cannot execute sqlite optimization query")))
}

fn optimize_database(connection: &Connection) -> Result<(), MBTileError> {
    info!("SQLite analyse");
    try!(connection.execute_batch("ANALYZE;").desc("Can't analyze sqlite"));
    info!("SQLite vacuum");
    try!(connection.execute_batch("VACUUM;").desc("Can't vacuum sqlite"));
    Ok(())
}

fn mbtiles_setup(connection: &Connection) -> Result<(), MBTileError> {
    Ok(try!(connection.execute_batch("
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
                      .desc("Can't create schema")))
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
    let mut metadata_file = try!(File::open(input.join("metadata.json"))
                                     .desc("Can't open metadata.json"));
    let mut buffer = String::new();
    try!(metadata_file.read_to_string(&mut buffer)
                      .desc("metadata.json wasn't readable"));
    // TODO: use try! add error type
    let data = try!(Json::from_str(buffer.as_str()));
    if data.is_object() {
        let obj = try!(data.as_object()
                           .ok_or_else(|| MBTileError::new_static("metadata is not an object")));
        for (key, value) in obj.iter() {
            let value_str = try!(value.as_string().ok_or_else(|| MBTileError::new_static("metadata object has a non string value")));
            try!(connection.execute("insert into metadata (name, value) values ($1, $2)",
                                    &[key, &value_str])
                           .desc("Can't insert medata in database"));
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
        let entry = try!(entry_res.desc("invalid entry"));
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
              .ok_or_else(|| MBTileError::new(format!("Unvalid unicode path: {:?}", os_str)))
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
    Ok(try!(zoom_string.parse::<u32>()
                       .desc("Can't parse component in integer format")))
}

fn parse_image_dir(component: Component, flag_scheme: Scheme) -> Result<u32, MBTileError> {
    let mut radix = 10u32;
    let mut x_string = try!(parse_comp(component));
    if let Scheme::Ags = flag_scheme {
        x_string = x_string.replace("R", "");
        radix = 16;
    }
    Ok(try!(u32::from_str_radix(x_string.as_str(), radix)
                .desc("Can't parse component in integer format")))
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
            return Err(MBTileError::new(format!("The filtered extention {} \
is different than the path's extention {}",
                                                filtered_extension,
                                                parts[1])));
        }
        if let Scheme::Ags = flag_scheme {
            x_part = x_string.replace("C", "");
            radix = 16;
        }
    }
    x_string = x_part;
    Ok(try!(u32::from_str_radix(x_string.as_str(), radix)
                .desc("Can't parse component in integer format")))
}

fn insert_grid_json(connection: &Connection, grid_path: &Path) -> Result<(), MBTileError> {
    let mut grid_file = try!(File::open(grid_path).desc(format!("Can't open {:?}", grid_path)));
    let mut buffer = Vec::new();
    try!(grid_file.read_to_end(&mut buffer)
                  .desc(format!("Can't read file {:?}", grid_path)));
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
    let mut image_file = try!(File::open(image_path).desc(format!("Can't open {:?}", image_path)));
    let mut buffer = Vec::new();
    try!(image_file.read_to_end(&mut buffer)
                   .desc(format!("Can't read file {:?}", image_path)));
    try!(connection.execute("insert into tiles (zoom_level,
                    tile_column, tile_row, tile_data) values
                    ($1, $2, $3, $4);",
                            &[&(zoom as i64), &(column as i64), &(row as i64), &buffer])
                   .desc(format!("Can't insert {:?}", image_path)));
    Ok(())
}

fn query_json(statement: &mut Statement,
              params: &[&ToSql])
              -> Result<BTreeMap<String, Json>, MBTileError> {
    let rows = try!(statement.query_map(&params, |row| {
        (row.get::<String>(0), Json::String(row.get::<String>(1)))
    }));

    let data: BTreeMap<_, _> = try!(rows.collect());
    Ok(data)
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
    let output = try!(opt_output.or_else(|| {
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
    try!(fs::create_dir_all(&output_path).desc("Can't create the output directory"));
    let connection = try!(mbtiles_connect(&input_path));
    let mut metadata_statement = try!(connection.prepare("select name, value from metadata;"));
    let metadata_map = try!(query_json(&mut metadata_statement, &[]));
    let json_obj = Json::Object(metadata_map);
    let json_str = json_obj.to_string();
    let metadata_path = output_path.join("metadata.json");
    let mut metadata_file = try!(File::create(metadata_path).desc("Can't create metadata file"));
    try!(metadata_file.write(json_str.as_bytes())
                      .desc("Can't write metadata file"));
    // TODO show pregression:
    // let zoom_level_count = get_count(&connection, "tiles");

    let mut tiles_statement =
        try!(connection.prepare("select zoom_level, tile_column, tile_row, tile_data from tiles;"));
    let tiles_rows = try!(tiles_statement.query(&[]));
    for tile_res in tiles_rows {
        let tile = try!(tile_res);
        try!(export_tile(&tile, output_path, flag_scheme, flag_image_format));
    }
    try!(export_grid(&connection, flag_scheme, &output_path, flag_grid_callback));
    Ok(())
}

fn export_tile(tile: &Row,
               output_path: &Path,
               flag_scheme: Scheme,
               flag_image_format: ImageFormat)
               -> Result<(), MBTileError> {
    let (z, x, mut y): (u32, u32, u32) = (tile.get::<i32>(0) as u32,
                                          tile.get::<i32>(1) as u32,
                                          tile.get::<i32>(2) as u32);
    let tile_dir = match flag_scheme {
        Scheme::Xyz => {
            y = flip_y(z, y as u32);
            output_path.join(z.to_string()).join(x.to_string())
        }
        Scheme::Wms => {
            output_path.join(format!("{:02}", z))
                       .join(format!("{:02}", z))
                       .join(format!("{:03}", x as i32 / 1000000))
                       .join(format!("{:03}", (x as i32 / 1000) % 1000))
                       .join(format!("{:02}", x as i32 % 1000))
                       .join(format!("{:02}", y as i32 / 1000000))
                       .join(format!("{:02}", (y as i32 / 1000) % 1000))
        }
        _ => output_path.join(z.to_string()).join(x.to_string()),
    };
    try!(fs::create_dir_all(&tile_dir)
             .desc(format!("Can't create the tile directory: {:?}", tile_dir)));
    let tile_path = match flag_scheme {
        Scheme::Wms => {
            tile_dir.join(format!("{:03}.{}",
                                  y as i32 % 1000,
                                  get_extension(flag_image_format)))
        }
        _ => tile_dir.join(format!("{}.{}", y, get_extension(flag_image_format))),
    };
    let mut tile_file = try!(File::create(tile_path));
    try!(tile_file.write_all(&tile.get::<Vec<u8>>(3)));
    Ok(())
}

fn get_count(connection: &Connection, table: &str) -> Result<i32, MBTileError> {
    connection.query_row_safe("select count(zoom_level) from (?);",
                              &[&table],
                              |row| row.get::<i32>(0))
              .desc(format!("Can't get {} zoom level", table))
}

fn export_grid(connection: &Connection,
               flag_scheme: Scheme,
               output_path: &Path,
               flag_grid_callback: String)
               -> Result<(), MBTileError> {
    // TODO show pregression:
    // let grids_zoom_level_count = get_count(&connection, "grids");
    let mut grids_statement =
        try!(connection.prepare("select zoom_level, tile_column, tile_row, grid from grids;"));
    let grids_rows = try!(grids_statement.query(&[]));
    for grid_row in grids_rows {
        let grid = try!(grid_row);
        let (zoom_level, tile_column, mut y): (i32, i32, i32) = (grid.get(0),
                                                                 grid.get(1),
                                                                 grid.get(2));
        if let Scheme::Xyz = flag_scheme {
            y = flip_y(zoom_level as u32, y as u32) as i32;
        }
        let grid_dir = output_path.join(zoom_level.to_string()).join(tile_column.to_string());
        try!(fs::create_dir_all(&grid_dir)
                 .desc(format!("Can't create the directory: {:?}", grid_dir)));
        let grid_zip = grid.get::<Vec<u8>>(3);
        let grid_cursor = Cursor::new(grid_zip);
        let mut zip_archive = try!(ZipArchive::new(grid_cursor));
        let mut zip_file = try!(zip_archive.by_index(0));
        let mut unzipped_grid = String::new();
        try!(zip_file.read_to_string(&mut unzipped_grid));
        let grid_json = try!(Json::from_str(unzipped_grid.as_str()));

        let mut grid_data_statement = try!(connection.prepare("select key_name, key_json FROM
            grid_data WHERE
            zoom_level = (?) and
            tile_column = (?) and
            tile_row = (?);"));
        let data = try!(query_json(&mut grid_data_statement, &[&zoom_level, &tile_column, &y]));

        let grid_object = if let Json::Object(mut grid_object) = grid_json {
            grid_object.insert("data".to_owned(), Json::Object(data));
            grid_object
        } else {
            return Err(MBTileError::new_static("grid is not an object"));
        };
        let grid_file_path = grid_dir.join(format!("{}.grid.json", y));
        let mut grid_file = try!(File::create(grid_file_path));
        let grid_json = try!(json::encode(&grid_object));
        let dump = match flag_grid_callback.as_str() {
            "" | "false" | "null" => grid_json,
            callback => format!("{}({})", callback, grid_json),
        };
        try!(grid_file.write_all(dump.as_bytes()));
    }
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
