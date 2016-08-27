use rusqlite::{Connection, Row};
use std::iter::Iterator;
use walkdir::{DirEntry, WalkDir, WalkDirIterator};
use std::path::{Component, Path, PathBuf};
use std::fs::{self, File};
use std::io::prelude::*;
use mbtile_error::{MBTileError, ToMBTileResult};
use rustc_serialize::json::{self, Json};
use std::fmt;
use std::collections::BTreeMap;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use std::io::Cursor;
use regex::Regex;

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

fn insert_metadata(input: &PathBuf, connection: &Connection) -> Result<(), MBTileError> {
    if input.is_file() {
        info!("metadata.json was not found");
        return Ok(());
    }
    let mut metadata_file = try!(File::open(input.join("metadata.json"))
                                     .desc(format!("Can't open metadata.json: {:?}", input)));
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

pub fn import<P: AsRef<Path>>(input: P,
                              output: P,
                              flag_scheme: Scheme,
                              flag_image_format: ImageFormat)
                              -> Result<(), MBTileError> {
    info!("Importing disk to MBTiles");
    let input_path: PathBuf = input.as_ref().to_path_buf();
    let output_path: PathBuf = output.as_ref().to_path_buf();
    debug!("{:?} --> {:?}", &input_path, &output_path);
    if !input_path.is_dir() {
        return Err(MBTileError::new_static("Can only import from a directory"));
    }
    let connection = try!(mbtiles_connect(&output_path));
    try!(optimize_connection(&connection));
    try!(mbtiles_setup(&connection));
    try!(insert_metadata(&input_path, &connection));
    try!(walk_dir_image(&input_path, flag_scheme, flag_image_format, &connection));
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
                        parse_filename_and_insert(end_comp[2],
                                                  flag_scheme,
                                                  flag_image_format,
                                                  zoom,
                                                  image_dir,
                                                  entry_path,
                                                  connection)
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

fn parse_filename_and_insert(component: Component,
                             flag_scheme: Scheme,
                             image_format: ImageFormat,
                             zoom: u32,
                             image_dir: u32,
                             entry_path: &Path,
                             connection: &Connection)
                             -> Result<(), MBTileError> {
    let filename = try!(parse_comp(component));
    let parts: Vec<&str> = filename.split('.').collect();

    let mut radix = 10u32;
    let mut stem_part = parts[0].to_owned();
    if let Scheme::Ags = flag_scheme {
        stem_part = stem_part.replace("C", "");
        radix = 16;
    }
    let image_filename = try!(u32::from_str_radix(stem_part.as_str(), radix)
                                  .desc("Can't parse component in integer format"));
    let (col, row) = match flag_scheme {
        Scheme::Ags => (image_filename, flip_y(zoom, image_dir)),
        Scheme::Xyz => (image_dir, flip_y(zoom, image_filename)),
        _ => (image_dir, image_filename),
    };

    let filtered_extension = get_extension(image_format);
    if parts.len() == 2 && parts[1] == filtered_extension {
        info!("Zoom: {}, Col: {}, Row {}", zoom, col, row);
        insert_image_sqlite(entry_path, zoom, col, row, connection)
    } else if parts.len() == 3 && parts[1] == "grid" && parts[2] == "json" {
        insert_grid_json(entry_path, zoom, col, row, connection)
    } else {
        Err(MBTileError::new(format!("The filtered extention {} \
is different than the path's extention {}",
                                     filtered_extension,
                                     parts[1])))
    }
}

fn insert_grid_json(grid_path: &Path,
                    zoom: u32,
                    column: u32,
                    row: u32,
                    connection: &Connection)
                    -> Result<(), MBTileError> {
    let mut grid_file = try!(File::open(grid_path).desc(format!("Can't open {:?}", grid_path)));
    let mut grid_content = String::new();
    try!(grid_file.read_to_string(&mut grid_content)
                  .desc(format!("Can't read file {:?}", grid_path)));
    let re = try!(Regex::new(r"[\w\s=+-/]+\((\{(.|\n)*\})\);?"));
    grid_content = if let Some(capture) = re.captures(grid_content.as_str()) {
        try!(capture.at(1).ok_or_else(|| MBTileError::new_static("Can't parse grid"))).to_owned()
    } else {
        grid_content.clone()
    };
    let utfgrid = try!(Json::from_str(grid_content.as_str()));
    let (data_opt, utfgrid_obj) = if let Json::Object(mut utfgrid_obj) = utfgrid {
        (utfgrid_obj.remove("data"), utfgrid_obj)
    } else {
        return Err(MBTileError::new_static("grid json not an object"));
    };
    let kk = Json::Object(utfgrid_obj);
    let filtered_json_grid = try!(json::encode(&kk));
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::Default);
    try!(encoder.write_all(filtered_json_grid.as_bytes()));
    let zipped_json = try!(encoder.finish());
    try!(connection.execute("insert into grids (zoom_level, tile_column, tile_row, grid) values ($1, $2, $3, $4);",
                            &[&(zoom as i64), &(column as i64), &(row as i64), &zipped_json])
               .desc("Can't insert zipped grid in database"));
    let utfgrid_obj = try!(kk.as_object()
                             .ok_or_else(|| MBTileError::new_static("grid is not an object")));
    let aa = &utfgrid_obj.get("keys");
    if let Some(&Json::Array(ref keys_array)) = *aa {
        let filtered_keys = keys_array.iter().filter_map(|k| {
            k.as_string().and_then(|k| {
                if k == "" {
                    None
                } else {
                    Some(k)
                }
            })
        });
        // let filtered_keys = try!(gg.and_then(|keys_res| keys_res.filter(|&k| k != "")));
        for key in filtered_keys {
            if let Some(ref data) = data_opt {
                if let Json::Object(ref data_obj) = *data {
                    let key_json = &data_obj[key];
                    try!(connection.execute("insert into grid_data (zoom_level, tile_column, tile_row, key_name, key_json) values ($1, $2, $3, $4, $5);",
                    &[&(zoom as i64), &(column as i64), &(row as i64), &key, &key_json.to_string()]));
                } else {
                    println!("Can't get some data_obj {:?}", data);
                }
            } else {
                println!("Can't get some data {:?}", data_opt);
            }
        }
    } else {
        println!("Can't get some json array {:?}", aa);
    }
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

fn export_metadata(connection: &Connection,
output_path: &Path)
    -> Result<(), MBTileError> {
    let mut metadata_statement = try!(connection.prepare("select name, value from metadata;"));

    let metadata_statement_rows = try!(metadata_statement.query_map(&[], |row| {
        (row.get::<i32, String>(0), Json::String(row.get::<i32, String>(1)))
    }));
    let metadata_map: BTreeMap<_, _> = try!(metadata_statement_rows.collect());

    let json_obj = Json::Object(metadata_map);
    let json_str = json_obj.to_string();
    let metadata_path = output_path.join("metadata.json");
    let mut metadata_file = try!(File::create(metadata_path).desc("Can't create metadata file"));
    try!(metadata_file.write(json_str.as_bytes())
                      .desc("Can't write metadata file"));
    Ok(())
}

pub fn export<P: AsRef<Path>>(input: P,
                              opt_output: Option<P>,
                              flag_scheme: Scheme,
                              flag_image_format: ImageFormat,
                              flag_grid_callback: String)
                              -> Result<(), MBTileError> {
    let input_path: PathBuf = input.as_ref().to_path_buf();
    if !input_path.is_file() {
        return Err(MBTileError::new(format!("Can't export from a file at path {:?}", input_path)));
    }
    let output: PathBuf = try!(opt_output
        .map(|p| p.as_ref().to_path_buf())
        .or_else(|| {
           input_path.file_stem()
                     .and_then(|stem| Some(PathBuf::from(stem)))
                     //.map(|stem_str| stem_str.to_owned())
       })
       .ok_or(MBTileError::new_static("Cannot identify an output directory")));
    debug!("Exporting MBTiles to disk");
    debug!("{:?} --> {:?}", &input_path, &output);
    let output_path = Path::new(&output);
    if output_path.exists() {
        return Err(MBTileError::new_static("Directory already exists"));
    }
    try!(fs::create_dir_all(&output_path).desc("Can't create the output directory"));
    let connection = try!(mbtiles_connect(&input_path));
    try!(export_metadata(&connection, &output_path));
    // TODO show pregression:
    // let zoom_level_count = get_count(&connection, "tiles");

    let mut tiles_statement =
        try!(connection.prepare("select zoom_level, tile_column, tile_row, tile_data from tiles;"));
    let mut tiles_rows = try!(tiles_statement.query(&[]));
    while let Some(tile_res) = tiles_rows.next() {
        let tile = try!(tile_res);
        try!(export_tile(&tile, output_path, flag_scheme, flag_image_format));
    }
    try!(export_grid(&connection, &output_path, flag_scheme, flag_grid_callback));
    Ok(())
}

fn export_tile(tile: &Row,
               output_path: &Path,
               flag_scheme: Scheme,
               flag_image_format: ImageFormat)
               -> Result<(), MBTileError> {
    let (z, x, mut y): (u32, u32, u32) = (tile.get::<i32, i32>(0) as u32,
                                          tile.get::<i32, i32>(1) as u32,
                                          tile.get::<i32, i32>(2) as u32);
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
    try!(tile_file.write_all(&tile.get::<i32, Vec<u8>>(3)));
    Ok(())
}

// fn get_count(connection: &Connection, table: &str) -> Result<i32, MBTileError> {
//     connection.query_row_safe("select count(zoom_level) from (?);",
//                               &[&table],
//                               |row| row.get::<i32, i32>(0))
//               .desc(format!("Can't get {} zoom level", table))
// }

fn export_grid(connection: &Connection,
               output_path: &Path,
               flag_scheme: Scheme,
               flag_grid_callback: String)
               -> Result<(), MBTileError> {
    // TODO show pregression:
    // let grids_zoom_level_count = get_count(&connection, "grids");
    let mut grids_statement =
        try!(connection.prepare("select zoom_level, tile_column, tile_row, grid from grids;"));
    let mut grids_rows = try!(grids_statement.query(&[]));
    while let Some(grid_row) = grids_rows.next() {
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
        let grid_zip = grid.get::<i32, Vec<u8>>(3);
        let grid_cursor = Cursor::new(grid_zip);
        let mut decoder = ZlibDecoder::new(grid_cursor);
        let mut unzipped_grid = String::new();
        try!(decoder.read_to_string(&mut unzipped_grid));
        let grid_json = try!(Json::from_str(unzipped_grid.as_str())
                                 .desc(format!("Grid json: {}", unzipped_grid)));

        let mut grid_data_statement = try!(connection.prepare("select key_name, key_json FROM
            grid_data WHERE
            zoom_level = (?) and
            tile_column = (?) and
            tile_row = (?);"));

        let grid_data_rows = try!(grid_data_statement.query_map(&[&zoom_level, &tile_column, &y],
                                                                |row| {
                                                                    let json = row.get::<i32, String>(1);
                                                                    Json::from_str(json.as_str())
                    .map(|res| (row.get::<i32, String>(0), res))
                    .desc(format!("Can't parse json: {}", json))
                                                                }));
        let data: BTreeMap<_, _> = try!(grid_data_rows.map(|res| {
                                                          res.desc("")
                                                             .and_then(|rr| rr)
                                                      })
                                                      .collect());

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
            callback => format!("{}({});", callback, grid_json),
        };
        try!(grid_file.write_all(dump.as_bytes()));
    }
    Ok(())
}

pub fn metadata<P: AsRef<Path>>(input: P,
                opt_output: Option<P>) -> Result<(), MBTileError> {
    let input_path: PathBuf = input.as_ref().to_path_buf();
    if !input_path.is_file() {
        error!("Can only export from a file")
    }
    let output: PathBuf = try!(opt_output
        .map(|p| p.as_ref().to_path_buf())
        .or_else(|| {
           input_path.file_stem()
                     .and_then(|stem| Some(PathBuf::from(stem)))
                     //.map(|stem_str| stem_str.to_owned())
       })
       .ok_or(MBTileError::new_static("Cannot identify an output directory")));
    let output_path = output.join("metadata.json");
    let connection = try!(mbtiles_connect(&input_path));
    try!(export_metadata(&connection, &output_path));
    Ok(())
}
