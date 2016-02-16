extern crate rustc_serialize;
extern crate docopt;
extern crate rusqlite;

use docopt::Docopt;
use std::path::Path;
use rusqlite::Connection;

const USAGE: &'static str = "
MBTiles utils.

Usage:
    mbutiles <command> [options] <input> [<output>]
    mbutiles -h | --help
    mbutiles --version

Options:
  -h --help                   Show this help message and exit.
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

#[derive(RustcDecodable, Debug)]
enum ImageFormat {
    Png,
    Jpg,
    Webp,
    Pbf,
}

#[derive(RustcDecodable, Debug)]
struct Args {
    arg_command: Command,
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
    println!("{:?}", args);
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
    connection.execute_batch("
    PRAGMA synchronous=0;
    PRAGMA locking_mode=EXCLUSIVE;
    PRAGMA journal_mode=DELETE;
    ");
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

fn import(input: &Path, output: &Path,
    flag_scheme: Scheme,
    flag_image_format: Option<ImageFormat>,
    flag_grid_callback: Option<String>) {
    let input_path = Path::new(&input);
    if input_path.is_dir() {
        let connection = mbtiles_connect(output);
        optimize_connection(&connection);
        mbtiles_setup(&connection);
        if let Ok(dir_entries) = input_path.read_dir() {
            for zoom_entry in dir_entries {
                if let Ok(zoom_dir) = zoom_entry {
                    println!("{:?}", zoom_dir.path().as_path());
                } else {
                    continue;
                }
            }
        }
    } else {
        panic!("Can only import from a directory")
    }
}

fn export(input: String, output: Option<String>,
    flag_scheme: Scheme,
    flag_image_format: Option<ImageFormat>,
    flag_grid_callback: Option<String>) {
    let input_path = Path::new(&input);
    if input_path.is_file() {
    } else {
        panic!("Can only export from a file")
    }
}

fn metadata(input: String, output: Option<String>,
    flag_scheme: Scheme,
    flag_image_format: Option<ImageFormat>,
    flag_grid_callback: Option<String>) {
    let input_path = Path::new(&input);
    if input_path.is_file() {
    } else {
        panic!("Can only dumps from a file")
    }
}
