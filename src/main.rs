#![feature(plugin)]
#![plugin(clippy)]
#![plugin(regex_macros)]
#![feature(trace_macros, log_syntax)]
// #[warn(unused_variable)]

extern crate rustc_serialize;
extern crate docopt;
extern crate rusqlite;
extern crate walkdir;
extern crate regex;
#[macro_use(log, info, debug, error, warn)]
extern crate log;
extern crate stdio_logger;

use docopt::Docopt;
use std::path::Path;
use log::LogLevel;
use mbtiles::{Command, ImageFormat, Scheme, export, import, metadata};

#[macro_use]
mod mbtile_error;
mod mbtiles;

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
  --image-format=<format>     The format of the image tiles, either png, jpg, webp or pbf.\
 [default: png]
  --grid-callback=<callback>  Option to control JSONP callback for UTFGrid tiles.\
 If grids are not used as JSONP, you can remove callbacks specifying --grid_callback=\"\".\
 [default: grid]

 Commands:
    import
    export
    metadata
";

#[derive(RustcDecodable, Debug)]
struct Args {
    arg_command: Command,
    flag_verbose: bool,
    flag_scheme: Scheme,
    flag_image_format: ImageFormat,
    flag_grid_callback: String,
    arg_input: String,
    arg_output: Option<String>,
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
