extern crate docopt;
extern crate regex;
extern crate rusqlite;
extern crate walkdir;
#[macro_use(info, debug, error, warn)]
extern crate log;
extern crate flate2;
extern crate serde;
extern crate serde_json;
extern crate thiserror;

use crate::mbtiles::{export, import, metadata, Command, ImageFormat, Scheme};
use docopt::Docopt;
use log::LevelFilter;
use serde::Deserialize;
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};
use std::path::Path;

#[macro_use]
mod mbtile_error;
mod mbtiles;

const USAGE: &str = "
MBTiles utils.

Usage:
    mbutiles <command> [options] <input> \
                             [<output>]
    mbutiles -h | --help
    mbutiles -v | --version

Options:
  -h --help                   Show this help message and exit.
  --verbose                   Show log info.
  -v --version                Show version.
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
    version
";

#[derive(Deserialize, Debug)]
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
        .and_then(|d| d.version(Some("mbutiles 0.1.0".to_string())).deserialize())
        .unwrap_or_else(|e| e.exit());
    TermLogger::init(
        if args.flag_verbose {
            LevelFilter::Info
        } else {
            LevelFilter::Error
        },
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();
    info!("{:?}", args);
    match args.arg_command {
        Command::Import => {
            // import tiles dir into mbtiles
            let input = args.arg_input.clone();
            let output = args
                .arg_output
                .unwrap_or_else(|| format!("{}.mbtiles", input));
            if let Err(err) = import(
                &Path::new(&args.arg_input),
                &Path::new(&output),
                args.flag_scheme,
                args.flag_image_format,
            ) {
                error!("{:?}", err);
            }
        }
        Command::Export =>
        // export mbtiles to a dir
        {
            if let Err(err) = export(
                args.arg_input,
                args.arg_output,
                args.flag_scheme,
                args.flag_image_format,
                args.flag_grid_callback,
            ) {
                error!("{:?}", err);
            }
        }
        Command::Metadata =>
        // dumps metadata
        {
            if let Err(err) = metadata(args.arg_input, args.arg_output) {
                error!("{:?}", err);
            }
        }
    }
}
