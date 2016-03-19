# MBUtiles

MBUtiles is an utility in Rust, to generate MBTiles from tiles directories and extract tiles from MBTiles file.


## Installation

Install [Rust](https://www.rust-lang.org/)

Install MBUtiles with Cargo

## Usage

    >mbutiles -h
    MBTiles utils.

    Usage:
        mbutiles <command> [options] <input> [<output>]
        mbutiles -h | --help
        mbutiles --version

    Options:
      -h --help                   Show this help message and exit.
      --verbose                   Show log info.
      --version                   Show version.
      --scheme=<scheme>           Tiling scheme of the tiles. Default is "xyz" (z/x/y),other options are "tms" which is also z/x/y but uses a flipped y coordinate,and "wms" which replicates the MapServer WMS TileCache directory structure"z/000/000/x/000/000/y.png". [default: xyz]
      --image-format=<format>     The format of the image tiles, either png, jpg, webp or pbf.[default: png]
      --grid-callback=<callback>  Option to control JSONP callback for UTFGrid tiles.If grids are not used as JSONP, you can remove callbacks specifying --grid_callback="".[default: grid]

     Commands:
        import
        export
        metadata

## Compile

Install [Rust](https://www.rust-lang.org/)

Build:

    $cargo build

## Test

    $cargo test

## License

BSD (see LICENSE.md)

## Authors

Arnaud Marant  
Inspired by [mbutil](https://github.com/mapbox/mbutil) : [authors](https://github.com/mapbox/mbutil#authors)
