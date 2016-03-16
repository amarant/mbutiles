extern crate mbutiles;

use mbutiles::{export, import, Scheme, ImageFormat };
use std::fs;
use std::env;
use std::io::Error;
use std::path::PathBuf;

fn clear_data(output: &str) -> Result<(PathBuf, PathBuf),Error> {
    let current_dir = try!(env::current_dir());
    let tests = current_dir.join("tests");
    let output = tests.join(output);
    //println!("remove: {:?}", output);
    if output.exists() {
        try!(fs::remove_dir_all(output.clone()));
    }
    Ok((tests, output))
}

#[test]
fn export_saves_tiles_and_metadata() {
    let output_name = "output_saves_tiles_and_metadata";
    let (tests, output) = clear_data(output_name).unwrap();
    export(tests.join("data/one_tile.mbtiles"),
        Some(output.clone()), Scheme::Xyz, ImageFormat::Png, "".to_owned()).unwrap();
    assert!(output.join("0/0/0.png").exists());
    assert!(output.join("1/0/0.png").exists());
    assert!(output.join("metadata.json").exists());
    clear_data(output_name).unwrap();
}

#[test]
fn export_saves_tiles_and_metadata_and_back() {
    let output_name = "output_saves_tiles_and_metadata_and_back";
    let (tests, output) = clear_data(output_name).unwrap();
    export(tests.join("data/one_tile.mbtiles"),
        Some(output.clone()), Scheme::Xyz, ImageFormat::Png, "".to_owned()).unwrap();
    import(output.clone(), output.join("one_tile.mbtiles"),
        Scheme::Xyz, ImageFormat::Png, "".to_owned()).unwrap();
    assert!(output.join("one_tile.mbtiles").exists());
    clear_data(output_name).unwrap();
}

#[test]
fn export_saves_utf8grid_tiles_and_metadata() {
    let output_name = "saves_utf8grid_tiles_and_metadata";
    let (tests, output) = clear_data(output_name).unwrap();
    export(tests.join("data/utf8grid.mbtiles"),
        Some(output.clone()), Scheme::Xyz, ImageFormat::Png, "".to_owned()).unwrap();
    //assert!(output.join("0/0/0.grid.json").exists());
    assert!(output.join("0/0/0.png").exists());
    assert!(output.join("metadata.json").exists());
    clear_data(output_name).unwrap();
}
