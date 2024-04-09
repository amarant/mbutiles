extern crate mbutiles;
extern crate serde_json;
extern crate serde_json_path;

use mbutiles::{export, import, ImageFormat, Scheme};
use serde_json_path::JsonPath;
use std::env;
use std::fs;
use std::fs::File;
use std::io::Error;
use std::path::PathBuf;

fn clear_data(output: &str) -> Result<(PathBuf, PathBuf), Error> {
    let current_dir = env::current_dir()?;
    let tests = current_dir.join("tests");
    let output = tests.join(output);
    //println!("remove: {:?}", output);
    if output.exists() {
        fs::remove_dir_all(output.clone())?;
    }
    Ok((tests, output))
}

#[test]
fn export_saves_tiles_and_metadata() {
    let output_name = "output_saves_tiles_and_metadata";
    let (tests, output) = clear_data(output_name).unwrap();
    export(
        tests.join("data/one_tile.mbtiles"),
        Some(output.clone()),
        Scheme::Xyz,
        ImageFormat::Png,
        "".to_owned(),
    )
    .unwrap();
    assert!(output.join("0/0/0.png").exists());
    assert!(output.join("1/0/0.png").exists());
    assert!(output.join("metadata.json").exists());
    clear_data(output_name).unwrap();
}

#[test]
fn export_saves_tiles_and_metadata_and_back() {
    let output_name = "output_saves_tiles_and_metadata_and_back";
    let (tests, output) = clear_data(output_name).unwrap();
    export(
        tests.join("data/one_tile.mbtiles"),
        Some(output.clone()),
        Scheme::Xyz,
        ImageFormat::Png,
        "".to_owned(),
    )
    .unwrap();
    import(
        output.clone(),
        output.join("one_tile.mbtiles"),
        Scheme::Xyz,
        ImageFormat::Png,
    )
    .unwrap();
    assert!(output.join("one_tile.mbtiles").exists());
    clear_data(output_name).unwrap();
}

#[test]
fn export_saves_utf8grid_tiles_and_metadata() {
    let output_name = "saves_utf8grid_tiles_and_metadata";
    let (tests, output) = clear_data(output_name).unwrap();
    export(
        tests.join("data/utf8grid.mbtiles"),
        Some(output.clone()),
        Scheme::Xyz,
        ImageFormat::Png,
        "".to_owned(),
    )
    .unwrap();
    assert!(output.join("0/0/0.grid.json").exists());
    assert!(output.join("0/0/0.png").exists());
    assert!(output.join("metadata.json").exists());
    clear_data(output_name).unwrap();
}

#[test]
fn import_tiles_to_utf8grid_mbtiles() {
    let output_name = "tiles_to_utf8grid_mbtiles";
    let (tests, output) = clear_data(output_name).unwrap();
    export(
        tests.join("data/utf8grid.mbtiles"),
        Some(output.join("exported")),
        Scheme::Xyz,
        ImageFormat::Png,
        "".to_owned(),
    )
    .unwrap();
    import(
        output.join("exported"),
        output.join("imported.mbtiles"),
        Scheme::Xyz,
        ImageFormat::Png,
    )
    .unwrap();
    export(
        output.join("imported.mbtiles"),
        Some(output.join("imported")),
        Scheme::Xyz,
        ImageFormat::Png,
        "".to_owned(),
    )
    .unwrap();
    assert!(output.join("imported/0/0/0.grid.json").exists());
    let mut exported_grid = File::open(output.join("exported/0/0/0.grid.json")).unwrap();
    let exported_json = serde_json::from_reader(&mut exported_grid).unwrap();
    let mut imported_grid = File::open(output.join("imported/0/0/0.grid.json")).unwrap();
    let imported_json: serde_json::Value = serde_json::from_reader(&mut imported_grid).unwrap();
    let path = JsonPath::parse("$.data[77]").unwrap();
    assert_eq!(path.query(&imported_json), path.query(&exported_json));
    clear_data(output_name).unwrap();
}
