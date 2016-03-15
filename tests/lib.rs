extern crate mbutiles;

use mbutiles::{export, import, Scheme, ImageFormat };
use std::fs;
use std::env;
use std::io::Error;
use std::path::PathBuf;

fn clear_data() -> Result<(PathBuf, PathBuf),Error> {
    let current_dir = try!(env::current_dir());
    let tests = current_dir.join("tests");
    let output = tests.join("output");
    println!("remove: {:?}", output);
    //try!(fs::remove_dir_all(output));
    Ok((tests, output))
}

#[test]
fn a() {
    let (tests, output) = clear_data().unwrap();
    export(tests.join("data").join("one_tile.mbtiles"),
        Some(output), Scheme::Xyz, ImageFormat::Png, "".to_owned()).unwrap();
}
