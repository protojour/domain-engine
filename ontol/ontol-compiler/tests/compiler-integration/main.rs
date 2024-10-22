#![allow(clippy::disallowed_names)]

use ontol_test_utils::{file_url, TestCompile, TestPackages};
use std::{fs, path::PathBuf};

mod interface;
mod test_atlas;
mod test_bugs;
mod test_conduit;
mod test_demo;
mod test_entity;
mod test_error_spans;
mod test_faker;
mod test_fundamentals;
mod test_geojson;
mod test_json_schema;
mod test_multi_package;
mod test_repr;
mod test_stix_lite;
mod test_text_patterns;
mod test_workspaces;
mod unify;

fn main() {}

#[rstest::rstest]
#[ontol_macros::test]
fn compile(#[files("test-cases/compile/**/*.on")] path: PathBuf) {
    let contents = fs::read_to_string(&path).unwrap();

    let file_url = file_url(path.file_name().unwrap().to_str().unwrap());

    TestPackages::parse_multi_ontol(file_url, &contents).compile();
}

#[rstest::rstest]
#[ontol_macros::test]
fn error(#[files("test-cases/error/**/*.on")] path: PathBuf) {
    let contents = fs::read_to_string(&path).unwrap();

    let file_url = file_url(path.file_name().unwrap().to_str().unwrap());

    TestPackages::parse_multi_ontol(file_url, &contents).compile_fail();
}
