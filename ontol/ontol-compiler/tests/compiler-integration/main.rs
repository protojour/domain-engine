use ontol_test_utils::{SrcName, TestCompile, TestPackages};
use std::{borrow::Cow, fs, path::PathBuf};

mod examples;
mod interface;
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
mod unify;

fn main() {}

#[rstest::rstest]
#[ontol_macros::test]
fn compile(#[files("test-cases/compile/**/*.on")] path: PathBuf) {
    let contents = fs::read_to_string(&path).unwrap();

    let file_name = SrcName(Cow::Owned(
        path.file_name().unwrap().to_str().unwrap().into(),
    ));

    TestPackages::parse_multi_ontol(file_name, &contents).compile();
}

#[rstest::rstest]
#[ontol_macros::test]
fn error(#[files("test-cases/error/**/*.on")] path: PathBuf) {
    let contents = fs::read_to_string(&path).unwrap();

    let file_name = SrcName(Cow::Owned(
        path.file_name().unwrap().to_str().unwrap().into(),
    ));

    TestPackages::parse_multi_ontol(file_name, &contents).compile_fail();
}
