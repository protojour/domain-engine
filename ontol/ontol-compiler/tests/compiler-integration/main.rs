use ontol_compiler::{
    hir_unify::test_api::{parse_typed, test_unify},
    mem::Mem,
};
use ontol_test_utils::{init_tracing::init_test_tracing, SrcName, TestCompile, TestPackages};
use std::{borrow::Cow, fs, path::PathBuf};

mod examples;
mod interface;
mod serde;
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
mod test_readonly;
mod test_repr;
mod test_stix_lite;
mod test_text_patterns;
mod unify;

fn main() {}

#[rstest::rstest]
fn compile(#[files("test-cases/compile/**/*.on")] path: PathBuf) {
    init_test_tracing();

    let contents = fs::read_to_string(&path).unwrap();

    let file_name = SrcName(Cow::Owned(
        path.file_name().unwrap().to_str().unwrap().into(),
    ));

    TestPackages::parse_multi_ontol(file_name, &contents).compile();
}

#[rstest::rstest]
fn error(#[files("test-cases/error/**/*.on")] path: PathBuf) {
    init_test_tracing();

    let contents = fs::read_to_string(&path).unwrap();

    let file_name = SrcName(Cow::Owned(
        path.file_name().unwrap().to_str().unwrap().into(),
    ));

    TestPackages::parse_multi_ontol(file_name, &contents).compile_fail();
}

#[rstest::rstest]
fn hir_unify(#[files("test-cases/hir-unify/**/*.test")] path: PathBuf) {
    init_test_tracing();

    let contents = fs::read_to_string(path).unwrap();
    let mut without_comments = String::new();
    for line in contents.lines() {
        if !line.starts_with("//") {
            without_comments.push_str(line);
            without_comments.push('\n');
        }
    }

    let mem = Mem::default();
    let (scope, next) = parse_typed(&without_comments);
    let (expr, next) = parse_typed(next);

    let expected = next.trim();

    let output = test_unify(&mem, &scope, &expr);

    pretty_assertions::assert_eq!(expected, output);
}
