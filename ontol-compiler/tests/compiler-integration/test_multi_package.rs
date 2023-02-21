use crate::{
    assert_error_msg, assert_json_io_matches, util::TypeBinding, SourceName, TestCompile,
    TestPackages, ROOT_SRC_NAME,
};
use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use serde_json::json;
use test_log::test;

#[test]
fn import_package_not_found_error() {
    "
    use
    'pkg' // ERROR package not found
    as foo
    "
    .compile_fail()
}

#[test]
fn load_package() {
    TestPackages::with_sources([
        (
            SourceName("pkg"),
            "
            type bar
            ",
        ),
        (
            SourceName::root(),
            "
            use 'pkg' as foo
            ",
        ),
    ])
    .compile_ok(|env| {});
}
