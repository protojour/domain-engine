use crate::{assert_error_msg, assert_json_io_matches, util::TypeBinding, TestCompile};
use assert_matches::assert_matches;
use ontol_runtime::value::Data;
use serde_json::json;
use test_log::test;

#[test]
fn import_package_not_found_error() {
    "
    use 'foobar' as foobar // ERROR package not found
    "
    .compile_fail()
}
