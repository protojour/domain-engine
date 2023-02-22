use crate::assert_json_io_matches;
use crate::{
    assert_error_msg, util::TypeBinding, SourceName, TestCompile, TestPackages, ROOT_SRC_NAME,
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
            type foo {
                rel { 'prop' } int
            }
            ",
        ),
        (
            SourceName::root(),
            "
            use 'pkg' as other

            type bar {
                rel { 'foo' } other.foo
            }
            ",
        ),
    ])
    .compile_ok(|env| {
        let bar = TypeBinding::new(env, "bar");
        assert_json_io_matches!(
            bar,
            json!({
                "foo": {
                    "prop": 42
                }
            })
        );
    });
}
