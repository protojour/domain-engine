use crate::assert_json_io_matches;
use crate::{util::type_binding::TypeBinding, SourceName, TestCompile, TestPackages};
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

#[test]
fn dependency_dag() {
    TestPackages::with_sources([
        (
            SourceName::root(),
            "
            use 'a' as a
            use 'b' as b

            type foobar {
                rel { 'a' } a.a
                rel { 'b' } b.b
            }
            ",
        ),
        (
            SourceName("a"),
            "
            use 'c' as domain_c
            type a {
                rel { 'c' } domain_c.c
            }
            ",
        ),
        (
            SourceName("b"),
            "
            use 'c' as c
            type b {
                rel { 'c' } c.c
            }
            ",
        ),
        (SourceName("c"), "type c { rel . { int } }"),
    ])
    .compile_ok(|env| {
        // four user domains, plus core:
        assert_eq!(5, env.domains.len());

        let bar = TypeBinding::new(env, "foobar");
        assert_json_io_matches!(
            bar,
            json!({
                "a": { "c": 42 },
                "b": { "c": 43 }
            })
        );
    });
}
