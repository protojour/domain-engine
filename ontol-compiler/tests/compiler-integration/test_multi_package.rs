use ontol_test_utils::{
    assert_json_io_matches, type_binding::TypeBinding, SourceName, TestCompile, TestPackages,
};
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
            pub type foo {
                rel _ 'prop': int
            }
            ",
        ),
        (
            SourceName::root(),
            "
            use 'pkg' as other

            pub type bar {
                rel _ 'foo': other.foo
            }
            ",
        ),
    ])
    .compile_ok(|env| {
        let bar = TypeBinding::new(&env, "bar");
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

            pub type foobar {
                rel _ 'a': a.a
                rel _ 'b': b.b
            }
            ",
        ),
        (
            SourceName("a"),
            "
            use 'c' as domain_c
            pub type a {
                rel _ 'c': domain_c.c
            }
            ",
        ),
        (
            SourceName("b"),
            "
            use 'c' as c
            pub type b {
                rel _ 'c': c.c
            }
            ",
        ),
        (SourceName("c"), "pub type c { rel _ is: int }"),
    ])
    .compile_ok(|env| {
        // four user domains, plus core:
        assert_eq!(5, env.env.domain_count());

        let bar = TypeBinding::new(&env, "foobar");
        assert_json_io_matches!(
            bar,
            json!({
                "a": { "c": 42 },
                "b": { "c": 43 }
            })
        );
    });
}
