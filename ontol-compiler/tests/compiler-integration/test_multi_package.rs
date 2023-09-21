use ontol_test_utils::{assert_json_io_matches, expect_eq, SourceName, TestCompile, TestPackages};
use test_log::test;

#[test]
fn import_package_not_found_error() {
    "
    use
    'pkg' // ERROR package not found
    as foo
    "
    .compile_fail();
}

#[test]
fn load_package() {
    TestPackages::with_sources([
        (
            SourceName("pkg"),
            "
            pub def foo {
                rel .'prop': i64
            }
            ",
        ),
        (
            SourceName::root(),
            "
            use 'pkg' as other

            pub def bar {
                rel .'foo': other.foo
            }
            ",
        ),
    ])
    .compile_then(|test| {
        let [bar] = test.bind(["bar"]);
        assert_json_io_matches!(bar, Create, {
            "foo": {
                "prop": 42
            }
        });
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

            pub def foobar {
                rel .'a': a.a
                rel .'b': b.b
            }
            ",
        ),
        (
            SourceName("a"),
            "
            use 'c' as domain_c
            pub def a {
                rel .'c': domain_c.c
            }
            ",
        ),
        (
            SourceName("b"),
            "
            use 'c' as c
            pub def b {
                rel .'c': c.c
            }
            ",
        ),
        (SourceName("c"), "pub def c { rel .is: i64 }"),
    ])
    .compile_then(|test| {
        // four user domains, plus `ontol`:
        expect_eq!(actual = test.ontology.domains().count(), expected = 5);

        let [foobar] = test.bind(["foobar"]);
        assert_json_io_matches!(foobar, Create, {
            "a": { "c": 42 },
            "b": { "c": 43 }
        });
    });
}
