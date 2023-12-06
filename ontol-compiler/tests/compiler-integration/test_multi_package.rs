use ontol_test_utils::{
    assert_json_io_matches, expect_eq, serde_helper::serde_create, SourceName, TestCompile,
    TestPackages,
};
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
            def foo (
                rel .'prop': i64
            )
            ",
        ),
        (
            SourceName::root(),
            "
            use 'pkg' as other

            def bar (
                rel .'foo': other.foo
            )
            ",
        ),
    ])
    .compile_then(|test| {
        let [bar] = test.bind(["bar"]);
        assert_json_io_matches!(serde_create(&bar), {
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

            def foobar (
                rel .'a': a.a
                rel .'b': b.b
            )
            ",
        ),
        (
            SourceName("a"),
            "
            use 'c' as domain_c
            def a (
                rel .'c': domain_c.c
            )
            ",
        ),
        (
            SourceName("b"),
            "
            use 'c' as c
            def b (
                rel .'c': c.c
            )
            ",
        ),
        (SourceName("c"), "def c (rel .is: i64)"),
    ])
    .compile_then(|test| {
        // four user domains, plus `ontol`:
        expect_eq!(actual = test.ontology.domains().count(), expected = 5);

        let [foobar] = test.bind(["foobar"]);
        assert_json_io_matches!(serde_create(&foobar), {
            "a": { "c": 42 },
            "b": { "c": 43 }
        });
    });
}
