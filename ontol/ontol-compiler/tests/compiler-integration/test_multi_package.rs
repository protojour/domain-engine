use ontol_test_utils::{
    assert_json_io_matches, expect_eq, serde_helper::serde_create, src_name, TestCompile,
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
    TestPackages::with_static_sources([
        (
            src_name("root"),
            "
            use 'pkg' as other

            def bar (
                rel .'foo': other.foo
            )
            ",
        ),
        (
            src_name("pkg"),
            "
            def foo (
                rel .'prop': i64
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
    TestPackages::with_static_sources([
        (
            src_name("root"),
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
            src_name("a"),
            "
            use 'c' as domain_c
            def a (
                rel .'c': domain_c.c
            )
            ",
        ),
        (
            src_name("b"),
            "
            use 'c' as c
            def b (
                rel .'c': c.c
            )
            ",
        ),
        (src_name("c"), "def c (rel .is: i64)"),
    ])
    .compile_then(|test| {
        // four user domains, plus `ontol`:
        expect_eq!(actual = test.ontology().domains().count(), expected = 5);

        let [foobar] = test.bind(["foobar"]);
        assert_json_io_matches!(serde_create(&foobar), {
            "a": { "c": 42 },
            "b": { "c": 43 }
        });
    });
}

#[test]
fn multiple_roots() {
    TestPackages::with_static_sources([
        (
            src_name("foo"),
            "
            use 'baz' as baz
            def foo ()
            ",
        ),
        (
            src_name("bar"),
            "
            use 'baz' as baz
            def bar ()
            ",
        ),
        (
            src_name("baz"),
            "
            def baz ()
            ",
        ),
    ])
    .with_roots([src_name("foo"), src_name("bar")])
    .compile_then(|test| {
        let _ = test.bind(["foo.foo", "bar.bar", "baz.baz"]);
    });
}
