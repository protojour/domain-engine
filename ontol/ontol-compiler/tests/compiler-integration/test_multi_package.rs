use ontol_macros::test;
use ontol_test_utils::{
    TestCompile, TestPackages, assert_json_io_matches, expect_eq, file_url,
    serde_helper::serde_create,
};

#[test]
fn load_package() {
    TestPackages::with_static_sources([
        (
            file_url("root"),
            "
            use 'pkg' as other

            def bar (
                rel* 'foo': other.foo
            )
            ",
        ),
        (
            file_url("pkg"),
            "
            def foo (
                rel* 'prop': i64
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
            file_url("root"),
            "
            use 'a' as a
            use 'b' as b

            def foobar (
                rel* 'a': a.a
                rel* 'b': b.b
            )
            ",
        ),
        (
            file_url("a"),
            "
            use 'c' as domain_c
            def a (
                rel* 'c': domain_c.c
            )
            ",
        ),
        (
            file_url("b"),
            "
            use 'c' as c
            def b (
                rel* 'c': c.c
            )
            ",
        ),
        (file_url("c"), "def c (rel* is: i64)"),
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
            file_url("foo"),
            "
            use 'baz' as baz
            def foo ()
            ",
        ),
        (
            file_url("bar"),
            "
            use 'baz' as baz
            def bar ()
            ",
        ),
        (
            file_url("baz"),
            "
            def baz ()
            ",
        ),
    ])
    .with_entrypoints([file_url("foo"), file_url("bar")])
    .compile_then(|test| {
        let _ = test.bind(["foo.foo", "bar.bar", "baz.baz"]);
    });
}
