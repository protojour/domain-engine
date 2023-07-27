use ontol_test_utils::{
    assert_json_io_matches, expect_eq, type_binding::TypeBinding, SourceName, TestCompile,
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
    .compile_fail()
}

#[test]
fn load_package() {
    TestPackages::with_sources([
        (
            SourceName("pkg"),
            "
            pub type foo {
                rel .'prop': int
            }
            ",
        ),
        (
            SourceName::root(),
            "
            use 'pkg' as other

            pub type bar {
                rel .'foo': other.foo
            }
            ",
        ),
    ])
    .compile_ok(|test| {
        let bar = TypeBinding::new(&test, "bar");
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

            pub type foobar {
                rel .'a': a.a
                rel .'b': b.b
            }
            ",
        ),
        (
            SourceName("a"),
            "
            use 'c' as domain_c
            pub type a {
                rel .'c': domain_c.c
            }
            ",
        ),
        (
            SourceName("b"),
            "
            use 'c' as c
            pub type b {
                rel .'c': c.c
            }
            ",
        ),
        (SourceName("c"), "pub type c { rel .is: int }"),
    ])
    .compile_ok(|test| {
        // four user domains, plus core:
        expect_eq!(actual = test.ontology.domains().count(), expected = 5);

        let bar = TypeBinding::new(&test, "foobar");
        assert_json_io_matches!(bar, Create, {
            "a": { "c": 42 },
            "b": { "c": 43 }
        });
    });
}
