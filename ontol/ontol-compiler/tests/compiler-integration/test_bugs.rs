use ontol_test_utils::{src_name, TestCompile, TestPackages};

#[test]
#[should_panic = "not yet implemented"]
fn constant_optional_mismatch() {
    TestPackages::with_static_sources([
        (
            src_name("root"),
            "
            use 'other' as other

            def bar (
                rel .'bar': 'constant'
            )

            map(
                bar(
                    'bar': b
                ),
                other.foo(
                    'bar': b
                )
            )
            ",
        ),
        (
            src_name("other"),
            "
            def foo (
                rel .'bar'?: 'constant'
            )
            ",
        ),
    ])
    .compile();
}
