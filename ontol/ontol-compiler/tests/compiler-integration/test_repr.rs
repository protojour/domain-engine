use ontol_macros::test;
use ontol_test_utils::{src_name, TestCompile, TestPackages};

#[test]
fn test_repr_valid_mesh1() {
    TestPackages::with_static_sources([
        (
            src_name("entry"),
            "
            use 'si' as si

            def length (
                rel .is: si.meters
                rel .is: i64
            )

            def bar (
                rel .id: (rel .is: text)
                rel .'len': length
            )
            ",
        ),
        (src_name("si"), "def meters (rel .is: number)"),
    ])
    .compile_then(|test| {
        let [meters, length] = test.bind(["si.meters", "length"]);

        assert!(
            meters.type_info.operator_addr.is_none(),
            "meters is an abstract type"
        );
        assert!(
            length.type_info.operator_addr.is_some(),
            "length is a concrete type"
        );
    });
}
