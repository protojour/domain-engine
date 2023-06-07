use ontol_test_utils::TestCompile;

pub const DEMO: &str = include_str!("../../../examples/demo.on");

#[test]
fn test_compile_demo() {
    DEMO.compile_ok(|_| ());
}
