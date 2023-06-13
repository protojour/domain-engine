use ontol_test_utils::TestCompile;
use test_log::test;

pub const DEMO: &str = include_str!("../../../examples/demo.on");

#[test]
fn test_compile_demo() {
    DEMO.compile_ok(|_| ());
}
