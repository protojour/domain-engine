use ontol_test_utils::TestCompile;

pub const CONDUIT_PUBLIC: &str = include_str!("../../../examples/conduit/conduit_public.on");

#[test]
fn test_compile_conduit_public() {
    CONDUIT_PUBLIC.compile_fail();
}
