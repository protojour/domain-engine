use ontol_test_utils::TestCompile;
use test_log::test;

pub const CONDUIT_PUBLIC: &str = include_str!("../../../examples/conduit/conduit_public.on");
pub const CONDUIT_DB: &str = include_str!("../../../examples/conduit/conduit_db.on");

#[test]
fn test_compile_conduit_public() {
    CONDUIT_PUBLIC.compile_fail();
}

#[test]
fn test_compile_conduit_db() {
    CONDUIT_DB.compile_ok(|_env| {});
}
