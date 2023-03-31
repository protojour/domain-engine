use ontol_test_utils::TestCompile;
use test_log::test;

pub const STIX_ONT: &str = include_str!("../../../examples/stix_lite/stix.ont");

#[test]
fn test_stix_lite() {
    STIX_ONT.compile_fail()
}
