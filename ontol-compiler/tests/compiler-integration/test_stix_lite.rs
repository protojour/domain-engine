use ontol_test_utils::TestCompile;
use test_log::test;

pub const STIX_ONT: &str = include_str!("../../../examples/stix_lite/stix.ont");

#[test]
fn test_stix_lite() {
    STIX_ONT.compile_fail();
    /*
    STIX_ONT.compile_ok(|env| {
        let attack_pattern = TypeBinding::new(&env, "attack_pattern");
        assert_json_io_matches!(
            attack_pattern,
            json!({
                "type": "attack-pattern",
                "created_by_ref": "identity--a1a2a3a4-b1b2-c1c2-d1d2-d3d4d5d6d7d8",
            })
        );
    });
    */
}
