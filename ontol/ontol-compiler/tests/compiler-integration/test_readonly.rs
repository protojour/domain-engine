use ontol_macros::test;
use ontol_test_utils::TestCompile;

#[test]
fn readonly_property() {
    // this is just a suggestion, but I think the `|` operator makes sense here
    "
    def foo (
        rel .'id'|id: (rel .is: text)
        rel .'readonly'|readonly: text // ERROR definition not found in this scope
    )
    "
    .compile_fail();
}
