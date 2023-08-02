use ontol_test_utils::TestCompile;
use test_log::test;

// BUG: Should fail because `number` is abstract - therefore `foo` is also abstract
#[test]
fn test_abstract_number() {
    "
    type foo {
        rel .is: number
    }
    "
    .compile_ok(|test| {
        let [_foo] = test.bind(["foo"]);
    });
}
