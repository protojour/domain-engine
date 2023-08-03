use ontol_test_utils::TestCompile;
use test_log::test;

#[test]
fn test_non_repr1() {
    "
    type foo { // ERROR Entity not representable
        rel .'id'|id: { rel .is: string }
        rel .'n':
            number // NOTE Type of field is abstract
    }
    "
    .compile_fail();
}

#[test]
fn test_non_repr2() {
    "
    type meters { // NOTE Type is abstract
        rel .is: number
    }

    type bar { // ERROR Entity not representable
        rel .'id'|id: { rel .is: string }
        rel .'len': meters // NOTE Type of field is abstract
    }
    "
    .compile_fail();
}
