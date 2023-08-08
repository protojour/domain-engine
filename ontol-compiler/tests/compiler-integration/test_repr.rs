use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use test_log::test;

#[test]
fn test_repr_error1() {
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
fn test_repr_error2() {
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

#[test]
// BUG: This should not type check
#[should_panic = "Scripts did not fail to compile"]
fn test_repr_error3() {
    "
    type meters { rel .is: number }
    type has_length {
        rel .is: meters
        rel .is: string
    }
    "
    .compile_fail();
}

#[test]
fn test_repr_tuple() {
    "
    pub type tup {
        rel .0..2: int
    }

    pub type bar {
        rel .'tup': tup
    }
    "
    .compile_ok(|_| {});
}

#[test]
fn test_repr_valid_mesh1() {
    TestPackages::with_sources([
        (SourceName("si"), "pub type meters { rel .is: number }"),
        (
            SourceName::root(),
            "
            use 'si' as si

            type length {
                rel .is: si.meters
                rel .is: int
            }

            type bar {
                rel .id: { rel .is: string }
                rel .'len': length
            }
            ",
        ),
    ])
    .compile_ok(|test| {
        let [meters, length] = test.bind(["si.meters", "length"]);

        assert!(
            meters.type_info.operator_id.is_none(),
            "meters is an abstract type"
        );
        assert!(
            length.type_info.operator_id.is_some(),
            "length is a concrete type"
        );
    });
}
