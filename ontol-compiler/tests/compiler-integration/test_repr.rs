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
// BUG: the intersection logic is broken (does not consider representational vs. semantic types)
#[should_panic = "Classic intersection (repr = I64)"]
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
    .compile_ok(|_test| {});
}
