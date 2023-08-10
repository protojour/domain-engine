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
fn test_repr_error3() {
    "
    type meters { rel .is: number }

    type my_length { // ERROR Intersection of disjoint types
        rel .is: meters // NOTE Base type is number
        rel .is: string // NOTE Base type is string
    }
    "
    .compile_fail();
}

#[test]
fn test_repr_error4() {
    "
    // NB: meters is a concrete type here:
    type meters { rel .is: int }

    type my_length { // ERROR Intersection of disjoint types
        rel .is: meters // NOTE Base type is number
        rel .is: string // NOTE Base type is string
    }
    "
    .compile_fail();
}

// FIXME: The error message can be improved
#[test]
fn test_circular_subtyping() {
    "
    type foo
    type bar
    type baz
    rel foo is: bar // ERROR TODO: Conflicting optionality for is relation
    rel bar is: baz // ERROR TODO: Conflicting optionality for is relation
    rel baz is: foo // ERROR TODO: Conflicting optionality for is relation
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
