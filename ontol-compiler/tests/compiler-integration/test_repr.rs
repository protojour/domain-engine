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
    type meters { rel .is: i64 }

    type my_length { // ERROR Intersection of disjoint types
        rel .is: meters // NOTE Base type is number
        rel .is: string // NOTE Base type is string
    }
    "
    .compile_fail();
}

#[test]
fn error_circular_subtyping() {
    "
    type foo
    type bar
    type baz
    rel foo is: bar // ERROR Circular subtyping relation
    rel bar is: baz // ERROR Circular subtyping relation
    rel baz is: foo // ERROR Circular subtyping relation
    "
    .compile_fail();
}

#[test]
fn error_duplicate_parameter() {
    "
    type a {
        rel .max: 20 // NOTE defined here
    }
    type b {
        rel .max: 10 // NOTE defined here
    }
    type c {
        rel .max: 5 // NOTE defined here
    }
    type d { // ERROR duplicate type param `max`
        rel .is: a
        rel .is: b
        rel .is: c
    }
    "
    .compile_fail();
}

#[test]
fn test_repr_tuple() {
    "
    pub type tup {
        rel .0..2: i64
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
                rel .is: i64
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
