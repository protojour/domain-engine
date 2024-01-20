use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use test_log::test;

#[test]
fn test_repr_abstract_error1() {
    "
    def foo ( // ERROR type not representable
        rel .'id'|id: (rel .is: text)
        rel .'n':
            number // NOTE Type of field is abstract
    )
    "
    .compile_fail();
}

#[test]
fn test_repr_abstract_error2() {
    "
    def meters ( // NOTE Type is abstract
        rel .is: number
    )

    def bar ( // ERROR type not representable
        rel .'len': meters // NOTE Type of field is abstract
    )
    "
    .compile_fail();
}

#[test]
fn test_repr_abstract_seq_error() {
    "
    def foo (rel .is: number) // NOTE Type is abstract
    def bar (rel .0: foo) // ERROR type not representable// NOTE Type of field is abstract
    "
    .compile_fail();
}

#[test]
fn test_repr_error3() {
    "
    def meters (rel .is: number)

    def my_length ( // ERROR Intersection of disjoint types
        rel .is: meters // NOTE Base type is number
        rel .is: text // NOTE Base type is text
    )
    "
    .compile_fail();
}

#[test]
fn test_repr_error4() {
    "
    // NB: meters is a concrete type here:
    def meters (rel .is: i64)

    def my_length ( // ERROR Intersection of disjoint types
        rel .is: meters // NOTE Base type is number
        rel .is: text // NOTE Base type is text
    )
    "
    .compile_fail();
}

#[test]
fn error_circular_subtyping() {
    "
    def foo ()
    def bar ()
    def baz ()
    rel foo is: bar // ERROR Circular subtyping relation
    rel bar is: baz // ERROR Circular subtyping relation
    rel baz is: foo // ERROR Circular subtyping relation
    "
    .compile_fail();
}

#[test]
fn error_duplicate_parameter() {
    "
    def a (
        rel .max: 20 // NOTE defined here
    )
    def b (
        rel .max: 10 // NOTE defined here
    )
    def c (
        rel .max: 5 // NOTE defined here
    )
    def d ( // ERROR duplicate type param `max`
        rel .is: a
        rel .is: b
        rel .is: c
    )
    "
    .compile_fail();
}

#[test]
fn test_repr_tuple() {
    "
    def tup (
        rel .0..2: i64
    )

    def bar (
        rel .'tup': tup
    )
    "
    .compile_then(|_| {});
}

#[test]
fn test_repr_valid_mesh1() {
    TestPackages::with_sources([
        (SourceName("si"), "def meters (rel .is: number)"),
        (
            SourceName::root(),
            "
            use 'si' as si

            def length (
                rel .is: si.meters
                rel .is: i64
            )

            def bar (
                rel .id: (rel .is: text)
                rel .'len': length
            )
            ",
        ),
    ])
    .compile_then(|test| {
        let [meters, length] = test.bind(["si.meters", "length"]);

        assert!(
            meters.type_info.operator_addr.is_none(),
            "meters is an abstract type"
        );
        assert!(
            length.type_info.operator_addr.is_some(),
            "length is a concrete type"
        );
    });
}

#[test]
// BUG: todo causes panic in LSP
#[should_panic = "not yet implemented: Intersection"]
fn union_integers_no_question() {
    "
    def level (
        rel .is: 1
        rel .is: 2
        rel .is: 3
    )
    "
    .compile_fail();
}

#[test]
// BUG: this mirrors how enums are constucted for text, should work
fn union_integers() {
    "
    def level (
        rel .is?: 1 // ERROR cannot discriminate type
        rel .is?: 2 // ERROR cannot discriminate type
        rel .is?: 3 // ERROR cannot discriminate type
    )
    "
    .compile_fail();
}

#[test]
// BUG: example of "not yet implemented: More members".
// Expected to be able to compose like this.
#[should_panic = "not yet implemented: More members"]
fn more_members() {
    "
    def created (
        rel .'created'(rel .gen: create_time)?: datetime
    )
    def updated (
        rel .'updated'(rel .gen: update_time)?: datetime
    )
    def foo_id (
        fmt '' => 'foos/' => text => .
    )
    def foo (
        rel .'_id'|id(rel .gen: auto): foo_id
        rel .is: created
        rel .is: updated
        rel .'name': text
    )

    map foos (
        (),
        foo: { ..foo match () }
    )
    "
    .compile_fail();
}

#[test]
// BUG: somewhat related – this should work?
fn forward_mapping() {
    "
    def created (
        rel .'created'(rel .gen: create_time)?: datetime
    )
    def foo_id (
        fmt '' => 'foos/' => text => .
    )
    def foo (
        rel .'_id'|id(rel .gen: auto): foo_id
        rel .is: created
        rel .'name': text
    )

    map foos ( // ERROR BUG: Failed to generate forward mapping
        (),
        foo: { ..foo match () }
    )
    "
    .compile_fail();
}
