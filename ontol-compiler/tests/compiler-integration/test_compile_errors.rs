use crate::TestCompile;
use test_log::test;

#[test]
fn lex_error() {
    "( ;; ERROR lex error".compile_fail();
}

#[test]
fn parse_error1() {
    "() ;; ERROR parse error: expected keyword".compile_fail()
}

#[test]
fn underscore_not_allowed_at_start_of_identifier() {
    "(type! _foo) ;; ERROR parse error: expected ident".compile_fail()
}

#[test]
fn rel_type_not_found() {
    "
    (type! foo)
    (rel! foo { bar }
        baz ;; ERROR type not found
    )
    "
    .compile_fail()
}

#[test]
fn rel_duplicate_anonymous_relation() {
    "
    (type! foo)
    (type! bar)
    (rel! ;; ERROR unit type `bar` cannot be part of a union
        foo {} bar
    )
    (rel! ;; ERROR duplicate anonymous relationship
        foo {} bar
    )
    "
    .compile_fail()
}

#[test]
fn rel_mix_anonymous_and_named() {
    "
    (type! foo)
    (type! bar)
    (rel! foo {} bar)
    (rel! ;; ERROR invalid mix of relationship type for subject
        foo {foobar} bar
    )
    "
    .compile_fail()
}

#[test]
fn rel_array_range_with_dots_is_illegal() {
    "
    (type! foo)
    (rel!
        foo
        {n[..]} ;; ERROR parse error: expected end of list
        int
    )
    "
    .compile_fail()
}

#[test]
fn map_union_unit_type() {
    "
    (type! foo)
    (type! bar)
    (type! u)
    (rel! u {} foo) ;; ERROR unit type `foo` cannot be part of a union
    (rel! u {} bar) ;; ERROR unit type `bar` cannot be part of a union
    "
    .compile_fail()
}

#[test]
fn map_union_missing_discriminator() {
    "
    (type! foo)
    (type! bar)
    (rel! foo {a} 'constant')
    (rel! bar {b} string)
    (type! u)
    (rel! u {} foo)
    (rel! u {} bar) ;; ERROR cannot discriminate type
    "
    .compile_fail()
}

#[test]
fn map_union_non_uniform_discriminators() {
    "
    (type! foo)
    (type! bar)
    (rel! foo {a} 'constant')
    (rel! bar {b} 'other-constant')
    (type! u) ;; ERROR no uniform discriminator found for union variants
    (rel! u {} foo)
    (rel! u {} bar)
    "
    .compile_fail()
}

#[test]
fn non_disjoint_string_union() {
    "
    (type! u1)
    (rel! u1 {} 'a')
    (rel! u1 {} 'a') ;; ERROR duplicate anonymous relationship
    "
    .compile_fail()
}

#[test]
fn union_tree() {
    "
    (type! u1)
    (rel! u1 {} '1a')
    (rel! u1 {} '1b')
    (type! u2)
    (rel! u2 {} '2a')
    (rel! u2 {} '2b')
    (type! u3)
    (rel! u3 {} u1) ;; ERROR union tree not supported
    (rel! u3 {} u2) ;; ERROR union tree not supported
    "
    .compile_fail()
}

#[test]
fn sequence_mix1() {
    r#"
    (type! u)
    (rel! u {} int)
    (rel! u { 0 } string) ;; ERROR invalid mix of relationship type for subject
    "#
    .compile_fail();
}

#[test]
fn sequence_mix2() {
    r#"
    (type! u)
    (rel! u { a } int)
    (rel! u { 0 } string) ;; ERROR invalid mix of relationship type for subject
    "#
    .compile_fail();
}

#[test]
fn sequence_overlapping_indices() {
    r#"
    (type! u)
    (rel! u { 0..3 } int)
    (rel! u { 2..4 } string) ;; ERROR overlapping indexes
    "#
    .compile_fail();
}

#[test]
fn sequence_ambiguous_infinite_tail() {
    r#"
    (type! u)
    (rel! u { 0.. } int)
    (rel! u { 1.. } string) ;; ERROR overlapping indexes
    "#
    .compile_fail();
}

#[test]
fn eq_undeclared_variable() {
    "
    (eq! ()
        :x ;; ERROR undeclared variable
        42
    )
    "
    .compile_fail()
}

#[test]
fn eq_incorrect_function_arguments() {
    "
    (eq! (:x)
        (+ ;; ERROR function takes 2 parameters, but 1 was supplied
            :x
        )
        42
    )
    "
    .compile_fail()
}

#[test]
fn eq_obj_non_domain_type_and_unit_type() {
    "
    (type! foo)
    (eq! (:x)
        (obj!
            number ;; ERROR expected domain type
        )
        (obj! ;; ERROR no properties expected
            foo (prop :x)
        )
    )
    "
    .compile_fail()
}

#[test]
fn eq_attribute_mismatch() {
    "
    (type! foo)
    (type! bar)
    (rel! foo { prop } bar)
    (rel! bar {} int)
    (eq! (:x)
        (obj! ;; ERROR missing property `prop`
            foo
            (_ :x) ;; ERROR expected named property
        )
        (obj! bar) ;; ERROR expected anonymous property
    )
    "
    .compile_fail()
}

#[test]
fn eq_duplicate_unknown_property() {
    "
    (type! foo)
    (type! bar)
    (rel! foo { a } bar)
    (eq! (:x)
        (obj! foo
            (a :x)
            (a :x) ;; ERROR duplicate property
            (b :x) ;; ERROR unknown property
        )
        (obj! bar)
    )
    "
    .compile_fail()
}

#[test]
fn eq_type_mismatch() {
    "
    (type! foo)
    (type! bar)
    (rel! foo { prop } string)
    (rel! bar { prop } int)
    (eq! (:x)
        (obj! foo (prop :x))
        (obj! bar
            (prop
                :x ;; ERROR type mismatch: expected `int`, found `string`
            )
        )
    )
    "
    .compile_fail()
}

#[test]
fn eq_type_mismatch_in_func() {
    "
    (type! foo)
    (type! bar)
    (rel! foo {prop} string)
    (rel! bar {prop} int)
    (eq! (:x)
        (obj! foo (prop :x))
        (obj! bar
            (prop
                (*
                    :x ;; ERROR type mismatch: expected `int`, found `string`
                    2
                )
            )
        )
    )
    "
    .compile_fail()
}

#[test]
fn eq_array_mismatch() {
    "
    (type! foo)
    (rel! foo { a[] } string)
    (rel! foo { b[] } string)

    (type! bar)
    (rel! bar { a } string)
    (rel! bar { b[] } int)

    (eq! (:x :y)
        (obj! foo (a :x) (b :y))
        (obj! bar
            (a :x) ;; ERROR type mismatch: expected `string`, found `string[]`
            (b :y) ;; ERROR type mismatch: expected `int[]`, found `string[]`
        )
    )
    "
    .compile_fail();
}

#[test]
fn union_in_named_relationship() {
    "
    (type! foo)
    (rel! foo {a} string)
    (rel! foo {a} int) ;; ERROR union in named relationship is not supported yet. Make a union type instead.
    "
    .compile_fail();
}

#[test]
fn test_serde_object_property_not_sugared() {
    "
    (type! foo)
    (type! bar)
    (rel! foo {a @unit aa[]} bar) ;; ERROR only entities may have named reverse relationship
    (rel! foo {b @unit bb[]} string) ;; ERROR only entities may have named reverse relationship
    "
    .compile_fail()
}

#[test]
fn unresolved_transitive_eq() {
    "
    (type! a)
    (type! b)
    (rel! a {} int)
    (rel! b {} int)

    (type! c)
    (type! d)
    (rel! c { p0 } a)
    (rel! d { p1 } b)

    (eq! (:x)
        (obj! c (p0
            :x ;; ERROR cannot convert this `a` from `b`: These types are not equated.
        ))
        (obj! d (p1
            :x ;; ERROR cannot convert this `b` from `a`: These types are not equated.
        ))
    )
    "
    .compile_fail();
}

#[test]
fn various_monadic_properties() {
    "
    (type! foo)
    (rel! foo { a } string)
    (default! foo { a } 'default') ;; ERROR parse error: unknown keyword

    (type! bar)
    ; a is either a string or not present
    (rel! bar { maybe? } string)

    ; bar and string may be related via b many times
    (rel! bar { array[] } string)

    ; a is either a string or null
    (rel! bar { nullable } string)
    ; FIXME: Should this work?
    (rel! bar { nullable } null) ;; ERROR union in named relationship is not supported yet. Make a union type instead.
    "
    .compile_fail()
}

#[test]
fn mix_of_index_and_edge_type() {
    r#"
    (type! foo)
    (type! bar)

    (rel! foo {0 @bar} string) ;; ERROR cannot mix index relation identifiers and edge types
    "#
    .compile_fail()
}
