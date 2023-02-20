use crate::TestCompile;
use pretty_assertions::assert_eq;
use test_log::test;

#[test]
fn invalid_statement() {
    "foobar // ERROR parse error: found `foobar`, expected one of `type`, `rel`, `eq`"
        .compile_fail();
}

#[test]
fn type_parse_error() {
    "type // ERROR parse error: expected identifier".compile_fail();
    "type {} // ERROR parse error: found `{`, expected identifier".compile_fail();
}

#[test]
fn underscore_not_allowed_at_start_of_identifier() {
    "type _foo // ERROR parse error: found `_`, expected identifier".compile_fail()
}

#[test]
fn rel_type_not_found() {
    "
    type foo
    rel foo { 'bar' }
        baz // ERROR type not found
    
    "
    .compile_fail()
}

#[test]
fn rel_duplicate_anonymous_relation() {
    "
    type foo
    type bar
    rel // ERROR unit type `bar` cannot be part of a union
        . { bar } foo    
    rel // ERROR duplicate anonymous relationship
        . { bar } foo
    "
    .compile_fail()
}

#[test]
fn rel_mix_anonymous_and_named() {
    "
    type foo
    type bar
    rel . { bar } foo

    rel // ERROR invalid mix of relationship type for subject
        foo { 'foobar' } bar
    "
    .compile_fail()
}

#[test]
fn map_union_unit_type() {
    "
    type foo
    type bar
    type u
    rel . { foo } u // ERROR unit type `foo` cannot be part of a union
    rel . { bar } u // ERROR unit type `bar` cannot be part of a union
    "
    .compile_fail()
}

#[test]
fn map_union_missing_discriminator() {
    "
    type foo
    type bar
    rel foo { 'a' } 'constant'
    rel bar { 'b' } string
    type u
    rel . { foo } u
    rel . { bar } u // ERROR cannot discriminate type
    "
    .compile_fail()
}

#[test]
fn map_union_non_uniform_discriminators() {
    "
    type foo
    type bar
    rel foo { 'a' } 'constant'
    rel bar { 'b' } 'other-constant'
    type u // ERROR no uniform discriminator found for union variants
    rel . { foo } u
    rel . { bar } u
    "
    .compile_fail()
}

#[test]
fn non_disjoint_string_union() {
    "
    type u1
    rel . { 'a' } u1
    rel . { 'a' } u1 // ERROR duplicate anonymous relationship
    "
    .compile_fail()
}

#[test]
fn union_tree() {
    "
    type u1
    rel . { '1a' } u1
    rel . { '1b' } u1
    type u2
    rel . { '2a' } u2
    rel . { '2b' } u2
    type u3
    rel . { u1 } u3 // ERROR union tree not supported
    rel . { u2 } u3 // ERROR union tree not supported
    "
    .compile_fail()
}

#[test]
fn sequence_mix1() {
    "
    type u
    rel . { int } u
    rel u { 0 } string // ERROR invalid mix of relationship type for subject
    "
    .compile_fail();
}

#[test]
fn sequence_mix2() {
    "
    type u
    rel u { 'a' } int
    rel u { 0 } string // ERROR invalid mix of relationship type for subject
    "
    .compile_fail();
}

#[test]
fn sequence_overlapping_indices() {
    "
    type u
    rel u { 0..3 } int
    rel u { 2..4 } string // ERROR overlapping indexes
    "
    .compile_fail();
}

#[test]
fn sequence_ambiguous_infinite_tail() {
    r#"
    type u
    rel u { 0.. } int
    rel u { 1.. } string // ERROR overlapping indexes
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
    .s_compile_fail()
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
    .s_compile_fail()
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
            foo { 'prop' :x }
        )
    )
    "
    .s_compile_fail()
}

#[test]
fn eq_attribute_mismatch() {
    "
    (type! foo)
    (type! bar)
    (rel! foo { 'prop' } bar)
    (rel! _ { int } bar)
    (eq! (:x)
        (obj! ;; ERROR missing property `prop`
            foo
            :x ;; ERROR expected named property
        )
        (obj! bar) ;; ERROR expected expression
    )
    "
    .s_compile_fail()
}

#[test]
fn eq_duplicate_unknown_property() {
    "
    (type! foo)
    (type! bar)
    (rel! foo { 'a' } bar)
    (eq! (:x)
        (obj! foo
            { 'a' :x }
            { 'a' :x } ;; ERROR duplicate property
            { 'b' :x } ;; ERROR unknown property
        )
        (obj! bar)
    )
    "
    .s_compile_fail()
}

#[test]
fn eq_type_mismatch() {
    "
    (type! foo)
    (type! bar)
    (rel! foo { 'prop' } string)
    (rel! bar { 'prop' } int)
    (eq! (:x)
        (obj! foo { 'prop' :x })
        (obj! bar
            { 'prop'
                :x ;; ERROR type mismatch: expected `int`, found `string`
            }
        )
    )
    "
    .s_compile_fail_then(|errors| {
        assert_eq!("x", errors[0].span_text);
    })
}

#[test]
fn eq_type_mismatch_in_func() {
    "
    (type! foo)
    (type! bar)
    (rel! foo { 'prop' } string)
    (rel! bar { 'prop' } int)
    (eq! (:x)
        (obj! foo { 'prop' :x })
        (obj! bar
            { 'prop'
                (*
                    :x ;; ERROR type mismatch: expected `int`, found `string`
                    2
                )
            }
        )
    )
    "
    .s_compile_fail()
}

#[test]
fn eq_array_mismatch() {
    "
    (type! foo)
    (rel! foo { 'a'* } string)
    (rel! foo { 'b'* } string)

    (type! bar)
    (rel! bar { 'a' } string)
    (rel! bar { 'b'* } int)

    (eq! (:x :y)
        (obj! foo { 'a' :x } { 'b' :y })
        (obj! bar
            { 'a' :x } ;; ERROR type mismatch: expected `string`, found `string[]`
            { 'b' :y } ;; ERROR type mismatch: expected `int[]`, found `string[]`
        )
    )
    "
    .s_compile_fail();
}

#[test]
fn union_in_named_relationship() {
    "
    (type! foo)
    (rel! foo { 'a' } string)
    (rel! foo { 'a' } int) ;; ERROR union in named relationship is not supported yet. Make a union type instead.
    "
    .s_compile_fail();
}

#[test]
fn test_serde_object_property_not_sugared() {
    "
    (type! foo)
    (type! bar)
    (rel! foo { 'a' 'aa'*: _ } bar) ;; ERROR only entities may have named reverse relationship
    (rel! foo { 'b' 'bb'*: _ } string) ;; ERROR only entities may have named reverse relationship
    "
    .s_compile_fail()
}

#[test]
fn unresolved_transitive_eq() {
    "
    (type! a)
    (type! b)
    (rel! _ { int } a)
    (rel! _ { int } b)

    (type! c)
    (type! d)
    (rel! c { 'p0' } a)
    (rel! d { 'p1' } b)

    (eq! (:x)
        (obj! c { 'p0'
            :x ;; ERROR cannot convert this `a` from `b`: These types are not equated.
        })
        (obj! d { 'p1'
            :x ;; ERROR cannot convert this `b` from `a`: These types are not equated.
        })
    )
    "
    .s_compile_fail();
}

#[test]
fn various_monadic_properties() {
    "
    (type! foo)
    (rel! foo { 'a' } string)
    (default! foo { 'a' } 'default') ;; ERROR parse error: unknown keyword

    (type! bar)
    ; a is either a string or not present
    (rel! bar { 'maybe'? } string)

    ; bar and string may be related via b many times
    (rel! bar { 'array'* } string)

    ; a is either a string or null
    (rel! bar { 'nullable' } string)
    ; FIXME: Should this work?
    (rel! bar { 'nullable' } _) ;; ERROR union in named relationship is not supported yet. Make a union type instead.
    "
    .s_compile_fail()
}

#[test]
fn mix_of_index_and_edge_type() {
    r#"
    (type! foo)
    (type! bar)

    (rel! foo { 0: bar } string) ;; ERROR cannot mix index relation identifiers and edge types
    "#
    .s_compile_fail()
}

#[test]
fn invalid_subject_types() {
    r#"
    (rel!
        'a' ;; ERROR invalid subject type. Must be a domain type, unit, empty sequence or empty string
        { _ }
        string
    )
    "#
    .s_compile_fail()
}

#[test]
fn invalid_relation_chain() {
    r#"
    (rel!
        _ {_}
        _ {_} {_}
        _
        _ ;; ERROR parse error: expected brace
        {_}
    )
    "#
    .s_compile_fail()
}

#[test]
fn spans_are_correct_projected_from_regex_syntax_errors() {
    r#"
    type lol
    rel . { /abc\/(?P<42>.)/ } lol // ERROR invalid regex: invalid capture group character
    "#
    .compile_fail_then(|errors| {
        assert_eq!("4", errors[0].span_text);
    })
}

#[test]
fn complains_about_ambiguous_pattern_based_unions() {
    "
    type foo
    type bar
    type barbar
    type union // ERROR variants of the union have prefixes that are prefixes of other variants

    rel '' { 'foo' } { uuid } foo
    rel '' { 'bar' } { uuid } bar
    rel '' { 'barbar' } { uuid } barbar

    rel . { foo } union
    rel . { bar } union
    rel . { barbar } union
    "
    .compile_fail();
}
