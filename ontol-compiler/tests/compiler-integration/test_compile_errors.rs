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
    (rel! (foo) bar
        (baz) ;; ERROR type not found
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
        (foo) _ (bar)
    )
    (rel! ;; ERROR duplicate anonymous relationship
        (foo) _ (bar)
    )
    "
    .compile_fail()
}

#[test]
fn rel_mix_anonymous_and_named() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) _ (bar))
    (rel! ;; ERROR cannot mix named and anonymous relations on the same type
        (foo) foobar (bar)
    )
    "
    .compile_fail()
}

#[test]
fn rel_array_range_without_start_or_end() {
    "
    (type! foo)
    (rel!
        (foo)
        n[..] ;; ERROR parse error: expected array range constraint
        (int)
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
    (rel! (u) _ (foo)) ;; ERROR unit type `foo` cannot be part of a union
    (rel! (u) _ (bar)) ;; ERROR unit type `bar` cannot be part of a union
    "
    .compile_fail()
}

#[test]
fn map_union_missing_discriminator() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) a "constant")
    (rel! (bar) b (string))
    (type! u)
    (rel! (u) _ (foo))
    (rel! (u) _ (bar)) ;; ERROR cannot discriminate type
    "#
    .compile_fail()
}

#[test]
fn map_union_non_uniform_discriminators() {
    r#"
    (type! foo)
    (type! bar)
    (rel! (foo) a "constant")
    (rel! (bar) b "other-constant")
    (type! u) ;; ERROR no uniform discriminator found for union variants
    (rel! (u) _ (foo))
    (rel! (u) _ (bar))
    "#
    .compile_fail()
}

#[test]
fn non_disjoint_string_union() {
    r#"
    (type! u1)
    (rel! (u1) _ "a")
    (rel! (u1) _ "a") ;; ERROR duplicate anonymous relationship
    "#
    .compile_fail()
}

#[test]
fn union_tree() {
    r#"
    (type! u1)
    (rel! (u1) _ "1a")
    (rel! (u1) _ "1b")
    (type! u2)
    (rel! (u2) _ "2a")
    (rel! (u2) _ "2b")
    (type! u3)
    (rel! (u3) _ (u1)) ;; ERROR union tree not supported
    (rel! (u3) _ (u2)) ;; ERROR union tree not supported
    "#
    .compile_fail()
}

#[test]
fn tuple_in_union() {
    r#"
    (type! u)
    (rel! (u) _ (tuple! "a")) ;; ERROR cannot discriminate type
    (rel! (u) _ "b")
    "#
    .compile_fail();
}

#[test]
fn no_need_to_support_differing_cardinality_in_unions_at_least_yet() {
    r#"
    (type! u)
    (rel! (u) _ (int))
    (rel! (u) _[] (string)) ;; ERROR invalid cardinality combination in union
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
    (rel! (foo) prop (bar))
    (rel! (bar) _ (int))
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
    (rel! (foo) a (bar))
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
    (rel! (foo) prop (string))
    (rel! (bar) prop (int))
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
    (rel! (foo) prop (string))
    (rel! (bar) prop (int))
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
    r#"
    (type! foo)
    (rel! (foo) a[] (string))
    (rel! (foo) b[] (string))

    (type! bar)
    (rel! (bar) a (string))
    (rel! (bar) b[] (int))

    (eq! (:x :y)
        (obj! foo (a :x) (b :y))
        (obj! bar
            (a :x) ;; ERROR type mismatch: expected `string`, found `string[]`
            (b :y) ;; ERROR type mismatch: expected `int[]`, found `string[]`
        )
    )
    "#
    .compile_fail();
}

#[test]
fn union_in_named_relationship() {
    r#"
    (type! foo)
    (rel! (foo) a (string))
    (rel! (foo) a (int)) ;; ERROR union in named relationship is not supported yet. Make a union type instead.
    "#
    .compile_fail();
}

#[test]
fn test_serde_object_property_not_sugared() {
    "
    (type! foo)
    (type! bar)
    (rel! (foo) a @(unit) aa[] (bar)) ;; ERROR only entities may have named reverse relationship
    (rel! (foo) b @(unit) bb[] (string)) ;; ERROR only entities may have named reverse relationship
    "
    .compile_fail()
}

#[test]
fn unresolved_transitive_eq() {
    r#"
    (type! a)
    (type! b)
    (rel! (a) _ (int))
    (rel! (b) _ (int))

    (type! c)
    (type! d)
    (rel! (c) p0 (a))
    (rel! (d) p1 (b))

    (eq! (:x)
        (obj! c (p0
            :x ;; ERROR cannot convert this `a` from `b`: These types are not equated.
        ))
        (obj! d (p1
            :x ;; ERROR cannot convert this `b` from `a`: These types are not equated.
        ))
    )
    "#
    .compile_fail();
}

#[test]
fn various_monadic_properties() {
    r#"
    (type! foo)
    (rel! (foo) a (string))
    (default! (foo) a "default") ;; ERROR parse error: unknown keyword

    (type! bar)
    ; a is either a string or not present
    (rel! (bar) maybe? (string))

    ; bar and string may be related via b many times
    (rel! (bar) array[] (string))

    ; bar and string may be related via c many times, minimum 1
    (rel! (bar) maybe_array[1..]? (string))

    ; a is either a string or null
    (rel! (bar) nullable (string))
    ; FIXME: Should this work?
    (rel! (bar) nullable (null)) ;; ERROR union in named relationship is not supported yet. Make a union type instead.
    "#
    .compile_fail()
}

#[test]
fn mix_of_index_and_edge_type() {
    r#"
    (type! foo)
    (type! bar)

    (rel! (foo) 0 @(bar) (string)) ;; ERROR cannot mix index relation identifiers and edge types
    "#
    .compile_fail()
}
