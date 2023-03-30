use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use pretty_assertions::assert_eq;
use test_log::test;

// BUG: This should recognize the `//` comment token
#[test]
fn lex_error() {
    "; // ERROR lex error: illegal character `;`// ERROR lex error: illegal character `;`// ERROR parse error: found `/`, expected one of `use`, `type`, `rel`, `map`, `pub`".compile_fail();
}

#[test]
fn invalid_statement() {
    "foobar // ERROR parse error: found `foobar`, expected one of `use`, `type`, `rel`, `map`, `pub`"
        .compile_fail();
}

#[test]
fn type_parse_error() {
    "type // ERROR parse error: expected identifier".compile_fail();
    "type {} // ERROR parse error: found `{`, expected identifier".compile_fail();
}

#[test]
fn incomplete_statement() {
    "use 'foobar' // ERROR parse error: expected `as`".compile_fail()
}

#[test]
fn underscore_not_allowed_at_start_of_identifier() {
    "type _foo // ERROR parse error: found `_`, expected identifier".compile_fail()
}

#[test]
fn lex_error_recovery_works() {
    "
    type foo
    type bar
    rel foo ['prop'] string
    rel bar ['prop'] int
    map (x) {
        foo {
            rel ['prop'] x
        }
        bar {
            rel ['prop']
                x
                ;; // ERROR lex error: illegal character `;`
                foobar // ERROR undeclared variable
        }
    }
    "
    .compile_fail()
}

#[test]
fn rel_type_not_found() {
    "
    type foo
    rel foo ['bar']
        baz // ERROR type not found
    
    "
    .compile_fail()
}

#[test]
fn duplicate_type() {
    "
    type foo
    type foo // ERROR duplicate type definition
    "
    .compile_fail()
}

#[test]
fn rel_duplicate_anonymous_relation() {
    "
    type foo
    type bar
    rel // ERROR unit type `bar` cannot be part of a union
        foo [or] bar
    rel // ERROR duplicate anonymous relationship
        foo [or] bar
    "
    .compile_fail()
}

#[test]
fn rel_mix_anonymous_and_named() {
    "
    type foo
    type bar
    rel foo [or] bar

    rel // ERROR invalid mix of relationship type for subject
        foo ['foobar'] bar
    "
    .compile_fail()
}

#[test]
fn map_union_unit_type() {
    "
    type foo
    type bar
    type u {
        rel _ [or] foo // ERROR unit type `foo` cannot be part of a union
        rel _ [or] bar // ERROR unit type `bar` cannot be part of a union
    }
    "
    .compile_fail()
}

#[test]
fn map_union_missing_discriminator() {
    "
    type foo
    type bar
    rel foo ['a'] 'constant'
    rel bar ['b'] string
    type u
    rel u [or] foo
    rel u [or] bar // ERROR cannot discriminate type
    "
    .compile_fail()
}

#[test]
fn map_union_non_uniform_discriminators() {
    "
    type foo
    type bar
    rel foo ['a'] 'constant'
    rel bar ['b'] 'other-constant'
    type u { // ERROR no uniform discriminator found for union variants
        rel _ [or] foo
        rel _ [or] bar
    }
    "
    .compile_fail()
}

#[test]
fn non_disjoint_string_union() {
    "
    type u1 {
        rel _ [or] 'a'
        rel _ [or] 'a' // ERROR duplicate anonymous relationship
    }
    "
    .compile_fail()
}

#[test]
fn union_tree() {
    "
    type u1 {
        rel _ [or] '1a'
        rel _ [or] '1b'
    }
    type u2 {
        rel _ [or] '2a'
        rel _ [or] '2b'
    }
    type u3 {
        rel _ [or] u1 // ERROR union tree not supported
        rel _ [or] u2 // ERROR union tree not supported
    }
    "
    .compile_fail()
}

#[test]
fn sequence_mix1() {
    "
    type u {
        rel _ [or] int
        rel _ [0] string // ERROR invalid mix of relationship type for subject
    }
    "
    .compile_fail();
}

#[test]
fn sequence_mix2() {
    "
    type u
    rel u ['a'] int
    rel u [0] string // ERROR invalid mix of relationship type for subject
    "
    .compile_fail();
}

#[test]
fn sequence_overlapping_indices() {
    "
    type u
    rel u [0..3] int
    rel u [2..4] string // ERROR overlapping indexes
    "
    .compile_fail();
}

#[test]
fn sequence_ambiguous_infinite_tail() {
    r#"
    type u
    rel u [0..] int
    rel u [1..] string // ERROR overlapping indexes
    "#
    .compile_fail();
}

#[test]
fn map_undeclared_variable() {
    "
    type foo
    type bar
    map () {
        foo { x } // ERROR undeclared variable
        bar {}
    }
    "
    .compile_fail()
}

#[test]
#[ignore = "there are no functions right now, only infix operators"]
fn map_incorrect_function_arguments() {
    "
    map (x) {
        (+ ;; ERROR function takes 2 parameters, but 1 was supplied
            x
        )
        42
    }
    "
    .compile_fail()
}

#[test]
fn map_obj_non_domain_type_and_unit_type() {
    "
    type foo
    map (x) {
        number {} // ERROR expected domain type
        foo { // ERROR no properties expected
            rel ['prop'] x
        }
    }
    "
    .compile_fail()
}

#[test]
fn map_attribute_mismatch() {
    "
    type foo
    type bar
    rel foo ['prop'] bar
    rel bar [or] int
    map (x) {
        foo { // ERROR missing property `prop`
            x // ERROR expected named property
        }
        bar {} // ERROR expected expression
    }
    "
    .compile_fail()
}

#[test]
fn map_duplicate_unknown_property() {
    "
    type foo
    type bar
    rel foo ['a'] bar
    map (x) {
        foo {
            rel ['a'] x
            rel ['a'] x // ERROR duplicate property
            rel ['b'] x // ERROR unknown property
        }
        bar {}
    }
    "
    .compile_fail()
}

#[test]
fn map_type_mismatch() {
    "
    type foo
    type bar
    rel foo ['prop'] string
    rel bar ['prop'] int
    map (x) {
        foo {
            rel ['prop'] x
        }
        bar {
            rel ['prop']
                x // ERROR type mismatch: expected `int`, found `string`
        }
    }
    "
    .compile_fail_then(|errors| {
        assert_eq!("x", errors[0].span_text);
    })
}

#[test]
fn map_type_mismatch_in_func() {
    "
    type foo
    type bar
    rel foo ['prop'] string
    rel bar ['prop'] int
    map (x) {
        foo {
            rel ['prop'] x
        }
        bar {
            rel ['prop']
                x // ERROR type mismatch: expected `int`, found `string`
                * 2
        }
    }
    "
    .compile_fail()
}

#[test]
fn map_array_mismatch() {
    "
    type foo
    rel foo ['a'*] string
    rel foo ['b'*] string

    type bar
    rel bar ['a'] string
    rel bar ['b'*] int

    map (x y) {
        foo {
            rel ['a'] x
            rel ['b'] y
        }
        bar {
            rel ['a'] x // ERROR type mismatch: expected `string`, found `string[]`
            rel ['b'] y // ERROR type mismatch: expected `int[]`, found `string[]`
        }
    }
    "
    .compile_fail();
}

#[test]
fn union_in_named_relationship() {
    "
    type foo
    rel foo ['a'] string
    rel foo ['a'] int // ERROR union in named relationship is not supported yet. Make a union type instead.
    "
    .compile_fail();
}

#[test]
fn test_serde_object_property_not_sugared() {
    "
    type foo
    type bar
    rel foo ['a' | 'aa'*: ()] bar // ERROR only entities may have named reverse relationship
    rel foo ['b' | 'bb'*: ()] string // ERROR only entities may have named reverse relationship
    "
    .compile_fail()
}

#[test]
fn unresolved_transitive_map() {
    "
    type a { rel _ [or] int }
    type b { rel _ [or] int }

    type c { rel _ ['p0'] a }
    type d { rel _ ['p1'] b }

    map (x) {
        c {
            rel ['p0']
                x // ERROR cannot convert this `a` from `b`: These types are not equated.
        }
        d {
            rel ['p1']
                x // ERROR cannot convert this `b` from `a`: These types are not equated.
        }
    }
    "
    .compile_fail();
}

#[test]
fn various_monadic_properties() {
    "
    type foo
    rel foo ['a'] string
    // default foo ['a'] 'default'

    type bar
    // a is either a string or not present
    rel bar ['maybe'?] string

    // bar and string may be related via b many times
    rel bar ['array'*] string

    // a is either a string or null
    rel bar ['nullable'] string
    // FIXME: Should this work?
    rel bar ['nullable'] () // ERROR union in named relationship is not supported yet. Make a union type instead.
    "
    .compile_fail()
}

#[test]
fn mix_of_index_and_edge_type() {
    r#"
    type foo
    type bar

    rel foo [0: bar] string // ERROR cannot mix index relation identifiers and edge types
    "#
    .compile_fail()
}

#[test]
fn invalid_subject_types() {
    "
    rel
        'a' // ERROR invalid subject type. Must be a domain type, empty sequence or empty string
        [()]
        string
    "
    .compile_fail()
}

#[test]
fn invalid_relation_chain() {
    "
    rel
        () [()]
        () [()] [()]
        ()
        () // ERROR parse error: found `(`, expected one of `[`, `{`, `type`, `rel`, `map`, `pub`
        [()]
    "
    .compile_fail()
}

#[test]
fn spans_are_correct_projected_from_regex_syntax_errors() {
    r#"
    type lol
    rel () [/abc\/(?P<42>.)/] lol // ERROR invalid regex: invalid capture group character
    "#
    .compile_fail_then(|errors| {
        assert_eq!("4", errors[0].span_text);
    })
}

#[test]
fn complains_about_non_disambiguatable_string_id() {
    "
    type animal_id { rel '' [string] _ }
    type plant_id { rel '' [string] _ }
    type animal {
        rel animal_id [identifies] _
        rel _ ['class'] 'animal'
    }
    type plant {
        rel plant_id [identifies] _
        rel _ ['class'] 'plant'
    }
    type lifeform { // ERROR entity variants of the union are not uniquely identifiable
        rel _ [or] animal
        rel _ [or] plant
    }
    "
    .compile_fail();
}

#[test]
fn complains_about_ambiguous_pattern_based_unions() {
    "
    type foo
    type bar
    type barbar
    type union // ERROR variants of the union have prefixes that are prefixes of other variants

    rel '' ['foo'] [uuid] foo
    rel '' ['bar'] [uuid] bar
    rel '' ['barbar'] [uuid] barbar

    rel union [or] foo
    rel union [or] bar
    rel union [or] barbar
    "
    .compile_fail();
}

#[test]
fn compile_error_in_dependency() {
    TestPackages::with_sources([
        (
            SourceName("fail"),
            "
            ! // ERROR parse error: found `!`, expected one of `use`, `type`, `rel`, `map`, `pub`
            ",
        ),
        (SourceName::root(), "use 'fail' as f"),
    ])
    .compile_fail();
}

#[test]
fn fail_import_private_type() {
    TestPackages::with_sources([
        (SourceName("dep"), "type foo"),
        (
            SourceName::root(),
            "
            use 'dep' as dep
            pub type bar {
                rel _ ['foo'] dep.foo // ERROR private type
            }
            ",
        ),
    ])
    .compile_fail();
}

#[test]
fn namespace_not_found() {
    "
    type foo {
        rel _ ['prop']
            dep // ERROR namespace not found
            .foo
    }
    "
    .compile_fail();
}
