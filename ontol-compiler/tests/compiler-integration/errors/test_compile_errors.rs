use ontol_test_utils::{expect_eq, SourceName, TestCompile, TestPackages};
use test_log::test;

// BUG: This should recognize the `//` comment token
#[test]
fn error_lex() {
    "; // ERROR lex error: illegal character `;`// ERROR lex error: illegal character `;`// ERROR parse error: found `/`, expected one of `use`, `def`, `rel`, `fmt`, `map`"
        .compile_fail();
}

#[test]
fn error_comment_span() {
    // This tests that the eror span is correct
    r#"
    /// A comment - don't remove this
    def(pub) union {} // ERROR variants of the union have prefixes that are prefixes of other variants

    def a { fmt '' => 'foo' => . }
    def b { fmt '' => 'foobar' => . }

    rel union is?: a
    rel union is?: b
    "#
    .compile_fail();
}

#[test]
fn error_invalid_statement() {
    "foobar // ERROR parse error: found `foobar`, expected one of `use`, `def`, `rel`, `fmt`, `map`"
        .compile_fail();
}

#[test]
fn error_def_parse_error() {
    "def // ERROR parse error: expected `(`".compile_fail();
    "def {} // ERROR parse error: found `{`, expected `(`".compile_fail();
}

#[test]
fn error_incomplete_statement() {
    "use 'foobar' // ERROR parse error: expected `as`".compile_fail();
}

#[test]
fn error_underscore_not_allowed_at_start_of_identifier() {
    "def _foo // ERROR parse error: found `_`, expected `(`".compile_fail();
}

#[test]
fn error_lex_recovery_works() {
    "
    def foo {
        rel .'prop': text
    }
    def bar {
        rel .'prop': integer
    }
    map {
        foo {
            'prop': x
        }
        bar {
            'prop':
                x
                ;; // ERROR lex error: illegal character `;`
                foobar
        } // ERROR parse error: found `}`, expected one of `(`, `.`, `:`, `?`
    }
    "
    .compile_fail();
}

#[test]
fn error_rel_needs_a_triple() {
    "
    def a {
        rel 'b': i64 // ERROR parse error: found `:`, expected one of `(`, `{`, `..`
    }
    "
    .compile_fail();
}

#[test]
fn error_rel_type_not_found() {
    "
    def foo {}
    rel foo 'bar':
        baz // ERROR type not found
    "
    .compile_fail();
}

#[test]
fn error_rel_duplicate_anonymous_relation() {
    "
    def foo {}
    def bar {}
    rel // ERROR unit type `bar` cannot be part of a union
        foo is?: bar
    rel // ERROR duplicate anonymous relationship
        foo is?: bar
    "
    .compile_fail();
}

#[test]
fn error_duplicate_map_identifier() {
    "
    def t1 {}
    def t2 {}
    def t3 {}
    // note: map has a separate namespace:
    map t1 { t1 {} t2 {}}
    map t1 { // ERROR duplicate map identifier
        t2 {} t3 {}
    }
    "
    .compile_fail();
}

#[test]
fn error_map_union_unit_type() {
    "
    def foo {}
    def bar {}
    def u {
        rel .is?: foo // ERROR unit type `foo` cannot be part of a union
        rel .is?: bar // ERROR unit type `bar` cannot be part of a union
    }
    "
    .compile_fail();
}

#[test]
fn error_map_union_missing_discriminator() {
    "
    def foo {
        rel .'a': 'constant'
    }
    def bar {
        rel .'b': text
    }
    def u {
        rel .is?: foo
        rel .is?: bar // ERROR cannot discriminate type
    }
    "
    .compile_fail();
}

#[test]
fn error_map_union_non_uniform_discriminators() {
    "
    def foo {
        rel .'a': 'constant'
    }
    def bar {
        rel .'b': 'other-constant'
    }
    def u { // ERROR no uniform discriminator found for union variants
        rel .is?: foo
        rel .is?: bar
    }
    "
    .compile_fail();
}

#[test]
fn error_non_disjoint_text_union() {
    "
    def u1 {
        rel .is?: 'a'
        rel .is?: 'a' // ERROR duplicate anonymous relationship
    }
    "
    .compile_fail();
}

#[test]
fn error_sequence_mix1() {
    "
    def u {
        rel .is?: i64 // ERROR invalid mix of relationship type for subject
        rel .0: text
    }
    "
    .compile_fail();
}

#[test]
fn error_sequence_mix_abstract_object() {
    "
    def u { // ERROR type not representable
        rel .'a':
            integer // NOTE Type of field is abstract
        rel .0: text // ERROR invalid mix of relationship type for subject
    }
    "
    .compile_fail();
}

#[test]
fn sequence_overlapping_indices() {
    "
    def u {
        rel .0..3: i64
        rel .2..4: text // ERROR overlapping indexes
    }
    "
    .compile_fail();
}

#[test]
fn error_sequence_ambiguous_infinite_tail() {
    r#"
    def u {
        rel .0..: i64
        rel .1..: text // ERROR overlapping indexes
    }
    "#
    .compile_fail();
}

#[test]
fn error_union_in_named_relationship() {
    "
    def foo {
        rel .'a': text
        rel .'a': i64 // ERROR union in named relationship is not supported yet. Make a union instead.
    }
    "
    .compile_fail();
}

#[test]
fn error_various_monadic_properties() {
    "
    def foo {
        rel .'a': text
    }
    // default foo 'a': 'default'

    def bar {
        // a is either a text or not present
        rel .'maybe'?: text

        // bar and string may be related via b many times
        rel .'array': [text]

        // a is either a text or null
        rel bar 'nullable': text

        // FIXME: Should this work?
        rel .'nullable': () // ERROR union in named relationship is not supported yet. Make a union instead.
    }
    "
    .compile_fail();
}

#[test]
fn error_mix_of_index_and_edge_type() {
    r#"
    def foo {}
    def bar {}

    rel foo 0(rel .is: bar): text // ERROR cannot mix index relation identifiers and edge types
    "#
    .compile_fail();
}

#[test]
fn error_invalid_subject_types() {
    "
    rel
        'a' // ERROR subject must be a domain type
        'b': text
    "
    .compile_fail();
}

#[test]
fn error_invalid_relation_type() {
    "
    def foo {}
    def bar {}
    rel foo
        uuid: // ERROR invalid relation type
        bar
    "
    .compile_fail();
}

#[test]
fn error_invalid_fmt_syntax() {
    "
    fmt () => () () // ERROR parse error: found `(`, expected one of `def`, `rel`, `fmt`, `map`, `=>`
    "
    .compile_fail();
}

#[test]
fn error_invalid_fmt_semantics() {
    "
    fmt () // ERROR fmt needs at least two transitions: `fmt a => b => c`
    fmt () => () // ERROR fmt needs at least two transitions: `fmt a => b => c`
    fmt () => . => 'bar' // ERROR fmt only supports `.` at the final target position
    "
    .compile_fail();
}

#[test]
fn error_spans_are_correct_projected_from_regex_syntax_errors() {
    r"
    def lol {}
    rel () /abc\/(?P<42>.)/: lol // ERROR invalid regex: invalid capture group character
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "4");
    })
}

#[test]
fn error_complains_about_non_disambiguatable_text_id() {
    "
    def animal_id { fmt '' => text => . }
    def plant_id { fmt '' => text => . }
    def animal {
        rel animal_id identifies: .
        rel .'class': 'animal'
    }
    def plant {
        rel plant_id identifies: .
        rel .'class': 'plant'
    }
    def lifeform { // ERROR entity variants of the union are not uniquely identifiable
        rel .is?: animal
        rel .is?: plant
    }
    "
    .compile_fail();
}

#[test]
fn error_complains_about_ambiguous_pattern_based_unions() {
    "
    def foo {
        fmt '' => 'foo' => uuid => .
    }
    def bar {
        fmt '' => 'bar' => uuid => .
    }
    def barbar {
        fmt '' => 'barbar' => uuid => .
    }
    def union { // ERROR variants of the union have prefixes that are prefixes of other variants
        rel .is?: foo
        rel .is?: bar
        rel .is?: barbar
    }
    "
    .compile_fail();
}

#[test]
fn error_compile_error_in_dependency() {
    TestPackages::with_sources([
        (
            SourceName("fail"),
            "
            ! // ERROR parse error: found `!`, expected one of `use`, `def`, `rel`, `fmt`, `map`
            ",
        ),
        (SourceName::root(), "use 'fail' as f"),
    ])
    .compile_fail();
}

#[test]
fn error_rel_wildcard_span() {
    "
    rel integer // ERROR definition is sealed and cannot be modified
        'likes': integer
    "
    .compile_fail();
}

#[test]
fn error_fail_import_private_type() {
    TestPackages::with_sources([
        (SourceName("dep"), "def foo {}"),
        (
            SourceName::root(),
            "
            use 'dep' as dep
            def(pub) bar {
                rel . 'foo': dep.foo // ERROR private definition
            }
            ",
        ),
    ])
    .compile_fail();
}

#[test]
fn error_domain_named_relation() {
    TestPackages::with_sources([
        (SourceName("dep0"), ""),
        (SourceName("dep1"), ""),
        (
            SourceName::root(),
            "
            use 'dep0' as dep0
            use 'dep1' as dep1

            rel dep0 'fond_of': dep1 // ERROR subject must be a domain type// ERROR object must be a data type
            ",
        ),
    ])
    .compile_fail();
}

#[test]
fn error_namespace_not_found() {
    "
    def foo {
        rel .'prop':
            dep // ERROR namespace not found
            .foo
    }
    "
    .compile_fail();
}

#[test]
fn error_constant_in_weird_place() {
    "
    def foo {
        rel .'prop' := 42 // ERROR Incompatible literal// ERROR object must be a data type
    }
    "
    .compile_fail();
}

#[test]
fn bad_domain_relation() {
    TestPackages::with_sources([
        (SourceName("a"), ""),
        (SourceName("b"), ""),
        (
            SourceName::root(),
            "
            use 'a' as a
            use 'b' as b

            rel a 'to'(): b // ERROR subject must be a domain type// ERROR object must be a data type
            ",
        ),
    ])
    .compile_fail();
}

#[test]
fn error_value_generator_as_field_type() {
    "def foo { rel .'prop': auto } // ERROR object must be a data type// ERROR type not representable// NOTE Type of field is abstract".compile_fail();
}

#[test]
fn error_nonsense_value_generator() {
    "
    def bar { rel .'prop': text }
    def foo {
        rel .'bar'
            (rel .gen: auto) // ERROR Cannot generate a value of type bar
        : bar
    }
    "
    .compile_fail();
}

#[test]
fn error_test_lazy_seal_by_map() {
    "
    def foo { rel .'prop': text }
    def bar { rel .'prop': text }

    map {
        foo { 'prop': prop }
        bar { 'prop': prop }
    }

    rel
        foo // ERROR definition is sealed and cannot be modified
        'fail': text
    "
    .compile_fail();
}

#[test]
fn error_test_error_object_property_in_foreign_domain() {
    TestPackages::with_sources([
        (SourceName("foreign"), "def(pub) foo {}"),
        (
            SourceName::root(),
            "
            use 'foreign' as foreign

            def(pub) bar {
                rel .'foo': foreign.foo // This is OK
            }

            def(pub) baz {
                rel .'foo'::'baz'
                    foreign.foo // ERROR definition is sealed and cannot be modified
            }
            ",
        ),
    ])
    .compile_fail();
}

#[test]
fn error_ambiguous_number_resolution() {
    "
    def a {
        rel .is: float // NOTE Base type is float
    }
    def b {
        rel .is: i64 // NOTE Base type is integer
    }
    def c { // ERROR ambiguous number resolution
        rel .is: a
        rel .is: b
    }
    "
    .compile_fail();
}
