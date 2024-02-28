use ontol_test_utils::{expect_eq, TestCompile};
use test_log::test;

#[test]
#[ignore = "there are no functions right now, only infix operators"]
fn map_incorrect_function_arguments() {
    "
    map (
        (+ ;; ERROR function takes 2 parameters, but 1 was supplied
            x
        ),
        42,
    )
    "
    .compile_fail();
}

#[test]
fn map_obj_non_domain_type_and_unit_type() {
    "
    def foo ()
    map (
        number (), // ERROR expected domain type
        foo ( // ERROR no properties expected
            'prop': x
        )
    )
    "
    .compile_fail();
}

#[test]
fn map_attribute_mismatch() {
    "
    def foo ()
    def bar ()
    rel foo 'prop0': bar
    rel foo 'prop1': bar
    rel foo 'prop2': bar
    rel bar is: i64
    map (
        foo: x,
        bar () // ERROR expected attribute
    )
    "
    .compile_fail();
}

#[test]
fn map_missing_attributes_in_match_is_ok() {
    "
    def foo ( rel .'a'|'b': text )
    def bar ( rel .'c'|'d': text )
    map (
        foo match( 'a': x ),
        bar( 'c': x ) // ERROR missing property `d`// NOTE Consider using `match {}`
    )
    "
    .compile_fail();
}

#[test]
fn map_duplicate_unknown_property() {
    "
    def foo ()
    def bar ()
    rel foo 'a': bar
    map(
        foo(
            'a': x,
            'a': x, // ERROR duplicate property
            'b': x, // ERROR unknown property
        ),
        bar()
    )
    "
    .compile_fail();
}

#[test]
fn map_type_mismatch_simple() {
    "
    def foo ()
    def bar ()
    rel foo 'prop': text
    rel bar 'prop': i64
    map(
        foo(
            'prop': x
        ),
        bar(
            'prop':
                x // ERROR type mismatch: expected `i64`, found `text`
        )
    )
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "x");
    })
}

#[test]
fn map_type_mismatch_in_func() {
    "
    def foo ()
    def bar ()
    rel foo 'prop': text
    rel bar 'prop': i64
    map(
        foo('prop': x),
        bar(
            'prop':
                x // ERROR type mismatch: expected `i64`, found `text`
                * 2
        )
    )
    "
    .compile_fail();
}

#[test]
fn map_sequence_mismatch() {
    "
    def foo (
        rel .'a': {text}
        rel .'b': {text}
    )

    def bar (
        rel .'a': text
        rel .'b': {i64}
    )

    map(
        foo(
            'a': x, // ERROR {text} variable must be enclosed in {}
            'b': y, // ERROR {text} variable must be enclosed in {}
        ),
        bar(
            'a': x,
            'b': y, // ERROR {i64} variable must be enclosed in {}
        ),
    )
    "
    .compile_fail();
}

#[test]
fn array_map_without_braces() {
    "
    def foo (rel .'a': {text})
    def bar (rel .'b': {text})

    map(
        foo(
            'a': x // ERROR {text} variable must be enclosed in {}
        ),
        bar(
            'b': x // ERROR {text} variable must be enclosed in {}
        ),
    )
    "
    .compile_fail();
}

#[test]
fn only_entities_may_have_reverse_relationship() {
    "
    def foo ()
    def bar ()
    rel {foo} 'a'[]::'aa' bar // ERROR only entities may have named reverse relationship
    rel {foo} 'b'::'bb' text // ERROR only entities may have named reverse relationship
    "
    .compile_fail();
}

#[test]
fn unresolved_transitive_map() {
    "
    def a (rel .is?: i64)
    def b (rel .is?: i64)

    def c (rel .'p0': a)
    def d (rel .'p1': b)

    map(
        c(
            'p0':
                x // ERROR cannot convert this `a` from `b`: These types are not equated.
        ),
        d(
            'p1':
                x // ERROR cannot convert this `b` from `a`: These types are not equated.
        )
    )
    "
    .compile_fail();
}

#[test]
fn map_union() {
    "
    def foo (
        rel .'type': 'foo'
    )
    def bar (
        rel .'type': 'bar'
    )
    def foobar (
        rel .is?: foo
        rel .is?: bar
    )

    map(
        foobar(), // ERROR cannot map a union, map each variant instead
        foo(), // ERROR missing property `type`// NOTE Consider using `match {}`
    )
    "
    .compile_fail();
}

#[test]
fn map_invalid_unit_rel_params() {
    "
    def foo (rel .'foo': text)
    def bar (rel .'bar': text)

    map(
        foo(
            'foo'
                ['bug': b] // ERROR no relation parameters expected
                : s
        ),
        bar('bar': s)
    )
    "
    .compile_fail();
}

#[test]
fn map_duplicate_capture_groups_in_regex() {
    r"
    def a (rel .'a': text)
    def b ()
    map(
        a(
            'a': /(?<dupe>\w+) (?<dupe>\w+)!/ // ERROR invalid regex: duplicate capture group name
        ),
        b()
    )
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "dupe");
    })
}

#[test]
fn map_unbound_variable_in_regex_interpolation() {
    r"
    def a ()
    def b (rel .'b': text)
    map(
        a(),
        b(
            'b': /(?<bad_var>\w+)!/ // ERROR unbound variable
        )
    )
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "bad_var");
    })
}

#[test]
fn map_error_def_inference_ambiguity() {
    r"
    def foo (
        rel .'a': text
        rel .'b': text
    )
    map(
        (
            'a': x, // ERROR TODO: Inference failed: Variable is mentioned more than once in the opposing arm// ERROR unknown property
            'b': y, // ERROR TODO: Inference failed: Corresponding variable not found// ERROR unknown property
        ),
        foo('a': x, 'b': x)
    )
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "x");
        expect_eq!(actual = errors[1].span_text, expected = "'a'");
    })
}

#[test]
fn map_error_inference_cardinality_mismatch() {
    r"
    def foo (
        rel .'a': text
        rel .'b': text
    )
    map(
        ('a'?: x), // ERROR cardinality mismatch
        foo match('a': x)
    )
    "
    .compile_fail();
}

#[test]
fn must_bind_as_option_if_default_cannot_be_constructed() {
    "
    def outer1(rel .'a'?: inner1)
    def outer2(rel .'a'?: inner2)
    def inner1(rel .'b': text)
    def inner2(rel .'b': text)

    map(
        outer1(
            'a': // ERROR TODO: optional binding required, as a default value cannot be created
                inner1('b': x)
        ),
        outer2('a': inner2('b': x)), // ERROR TODO: optional binding required, as a default value cannot be created
    )
    "
    .compile_fail();
}

#[test]
fn autogen_primary_ids_must_be_optional() {
    "
    def a(rel .'id'[rel .gen: auto]|id: (rel .is: text))
    def b(rel .'id'[rel .gen: auto]|id: (rel .is: text))
    map(
        a('id': id), // ERROR TODO: optional binding required, as a default value cannot be created
        b('id': id), // ERROR TODO: optional binding required, as a default value cannot be created
    )
    "
    .compile_fail();
}

// TODO: This should perhaps compile?
#[test]
fn error_non_iterated_variable() {
    "
    def foo (
        rel .'p0': {text}
    )
    def bar (
        rel .'p1': {text}
    )

    map(
        foo(
            'p0': {x} // ERROR pattern requires an iterated variable (`..x`)
        ),
        bar(
            'p1': {x} // ERROR pattern requires an iterated variable (`..x`)
        )
    )
    "
    .compile_fail();
}

#[test]
fn map_error_unsolvable_equation() {
    "
    def foo (
        rel .'a': i64
        rel .'b': i64
    )
    def bar (
        rel .'a': i64
        rel .'b': i64
    )
    map(
        foo(
            'a': x + y, // ERROR unsolvable equation
            'b': y * x // ERROR unsolvable equation
        ),
        bar(
            'a': x,
            'b': y
        )
    )
    "
    .compile_fail();
}

#[test]
fn map_error_duplicate_in_equation() {
    "
    def foo (rel .'a': i64)
    def bar (rel .'a': i64)
    map(
        foo(
            'a':
                x // ERROR unsupported variable duplication. Try to simplify the expression
                +
                x // ERROR unsupported variable duplication. Try to simplify the expression
        ),
        bar(
            'a': x
        )
    )
    "
    .compile_fail();
}
