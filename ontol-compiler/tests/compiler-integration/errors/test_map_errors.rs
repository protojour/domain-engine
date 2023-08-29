use ontol_test_utils::{expect_eq, TestCompile};
use test_log::test;

#[test]
#[ignore = "there are no functions right now, only infix operators"]
fn map_incorrect_function_arguments() {
    "
    map {
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
    def foo
    map {
        number {} // ERROR expected domain type
        foo { // ERROR no properties expected
            'prop': x
        }
    }
    "
    .compile_fail()
}

#[test]
fn map_attribute_mismatch() {
    "
    def foo
    def bar
    rel foo 'prop0': bar
    rel foo 'prop1': bar
    rel foo 'prop2': bar
    rel bar is: i64
    map {
        foo: // ERROR expected named property// ERROR missing properties `prop0`, `prop1`, `prop2`// NOTE Consider using `match {}`
            x
        bar {} // ERROR expected expression attribute
    }
    "
    .compile_fail()
}

#[test]
fn map_missing_attributes_in_match_is_ok() {
    "
    def foo { rel .'a'|'b': string }
    def bar { rel .'c'|'d': string }
    map {
        foo match { 'a': x }
        bar { 'c': x } // ERROR missing property `d`// NOTE Consider using `match {}`
    }
    "
    .compile_fail()
}

#[test]
fn map_duplicate_unknown_property() {
    "
    def foo
    def bar
    rel foo 'a': bar
    map {
        foo {
            'a': x
            'a': x // ERROR duplicate property
            'b': x // ERROR unknown property
        }
        bar {}
    }
    "
    .compile_fail()
}

#[test]
fn map_type_mismatch_simple() {
    "
    def foo
    def bar
    rel foo 'prop': string
    rel bar 'prop': i64
    map {
        foo {
            'prop': x
        }
        bar {
            'prop':
                x // ERROR type mismatch: expected `i64`, found `string`
        }
    }
    "
    .compile_fail_then(|errors| {
        expect_eq!(actual = errors[0].span_text, expected = "x");
    })
}

#[test]
fn map_type_mismatch_in_func() {
    "
    def foo
    def bar
    rel foo 'prop': string
    rel bar 'prop': i64
    map {
        foo {
            'prop': x
        }
        bar {
            'prop':
                x // ERROR type mismatch: expected `i64`, found `string`
                * 2
        }
    }
    "
    .compile_fail()
}

#[test]
fn map_sequence_mismatch() {
    "
    def foo
    rel foo 'a': [string]
    rel foo 'b': [string]

    def bar
    rel bar 'a': string
    rel bar 'b': [i64]

    map {
        foo {
            'a': x // ERROR [string] variable must be enclosed in []
            'b': y // ERROR [string] variable must be enclosed in []
        }
        bar {
            'a': x
            'b': y // ERROR [i64] variable must be enclosed in []
        }
    }
    "
    .compile_fail();
}

#[test]
fn array_map_without_brackets() {
    "
    def foo { rel .'a': [string] }
    def bar { rel .'b': [string] }

    map {
        foo {
            'a': x // ERROR [string] variable must be enclosed in []
        }
        bar {
            'b': x // ERROR [string] variable must be enclosed in []
        }
    }
    "
    .compile_fail();
}

#[test]
fn only_entities_may_have_reverse_relationship() {
    "
    def foo
    def bar
    rel [foo] 'a'()::'aa' bar // ERROR only entities may have named reverse relationship
    rel [foo] 'b'::'bb' string // ERROR only entities may have named reverse relationship
    "
    .compile_fail()
}

#[test]
fn unresolved_transitive_map() {
    "
    def a { rel .is?: i64 }
    def b { rel .is?: i64 }

    def c { rel .'p0': a }
    def d { rel .'p1': b }

    map {
        c {
            'p0':
                x // ERROR cannot convert this `a` from `b`: These types are not equated.
        }
        d {
            'p1':
                x // ERROR cannot convert this `b` from `a`: These types are not equated.
        }
    }
    "
    .compile_fail();
}

#[test]
fn map_union() {
    "
    def foo {
        rel .'type': 'foo'
    }
    def bar {
        rel .'type': 'bar'
    }
    def foobar {
        rel .is?: foo
        rel .is?: bar
    }

    map {
        foobar {} // ERROR cannot map a union, map each variant instead
        foo {} // ERROR missing property `type`// NOTE Consider using `match {}`
    }
    "
    .compile_fail();
}

#[test]
fn map_invalid_unit_rel_params() {
    "
    def foo { rel .'foo': string }
    def bar { rel .'bar': string }

    map {
        foo {
            'foo'
                ('bug': b) // ERROR no relation parameters expected
                : s
        }
        bar { 'bar': s }
    }
    "
    .compile_fail();
}
