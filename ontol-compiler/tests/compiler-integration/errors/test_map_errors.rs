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
    type foo
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
    type foo
    type bar
    rel foo 'prop0': bar
    rel foo 'prop1': bar
    rel foo 'prop2': bar
    rel bar is: int
    map { // NOTE Consider using a one way mapping (`map => { .. }`) here
        foo: // ERROR expected named property// ERROR missing properties `prop0`, `prop1`, `prop2`
            x
        bar {} // ERROR expected expression attribute
    }
    "
    .compile_fail()
}

#[test]
fn map_missing_property_suggest_one_way() {
    "
    type foo { rel .'a'|'b'|'c': string }
    type bar { rel .'d': string }
    map { // NOTE Consider using a one way mapping (`map => { .. }`) here
        foo { 'a': x } // ERROR missing properties `b`, `c`
        bar { 'd': x }
    }
    "
    .compile_fail()
}

/// Tests that the "consider using one way mapping" does not appear if it's already a one-way mapping
#[test]
fn map_missing_attributes_one_way() {
    "
    type foo { rel .'a'|'b': string }
    type bar { rel .'c'|'d': string }
    map => {
        foo { 'a': x }
        bar { 'c': x } // ERROR missing property `d`
    }
    "
    .compile_fail()
}

#[test]
fn map_duplicate_unknown_property() {
    "
    type foo
    type bar
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
    type foo
    type bar
    rel foo 'prop': string
    rel bar 'prop': int
    map {
        foo {
            'prop': x
        }
        bar {
            'prop':
                x // ERROR type mismatch: expected `int`, found `string`
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
    type foo
    type bar
    rel foo 'prop': string
    rel bar 'prop': int
    map {
        foo {
            'prop': x
        }
        bar {
            'prop':
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
    rel foo 'a': [string]
    rel foo 'b': [string]

    type bar
    rel bar 'a': string
    rel bar 'b': [int]

    map {
        foo {
            'a': x // ERROR [string] variable must be enclosed in []
            'b': y // ERROR [string] variable must be enclosed in []
        }
        bar {
            'a': x
            'b': y // ERROR [int] variable must be enclosed in []
        }
    }
    "
    .compile_fail();
}

#[test]
fn array_map_without_brackets() {
    "
    type foo { rel .'a': [string] }
    type bar { rel .'b': [string] }

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
    type foo
    type bar
    rel [foo] 'a'()::'aa' bar // ERROR only entities may have named reverse relationship
    rel [foo] 'b'::'bb' string // ERROR only entities may have named reverse relationship
    "
    .compile_fail()
}

#[test]
fn unresolved_transitive_map() {
    "
    type a { rel .is?: int }
    type b { rel .is?: int }

    type c { rel .'p0': a }
    type d { rel .'p1': b }

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
    type foo {
        rel .'type': 'foo'
    }
    type bar {
        rel .'type': 'bar'
    }
    type foobar {
        rel .is?: foo
        rel .is?: bar
    }

    map {
        foobar {} // ERROR cannot map a union, map each variant instead
        foo {} // ERROR missing property `type`
    }
    "
    .compile_fail();
}

#[test]
fn map_invalid_unit_rel_params() {
    "
    type foo { rel .'foo': string }
    type bar { rel .'bar': string }

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
