use ontol_test_utils::TestCompile;
use serde_json::json;
use test_log::test;

// BUG: This should work somehow
#[test]
fn test_map_match_non_inherent_id() {
    r#"
    pub def key { rel .is: text }
    pub def ent { rel .id: key }
    map {
        key: key
        ent match { // ERROR no properties expected
            id: key
        }
    }
    "#
    .compile_fail();
}

#[test]
fn test_map_match_scalar_key() {
    r#"
    pub def key { rel .is: text }
    pub def foo {
        rel .'key'|id: key
        rel .'prop': text
    }
    map {
        key: key
        foo match { 'key': key }
    }
    "#
    .compile_then(|_test| {});
}

#[test]
// BUG
#[should_panic = "No serde operator id"]
fn test_map_match_parameterless_query() {
    r#"
    pub def key { rel .is: text }
    pub def foo {
        rel .'key'|id: key
        rel .'prop': text
    }
    map q {
        {}
        foo: [..foo match {}]
    }
    "#
    .compile_then(|test| {
        test.assert_named_forward_map("q", json!({}), json!({}));
    });
}
