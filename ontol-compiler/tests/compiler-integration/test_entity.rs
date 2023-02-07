use serde_json::json;
use test_log::test;

use crate::{util::TypeBinding, TestCompile, TEST_PKG};

#[test]
fn test_entity_experiment_etc() {
    r#"
    (type! artist-id)

    (entity! artist)
    (rel! (artist) name (string))

    (rel! (artist-id) _ "artist/{uuid}")
    ; (rel! (artist-id) uuid (string))

    (rel! (artist-id) id! (artist)) ;; ERROR cannot mix named and anonymous relations on the same type

    (entity! record)
    (rel! (record) name (string))
    ; i.e. syntax sugar for:
    ; (rel! (record) "name" (unit) name (string))

    (entity! instrument)

    (type! plays)

    (rel! (artist) plays[] (plays) played-by (instrument)) ;; ERROR parse error: expected end of list

    ; this adds an _edge object to related-to JSON
    (rel! (plays) how-much (string))
    "#
    .compile_fail()
}

fn example_json() {
    json!(
        {
            "some_prop": 42,
            "relation_to_entity": {
                "this_is_the_entity": 42,
                // This is where the edge type goes if it's not (unit),
                // and the object type is an entity (an entity must be a map type)
                "_edge": {}
            }
        }
    );
}
