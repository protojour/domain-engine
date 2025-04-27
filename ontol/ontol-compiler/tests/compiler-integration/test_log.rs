use ontol_macros::test;
use ontol_runtime::{attr::AttrRef, value::Value};
use ontol_test_utils::{
    TestCompile, assert_json_io_matches, expect_eq,
    serde_helper::{serde_create, serde_read},
};
use serde_json::json;

#[test]
fn inherent_id_no_autogen() {
    "
    domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()
    def foo_id (rel* is: text)
    def foo (
        rel. 'key': foo_id
        rel* ancestry.children: {foo}
    )
    arc ancestry {
        (p) children: (c)
    }
    "
    .compile_log_then(|test| {
        let [foo] = test.bind(["foo"]);
        assert_json_io_matches!(serde_create(&foo), { "key": "id", "children": [{ "key": "foreign_id" }] });

        let entity: Value = foo.entity_builder(json!("id"), json!({ "key": "id" })).into();
        expect_eq!(
            actual = serde_read(&foo).as_json(AttrRef::Unit(&entity)),
            expected = json!({ "key": "id" }),
        );
    });
}
