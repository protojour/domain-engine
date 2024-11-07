use indoc::indoc;
use ontol_examples::conduit::{
    blog_post_public, conduit_contrived_signup, conduit_db, conduit_public, feed_public,
};
use ontol_macros::test;
use ontol_runtime::{
    format_utils::Literal, property::ValueCardinality, tuple::CardinalIdx, value::Value, DefIdSet,
};
use ontol_test_utils::{test_map::YielderMock, TestCompile, TestPackages};
use serde_json::json;
use unimock::{matching, MockFn};

#[test]
fn test_compile_conduit_public() {
    conduit_public().1.compile_fail();
}

#[test]
fn test_compile_conduit_db() {
    conduit_db().1.compile();
}

#[test]
fn test_conduit_db_ontology_smoke() {
    let test = conduit_db().1.compile();
    let [article, user] = test.bind(["Article", "User"]);

    let (_, author_rel_info) = article
        .def
        .data_relationship_by_name("author", test.ontology())
        .unwrap();
    let author_edge_projection = author_rel_info.edge_kind().unwrap();

    assert_eq!(author_edge_projection.subject, CardinalIdx(0));
    assert_eq!(author_edge_projection.object, CardinalIdx(1));

    let author_edge_info = test
        .ontology()
        .find_edge(author_edge_projection.edge_id)
        .unwrap();

    assert_eq!(
        &author_edge_info.cardinals[0].target,
        &DefIdSet::from_iter([article.def_id()])
    );
    assert_eq!(
        &author_edge_info.cardinals[1].target,
        &DefIdSet::from_iter([user.def_id()])
    );
}

#[test]
fn test_map_conduit_blog_post() {
    let test = TestPackages::with_sources([blog_post_public(), conduit_db()]).compile();
    test.mapper().assert_map_eq(
        ("conduit_db.Article", "BlogPost"),
        json!({
            "article_id": "11111111-1111-1111-1111-111111111111",
            "slug": "s",
            "title": "t",
            "description": "d",
            "body": "THE BODY",
            "author": {
                "user_id": "22222222-2222-2222-2222-222222222222",
                "username": "some_user",
                "email": "e@mail",
                "password_hash": "h",
            },
            "tags": [{ "tag": "foobar" }],
            "created_at": "2023-01-01T00:00:00.000+00:00",
            "updated_at": "2023-01-01T00:00:00.000+00:00",
        }),
        json!({
            "post_id": "11111111-1111-1111-1111-111111111111",
            "name": "t",
            "contents": "THE BODY",
            "written_by": "some_user",
            "tags": ["foobar"]
        }),
    );
}

/// Test that the mapping works without providing the "tags" property in the input.
#[test]
fn test_map_conduit_no_tags_in_db_object() {
    let test = TestPackages::with_sources([blog_post_public(), conduit_db()]).compile();
    test.mapper().assert_map_eq(
        ("conduit_db.Article", "BlogPost"),
        json!({
            "article_id": "11111111-1111-1111-1111-111111111111",
            "slug": "s",
            "title": "t",
            "description": "d",
            "body": "THE BODY",
            "created_at": "2023-01-01T00:00:00.000+00:00",
            "updated_at": "2023-01-01T00:00:00.000+00:00",
            "author": {
                "user_id": "22222222-2222-2222-2222-222222222222",
                "username": "some_user",
                "email": "e@mail",
                "password_hash": "h",
            },
        }),
        json!({
            "post_id": "11111111-1111-1111-1111-111111111111",
            "name": "t",
            "contents": "THE BODY",
            "written_by": "some_user",
            "tags": []
        }),
    );
}

#[test]
fn test_map_match_conduit_blog_post_cond_clauses() {
    let test = TestPackages::with_sources([blog_post_public(), conduit_db()]).compile();
    let [article] = test.bind(["conduit_db.Article"]);
    let return_value = Value::sequence_of([article
        .entity_builder(
            json!("11111111-1111-1111-1111-111111111111"),
            json!({
                "slug": "s", "title": "t", "description": "d", "body": "b",
                "author": { "username": "u", "email": "e", "password_hash": "h" }
            }),
        )
        .into()]);

    test.mapper()
        .with_mock_yielder(
            YielderMock::yield_match
                .next_call(matching!(
                    eq!(&ValueCardinality::IndexSet),
                    eq!(&Literal(indoc! { r#"
                        (root $a)
                        (is-def $a def@2:5)
                        (match-prop $a p@2:5:10 (element-in $c))
                        (is-def $b def@2:4)
                        (match-prop $b p@2:4:1 (element-in $d))
                        (member $c (_ $b))
                        (member $d (_ 'someone'))
                        (order 'by_date')
                    "#
                    }))
                ))
                .returns(return_value.clone()),
        )
        .assert_named_forward_map(
            "posts",
            json!({ "written_by": "someone", }),
            json!([{
                "post_id": "11111111-1111-1111-1111-111111111111",
                "name": "t",
                "contents": "b",
                "tags": [],
                "written_by": "u",
            }]),
        );
}

#[test]
fn test_conduit_feed_public() {
    let test = TestPackages::with_sources([feed_public(), conduit_db()]).compile();
    let [article] = test.bind(["conduit_db.Article"]);
    let article_return_value = Value::sequence_of([article
        .entity_builder(
            json!("11111111-1111-1111-1111-111111111111"),
            json!({
                "slug": "s", "title": "t", "description": "d", "body": "b",
                "author": { "username": "foobar", "email": "e", "password_hash": "h" }
            }),
        )
        .into()]);

    test.mapper()
        .with_mock_yielder((
            YielderMock::yield_match
                .next_call(matching!(
                    eq!(&ValueCardinality::IndexSet),
                    eq!(&Literal(indoc! { r#"
                        (root $a)
                        (is-def $a def@2:5)
                        (match-prop $a p@2:5:10 (element-in $c))
                        (is-def $b def@2:4)
                        (match-prop $b p@2:4:1 (element-in $d))
                        (member $c (_ $b))
                        (member $d (_ 'foobar'))
                        (order 'by_date')
                    "#
                    }))
                ))
                .returns(article_return_value.clone()),
            YielderMock::yield_call_extern_http_json
                .next_call(matching!("http://localhost:8080/map_channel", _json))
                .answers(&|_, _url, json| {
                    let json_obj = json.as_object().unwrap();

                    json!({
                        "title": "extern title",
                        "username": "extern username",
                        "link": json_obj.get("link").unwrap(),
                        "items": json_obj.get("items").unwrap()
                    })
                }),
        ))
        .assert_named_forward_map(
            "feed",
            json!({ "username": "foobar", }),
            json!({
                "title": "extern title",
                "username": "extern username",
                "link": "http://blogs.com/foobar/feed",
                "items": [{
                    "guid": "11111111-1111-1111-1111-111111111111",
                    "title": "t",
                }]
            }),
        );
}

// BUG: There are many things that must be fixed for this to work:
#[test]
#[should_panic(expected = "unbound variable")]
fn test_map_conduit_contrived_signup() {
    TestPackages::with_sources([conduit_contrived_signup(), conduit_db()]).compile();
}
