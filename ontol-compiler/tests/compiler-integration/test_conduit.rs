use indoc::indoc;
use ontol_runtime::{format_utils::Literal, property::ValueCardinality, value::Value};
use ontol_test_utils::{
    examples::conduit::{
        BLOG_POST_PUBLIC, CONDUIT_CONTRIVED_SIGNUP, CONDUIT_DB, CONDUIT_PUBLIC, FEED_PUBLIC,
    },
    test_map::YielderMock,
    TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;
use unimock::{matching, MockFn};

#[test]
fn test_compile_conduit_public() {
    CONDUIT_PUBLIC.1.compile_fail();
}

#[test]
fn test_compile_conduit_db() {
    CONDUIT_DB.1.compile();
}

#[test]
fn test_map_conduit_blog_post() {
    let test = TestPackages::with_static_sources([BLOG_POST_PUBLIC, CONDUIT_DB]).compile();
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
    let test = TestPackages::with_static_sources([BLOG_POST_PUBLIC, CONDUIT_DB]).compile();
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
    let test = TestPackages::with_static_sources([BLOG_POST_PUBLIC, CONDUIT_DB]).compile();
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
                    eq!(&ValueCardinality::OrderedSet),
                    eq!(&Literal(indoc! { r#"
                        (root $a)
                        (is-entity $a def@2:5)
                        (match-prop $a S:2:66 (element-in $c))
                        (is-entity $b def@2:4)
                        (match-prop $b S:2:19 (element-in $d))
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
    let test = TestPackages::with_static_sources([FEED_PUBLIC, CONDUIT_DB]).compile();
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
                    eq!(&ValueCardinality::OrderedSet),
                    eq!(&Literal(indoc! { r#"
                        (root $a)
                        (is-entity $a def@2:5)
                        (match-prop $a S:2:66 (element-in $c))
                        (is-entity $b def@2:4)
                        (match-prop $b S:2:19 (element-in $d))
                        (member $c (_ $b))
                        (member $d (_ 'foobar'))
                        (order 'by_date')
                    "#
                    }))
                ))
                .returns(article_return_value.clone()),
            YielderMock::yield_call_extern_http_json
                .next_call(matching!("http://localhost:8080/map_channel", _json))
                .answers(move |(_url, json)| {
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
    TestPackages::with_static_sources([CONDUIT_CONTRIVED_SIGNUP, CONDUIT_DB]).compile();
}
