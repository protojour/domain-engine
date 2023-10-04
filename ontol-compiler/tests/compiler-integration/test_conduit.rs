use ontol_test_utils::{
    examples::{
        conduit::{
            BLOG_POST_PUBLIC, BLOG_POST_QUERY, CONDUIT_CONTRIVED_SIGNUP, CONDUIT_DB, CONDUIT_PUBLIC,
        },
        Root,
    },
    TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

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
    let test = TestPackages::with_sources([CONDUIT_DB, BLOG_POST_PUBLIC.root()]).compile();
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
            "contents": "THE BODY",
            "written_by": "some_user",
            "tags": ["foobar"]
        }),
    );
}

/// Test that the mapping works without providing the "tags" property in the input.
#[test]
fn test_map_conduit_no_tags_in_db_object() {
    let test = TestPackages::with_sources([CONDUIT_DB, BLOG_POST_PUBLIC.root()]).compile();
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
            "contents": "THE BODY",
            "written_by": "some_user",
            "tags": []
        }),
    )
}

// BUG: There are many things that must be fixed for this to work:
#[test]
fn test_map_conduit_contrived_signup() {
    TestPackages::with_sources([CONDUIT_DB, CONDUIT_CONTRIVED_SIGNUP.root()]).compile();
}

#[test]
fn test_map_blog_post_query() {
    TestPackages::with_sources([CONDUIT_DB, BLOG_POST_QUERY.root()]).compile();
}
