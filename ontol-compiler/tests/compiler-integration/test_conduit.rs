use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use serde_json::json;
use test_log::test;

use crate::unify::assert_domain_map;

pub const CONDUIT_PUBLIC: &str = include_str!("../../../examples/conduit/conduit_public.on");
pub const CONDUIT_DB: &str = include_str!("../../../examples/conduit/conduit_db.on");
pub const BLOG_POST_PUBLIC: &str = include_str!("../../../examples/conduit/blog_post_public.on");

#[test]
fn test_compile_conduit_public() {
    CONDUIT_PUBLIC.compile_fail();
}

#[test]
fn test_compile_conduit_db() {
    CONDUIT_DB.compile_ok(|_env| {});
}

#[test]
fn test_compile_conduit_blog_post() {
    TestPackages::with_sources([
        (SourceName("conduit_db"), CONDUIT_DB),
        (SourceName::root(), BLOG_POST_PUBLIC),
    ])
    .compile_ok(|test_env| {
        assert_domain_map(
            &test_env,
            ("conduit_db::Article", "BlogPost"),
            json!({
                "article_id": "11111111-1111-1111-1111-111111111111",
                "slug": "s",
                "title": "t",
                "description": "d",
                "body": "THE BODY",
                "author": {
                    "user_id": "22222222-2222-2222-2222-222222222222",
                    "username": "u",
                    "email": "e",
                    "password_hash": "h",
                }
            }),
            json!({
                "post_id": "11111111-1111-1111-1111-111111111111",
                "body": "THE BODY"
            }),
        )
    });
}
