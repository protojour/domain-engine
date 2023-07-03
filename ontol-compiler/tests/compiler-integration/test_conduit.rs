use ontol_test_utils::{SourceName, TestCompile, TestPackages};
use test_log::test;

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
    .compile_fail();
}
