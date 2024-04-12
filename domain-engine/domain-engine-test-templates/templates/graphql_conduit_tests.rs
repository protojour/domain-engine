use std::collections::HashMap;
use std::sync::Arc;

use domain_engine_core::{system::SystemApiMock, DomainEngine, Session};
use domain_engine_juniper::{
    context::ServiceCtx,
    gql_scalar::GqlScalar,
    juniper::{graphql_value, InputValue, ScalarValue, Value},
    Schema,
};
use domain_engine_test_utils::{
    graphql_test_utils::{Exec, GraphQLPageDebug, TestCompileSchema, ValueExt},
    system::mock_current_time_monotonic,
    unimock,
    unimock::MockFn,
};
use ontol_runtime::ontology::{config::DataStoreConfig, Ontology};
use ontol_test_utils::{
    examples::conduit::{BLOG_POST_PUBLIC, CONDUIT_DB, FEED_PUBLIC},
    expect_eq, TestPackages,
};
use test_log::test;
use tracing::info;

fn conduit_db_only() -> TestPackages {
    TestPackages::with_static_sources([CONDUIT_DB])
        .with_data_store(CONDUIT_DB.0, DataStoreConfig::Default)
}

async fn make_domain_engine(
    ontology: Arc<Ontology>,
    mock_clause: impl unimock::Clause,
) -> DomainEngine {
    DomainEngine::builder(ontology)
        .system(Box::new(unimock::Unimock::new(mock_clause)))
        .build(crate::TestDataStoreFactory::default(), Session::default())
        .await
        .unwrap()
}

#[test(tokio::test)]
async fn test_graphql_conduit_db() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([CONDUIT_DB.0]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic())
        .await
        .into();

    expect_eq!(
        actual = r#"mutation {
            User(
                create: [{
                    username: "u1",
                    email: "a@b",
                    password_hash: "s3cr3t",
                }]
            ) {
                node {
                    username
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "User": [{
                "node": {
                    "username": "u1"
                }
            }]
        })),
    );

    expect_eq!(
        actual = "{
            users {
                edges {
                    node {
                        username
                        email
                        password_hash
                        bio
                    }
                }
            }
        }"
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "users": {
                "edges": [
                    {
                        "node": {
                            "username": "u1",
                            "email": "a@b",
                            "password_hash": "s3cr3t",
                            "bio": "",
                        }
                    }
                ]
            },
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_db_create_with_foreign_reference() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([CONDUIT_DB.0]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic())
        .await
        .into();

    let response = r#"mutation {
        User(create: [{
            username: "u1",
            email: "a@b",
            password_hash: "s3cr3t",
        }]) {
            node { user_id }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    let user_id = response
        .field("User")
        .element(0)
        .field("node")
        .field("user_id")
        .scalar()
        .clone();

    expect_eq!(
        actual = r#"mutation new_article($authorId: ID!) {
            Article(create: [{
                slug: "the-slug",
                title: "The title",
                description: "An article",
                body: "THE BODY",
                author: {
                    user_id: $authorId
                }
                tags: [{ tag: "foobar" }]
            }]) {
                node {
                    slug
                    author {
                        username
                    }
                }
            }
        }"#
        .exec(
            [("authorId".to_owned(), InputValue::Scalar(user_id))],
            &schema,
            &ctx
        )
        .await,
        expected = Ok(graphql_value!({
            "Article": [{
                "node": {
                    "slug": "the-slug",
                    "author": {
                        "username": "u1"
                    }
                }
            }]
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_db_query_article_with_tags() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([CONDUIT_DB.0]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic())
        .await
        .into();

    let _response = r#"mutation {
        Article(create: [{
            slug: "the-slug",
            title: "The title",
            description: "An article",
            body: "THE BODY",
            author: {
                username: "u1",
                email: "a@b",
                password_hash: "s3cr3t",
            }
            tags: [{ tag: "foobar" }]
        }]) {
            node { slug }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    expect_eq!(
        actual = r#"{
            articles {
                edges {
                    node {
                        tags {
                            edges {
                                node {
                                    tag
                                }
                            }
                        }
                    }
                }
            }
        }"#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "articles": {
                "edges": [{
                    "node": {
                        "tags": {
                            "edges": [{
                                "node": {
                                    "tag": "foobar"
                                }
                            }]
                        }
                    }
                }]
            }
        })),
    );
}

struct ConduitBundle {
    domain_engine: Arc<DomainEngine>,
    db_schema: Schema,
    blog_schema: Schema,
    feed_schema: Schema,
}

impl ConduitBundle {
    async fn new(mock_clauses: impl unimock::Clause) -> Self {
        let test_packages =
            TestPackages::with_static_sources([BLOG_POST_PUBLIC, FEED_PUBLIC, CONDUIT_DB])
                .with_roots([BLOG_POST_PUBLIC.0, FEED_PUBLIC.0])
                .with_data_store(CONDUIT_DB.0, DataStoreConfig::Default);

        let (test, [blog_schema, feed_schema, db_schema]) =
            test_packages.compile_schemas([BLOG_POST_PUBLIC.0, FEED_PUBLIC.0, CONDUIT_DB.0]);

        Self {
            domain_engine: Arc::new(make_domain_engine(test.ontology_owned(), mock_clauses).await),
            db_schema,
            blog_schema,
            feed_schema,
        }
    }

    fn ctx(&self) -> ServiceCtx {
        self.domain_engine.clone().into()
    }

    async fn create_db_article_for_teh_user(&self, title: &str) -> String {
        // Insert using the data store domain directly:
        let response = r#"mutation new_article($title: String!) {
            Article(create: [{
                slug: "the-slug",
                title: $title,
                description: "An article",
                body: "THE BODY",
                author: {
                    username: "teh_user",
                    email: "a@b",
                    password_hash: "s3cr3t"
                }
            }]) {
                node { article_id }
            }
        }"#
        .exec(
            [("title".to_owned(), InputValue::Scalar(title.into()))],
            &self.db_schema,
            &self.ctx(),
        )
        .await
        .unwrap();

        let article_id = response
            .field("Article")
            .element(0)
            .field("node")
            .field("article_id")
            .scalar()
            .clone();

        article_id.as_string().unwrap()
    }

    async fn create_db_article_with_tag_for_teh_user(&self) {
        expect_eq!(
            actual = r#"mutation {
                Article(create: [{
                    slug: "the-slug",
                    title: "The title",
                    description: "An article",
                    body: "THE BODY",
                    author: {
                        username: "teh_user",
                        email: "a@b",
                        password_hash: "s3cr3t"
                    }
                    tags: [{ tag: "foobar" }]
                }]) {
                    node { slug }
                }
            }"#
            .exec([], &self.db_schema, &self.ctx())
            .await,
            expected = Ok(graphql_value!({
                "Article": [{
                    "node": {"slug": "the-slug"}
                }]
            })),
        );
    }
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_implicit_join() {
    let test = ConduitBundle::new(mock_current_time_monotonic()).await;
    test.create_db_article_for_teh_user("The title").await;

    expect_eq!(
        actual = r#"{
            posts(input: { written_by: "teh_user" }) {
                edges {
                    node {
                        contents
                        written_by
                    }
                }
            }
        }"#
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "posts": {
                "edges": [
                    {
                        "node": {
                            "contents": "THE BODY",
                            "written_by": "teh_user",
                        }
                    }
                ]
            }
        })),
    );

    expect_eq!(
        actual = r#"{
            posts(input: { written_by: "someone_else" }) {
                edges {
                    node {
                        contents
                        written_by
                    }
                }
            }
        }"#
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({ "posts": { "edges": [] } })),
    );
}

/// The purpose of this test is to ensure that SubSequence information
/// is threaded through the domain engine pipeline.
#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_paginated() {
    let test = ConduitBundle::new(mock_current_time_monotonic()).await;
    for _ in 0..3 {
        test.create_db_article_for_teh_user("The title").await;
    }

    let mut prev_end_cursor: Option<InputValue<GqlScalar>> = None;

    for index in 0..3 {
        info!("Executing page {index}");

        let response: Value<GqlScalar> = r#"
            query paginated_posts($after: String) {
                posts(input: { written_by: "teh_user" }, first: 1, after: $after) {
                    nodes {
                        contents
                        written_by
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                    totalCount
                }
            }
        "#
        .exec(
            if let Some(cursor) = &prev_end_cursor {
                HashMap::from([("after".to_owned(), cursor.clone())])
            } else {
                HashMap::default()
            },
            &test.blog_schema,
            &test.ctx(),
        )
        .await
        .unwrap();

        let debug = GraphQLPageDebug::parse_connection(&response, "posts").unwrap();
        assert_eq!(debug.total_count, Some(3));
        assert_eq!(debug.has_next_page, index < 2);
        prev_end_cursor = debug
            .end_cursor
            .map(|cursor| InputValue::Scalar(GqlScalar::String(cursor.into())));
    }
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_tags() {
    let test = ConduitBundle::new(mock_current_time_monotonic()).await;
    test.create_db_article_with_tag_for_teh_user().await;

    expect_eq!(
        actual = "{
            posts {
                edges {
                    node {
                        contents
                        written_by
                        tags
                    }
                }
            }
        }"
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "posts": {
                "edges": [
                    {
                        "node": {
                            "contents": "THE BODY",
                            "written_by": "teh_user",
                            "tags": ["foobar"]
                        }
                    }
                ]
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_no_join_real() {
    let test = ConduitBundle::new(mock_current_time_monotonic()).await;
    test.create_db_article_for_teh_user("The title").await;

    expect_eq!(
        actual = "{
            posts {
                edges {
                    node {
                        contents
                    }
                }
            }
        }"
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "posts": {
                "edges": [
                    {
                        "node": {
                            "contents": "THE BODY",
                        }
                    }
                ]
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_order() {
    let test = ConduitBundle::new(mock_current_time_monotonic()).await;
    test.create_db_article_for_teh_user("Oldest").await;
    test.create_db_article_for_teh_user("Middle").await;
    test.create_db_article_for_teh_user("Newest").await;

    expect_eq!(
        actual = "{ posts { nodes { name } } }"
            .exec([], &test.blog_schema, &test.ctx())
            .await,
        expected = Ok(graphql_value!({
            "posts": {
                "nodes": [
                    { "name": "Newest" },
                    { "name": "Middle" },
                    { "name": "Oldest" },
                ]
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_db_article_shallow_update() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([CONDUIT_DB.0]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic())
        .await
        .into();

    let response = r#"mutation {
        Article(
            create: [{
                slug: "the-slug",
                title: "The old title",
                description: "An article",
                body: "THE BODY",
                author: {
                    username: "u1",
                    email: "a@b",
                    password_hash: "s3cr3t",
                }
                tags: [{ tag: "foobar" }]
            }]
        ) {
            node {
                article_id
                updated_at
            }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    let article_id = response
        .field("Article")
        .element(0)
        .field("node")
        .field("article_id")
        .scalar()
        .clone();

    let update_mutation = format!(
        r#"mutation {{
            Article(
                update: [{{
                    article_id: "{article_id}"
                    title: "THE NEW TITLE"
                }}]
            ) {{
                node {{
                    article_id
                    slug
                    title
                    created_at
                    updated_at
                    author {{
                        username
                    }}
                }}
            }}
        }}"#
    );
    expect_eq!(
        actual = update_mutation.exec([], &schema, &ctx).await,
        expected = Ok(graphql_value!({
            "Article": [{
                "node": {
                    "article_id": article_id,
                    "slug": "the-slug",
                    "title": "THE NEW TITLE",
                    // Note: The TestSystem counts one year between successive reads of the time :)
                    "created_at": "1970-01-01T00:00:00+00:00",
                    "updated_at": "1971-01-01T00:00:00+00:00",
                    "author": {
                        "username": "u1"
                    }
                }
            }]
        }))
    );

    expect_eq!(
        actual = "{
            users {
                nodes {
                    authored_articles {
                        nodes { title }
                    }
                }
            }
        }"
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "users": {
                "nodes": [
                    {
                        "authored_articles": {
                            "nodes": [
                                {
                                    "title": "THE NEW TITLE"
                                }
                            ]
                        }
                    }
                ]
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_db_user_deletion() {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas([CONDUIT_DB.0]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic())
        .await
        .into();

    let response = r#"mutation {
        User(
            create: [{
                username: "to_be_deleted",
                email: "a@b",
                password_hash: "s3cr3t",
            }]
        ) {
            node {
                user_id
            }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    let user_id = response
        .field("User")
        .element(0)
        .field("node")
        .field("user_id")
        .scalar()
        .clone();

    r#"mutation {
        User(
            create: [{
                username: "retained_user",
                email: "b@c",
                password_hash: "123abc",
            }]
        ) {
            node {
                user_id
            }
        }
    }"#
    .exec([], &schema, &ctx)
    .await
    .unwrap();

    // Do the action deletion: One real user and one bogus user ID
    expect_eq!(
        actual = {
            let mutation = format!(
                r#"mutation {{
                    User(delete: ["{user_id}", "deadbeef-baad-baad-baad-c0ffeeeeeeee"]) {{
                        deleted
                    }}
                }}"#
            );
            mutation.exec([], &schema, &ctx).await
        },
        expected = Ok(graphql_value!({
            "User": [
                { "deleted": true },
                { "deleted": false }
            ]
        })),
    );

    // Verify that the user was actually deleted
    expect_eq!(
        actual = "{ users { nodes { username } } }"
            .exec([], &schema, &ctx)
            .await,
        expected = Ok(graphql_value!({
            "users": {
                "nodes": [
                    { "username": "retained_user" }
                ]
            },
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_update_body_create_comment() {
    let test = ConduitBundle::new(mock_current_time_monotonic()).await;
    let article_id = test.create_db_article_for_teh_user("The title").await;

    expect_eq!(
        actual = format!(
            "mutation {{
                BlogPost(update: [
                    {{
                        post_id: \"{article_id}\"
                        contents: \"The new body\"
                    }}
                ]) {{
                    node {{ contents }}
                }}
            }}"
        )
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "BlogPost": [{ "node": {
                "contents": "The new body"
            } }]
        })),
    );

    let comment_response = format!(
        "mutation {{
            Comment(create: [
                {{
                    comment_on: {{
                        article_id: \"{article_id}\"
                    }}
                    author: {{
                        username: \"troll\",
                        email: \"troll@troll.no\",
                        password_hash: \"s3cr3t\"
                    }}
                    body: \"this sucks\"
                }}
            ]) {{
                node {{ body, id }}
            }}
        }}"
    )
    .exec([], &test.db_schema, &test.ctx())
    .await
    .unwrap();

    let comment_node = comment_response
        .field("Comment")
        .element(0)
        .field("node")
        .clone();

    expect_eq!(
        actual = comment_node.field("body").clone(),
        expected = graphql_value!("this sucks")
    );

    // Comment ID uses ontol `serial`, which should be string-encoded:
    let Value::Scalar(GqlScalar::String(_id)) = comment_node.field("id") else {
        panic!("Comment ID was not a string");
    };
}

#[test(tokio::test)]
async fn test_graphql_blog_post_conduit_delete() {
    let test = ConduitBundle::new(mock_current_time_monotonic()).await;
    let article_id = test.create_db_article_for_teh_user("The title").await;

    expect_eq!(
        actual = format!(
            "mutation {{
                BlogPost(delete: [\"{article_id}\"]) {{
                    deleted
                }}
            }}"
        )
        .exec([], &test.blog_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "BlogPost": [{ "deleted": true }]
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_feed_public_no_query_selection() {
    let test = ConduitBundle::new((
        mock_current_time_monotonic(),
        SystemApiMock::call_http_json_hook
            .next_call(unimock::matching!(
                "http://localhost:8080/map_channel",
                _,
                _
            ))
            .answers(&|_, _, _, input| {
                // The query does not select `items` in the channel,
                // so that query output ("items") should be empty, even if the datastore contains
                // an article for "teh_user":
                assert_eq!(
                    input.as_slice(),
                    br#"{"link":"http://blogs.com/teh_user/feed","username":"teh_user","items":[]}"#
                );

                // Inject "title"
                Ok(br#"{
                    "title": "http",
                    "link": "",
                    "username": "",
                    "items": []
                }"#
                .to_vec())
            }),
    ))
    .await;

    // create the article
    test.create_db_article_for_teh_user("The title").await;

    expect_eq!(
        actual = "
        {{
            feed(input: {{ username: \"teh_user\" }}) {{
                title
            }}
        }}"
        .to_string()
        .exec([], &test.feed_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "feed": {
                "title": "http"
            }
        })),
    );
}

#[test(tokio::test)]
async fn test_graphql_conduit_feed_public_with_items_query() {
    let test = ConduitBundle::new((
        mock_current_time_monotonic(),
        SystemApiMock::call_http_json_hook
            .next_call(unimock::matching!(
                "http://localhost:8080/map_channel",
                _,
                _
            ))
            .answers(&|_, _, _, input| {
                // Assert that there is at least one item (from the datastore),
                // and there is one if the JSON contains "guid"
                assert!(input.windows(6).any(|subslice| subslice == b"\"guid\""));

                // Inject "title"
                Ok(br#"{
                    "title": "http",
                    "link": "",
                    "username": "",
                    "items": [{
                        "guid": "11111111-2222-3333-4444-555555555555",
                        "title": "pwned"
                    }]
                }"#
                .to_vec())
            }),
    ))
    .await;

    test.create_db_article_for_teh_user("The title").await;

    expect_eq!(
        actual = "
        {{
            feed(input: {{ username: \"teh_user\" }}) {{
                title
                items {{ guid }}
            }}
        }}"
        .to_string()
        .exec([], &test.feed_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "feed": {
                "title": "http",
                "items": [{ "guid": "11111111-2222-3333-4444-555555555555" }]
            }
        })),
    );
}
