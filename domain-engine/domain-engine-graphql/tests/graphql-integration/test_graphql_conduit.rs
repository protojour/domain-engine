use core::str;
use std::collections::HashMap;
use std::sync::Arc;

use domain_engine_core::{DomainEngine, Session, system::SystemApiMock};
use domain_engine_graphql::{
    domain::{DomainSchema, context::ServiceCtx},
    gql_scalar::GqlScalar,
    juniper::{InputValue, ScalarValue, Value, graphql_value},
};
use domain_engine_test_utils::{
    dynamic_data_store::DynamicDataStoreClient,
    graphql_test_utils::{Exec, GraphQLPageDebug, TestCompileSchema, ValueExt},
    system::mock_current_time_monotonic,
    unimock,
    unimock::MockFn,
};
use ontol_examples::{
    AsAtlas,
    conduit::{blog_post_public, conduit_db, feed_public},
};
use ontol_macros::datastore_test;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::{TestPackages, expect_eq};
use tracing::info;

fn conduit_db_only() -> TestPackages {
    TestPackages::with_sources([conduit_db()])
}

async fn make_domain_engine(
    ontology: Arc<Ontology>,
    mock_clause: impl unimock::Clause,
    ds: &str,
) -> DomainEngine {
    DomainEngine::builder(ontology)
        .system(Box::new(unimock::Unimock::new(mock_clause)))
        .build(
            DynamicDataStoreClient::new(ds).connect().await.unwrap(),
            Session::default(),
        )
        .await
        .unwrap()
}

#[datastore_test(tokio::test)]
async fn conduit_db_general(ds: &str) {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas(["conduit_db"]);
    let ctx: ServiceCtx =
        make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds)
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

#[datastore_test(tokio::test)]
async fn conduit_db_create_with_foreign_reference(ds: &str) {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas(["conduit_db"]);
    let ctx: ServiceCtx =
        make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds)
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

#[datastore_test(tokio::test)]
async fn conduit_db_query_article_with_tags(ds: &str) {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas(["conduit_db"]);
    let ctx: ServiceCtx =
        make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds)
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
    db_schema: DomainSchema,
    blog_schema: DomainSchema,
    feed_schema: DomainSchema,
}

impl ConduitBundle {
    async fn new(mock_clauses: impl unimock::Clause, ds: &str) -> Self {
        let test_packages = TestPackages::with_sources([
            blog_post_public(),
            feed_public(),
            conduit_db().as_atlas("conduit"),
        ])
        .with_entrypoints([blog_post_public().0, feed_public().0]);

        let (test, [blog_schema, feed_schema, db_schema]) =
            test_packages.compile_schemas(["blog_post_public", "feed_public", "conduit_db"]);

        Self {
            domain_engine: Arc::new(
                make_domain_engine(test.ontology_owned(), mock_clauses, ds).await,
            ),
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

#[datastore_test(tokio::test)]
async fn blog_post_implicit_join(ds: &str) {
    let test = ConduitBundle::new(mock_current_time_monotonic(), ds).await;
    test.create_db_article_for_teh_user("The title").await;

    info!("expecting filtered results");

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

    info!("expecting empty results");

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
#[datastore_test(tokio::test)]
async fn blog_post_paginated(ds: &str) {
    let test = ConduitBundle::new(mock_current_time_monotonic(), ds).await;
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

#[datastore_test(tokio::test)]
async fn blog_post_tags(ds: &str) {
    let test = ConduitBundle::new(mock_current_time_monotonic(), ds).await;
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

#[datastore_test(tokio::test)]
async fn blog_post_no_join_real(ds: &str) {
    let test = ConduitBundle::new(mock_current_time_monotonic(), ds).await;
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

#[datastore_test(tokio::test)]
async fn blog_post_order(ds: &str) {
    let test = ConduitBundle::new(mock_current_time_monotonic(), ds).await;
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

#[datastore_test(tokio::test)]
async fn conduit_db_article_shallow_update(ds: &str) {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas(["conduit_db"]);
    let ctx: ServiceCtx =
        make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds)
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
                    "created_at": "1971-01-01T00:00:00Z",
                    "updated_at": "1974-01-01T00:00:00Z",
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

#[datastore_test(tokio::test)]
async fn conduit_db_user_deletion(ds: &str) {
    let test_packages = conduit_db_only();
    let (test, [schema]) = test_packages.compile_schemas(["conduit_db"]);
    let ctx: ServiceCtx =
        make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds)
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

#[datastore_test(tokio::test)]
async fn blog_post_update_body_create_comment(ds: &str) {
    let test = ConduitBundle::new(mock_current_time_monotonic(), ds).await;
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

#[datastore_test(tokio::test)]
async fn blog_post_delete(ds: &str) {
    let test = ConduitBundle::new(mock_current_time_monotonic(), ds).await;
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

#[datastore_test(tokio::test)]
async fn feed_public_no_query_selection(ds: &str) {
    let test = ConduitBundle::new(
        (
            mock_current_time_monotonic(),
            SystemApiMock::call_http_json_hook
                .next_call(unimock::matching!(
                    "http://localhost:8080/map_channel",
                    _,
                    _
                ))
                .answers(&|_, _, _, input| {
                    // previously, it was assumed that the data flow analysis was smart enough
                    // to not select "items", and leave it empty, since it's empty from the GraphQL PoV.
                    // But there are many factors that complicates this:
                    // 1. Since an outer (which is external!) mapping is used in the query, the compiler has no knowledge about the data flow.
                    // 2. We should probably not try to be very clever about messing with GraphQL selection.
                    assert!(input.windows(6).any(|subslice| subslice == b"\"guid\""));

                    // Inject "title"
                    Ok(br#"{
                        "title": "http",
                        "link": "",
                        "username": "",
                        "items": []
                    }"#
                    .to_vec())
                }),
        ),
        ds,
    )
    .await;

    // create the article
    test.create_db_article_for_teh_user("The title").await;

    expect_eq!(
        actual = "
        {
            feed(input: { username: \"teh_user\" }) {
                title
            }
        }"
        .exec([], &test.feed_schema, &test.ctx())
        .await,
        expected = Ok(graphql_value!({
            "feed": {
                "title": "http"
            }
        })),
    );
}

#[datastore_test(tokio::test)]
async fn feed_public_with_items_query(ds: &str) {
    let test = ConduitBundle::new(
        (
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
        ),
        ds,
    )
    .await;

    test.create_db_article_for_teh_user("The title").await;

    expect_eq!(
        actual = "
        {
            feed(input: { username: \"teh_user\" }) {
                title
                items { guid }
            }
        }"
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

#[datastore_test(tokio::test)]
async fn feed_public_is_immutable(ds: &str) {
    let test = ConduitBundle::new((), ds).await;

    assert!(test.feed_schema.schema.mutation_type().is_none());
}
