use ontol_runtime::{config::DataStoreConfig, query::Query};
use ontol_test_utils::{
    assert_error_msg, expect_eq,
    serde_utils::{create_de, read_de, read_ser},
    type_binding::TypeBinding,
    SourceName, TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;
use uuid::Uuid;

use crate::domain_engine::DomainEngine;

fn conduit_db() -> TestPackages {
    TestPackages::with_sources([(
        SourceName::root(),
        include_str!("../../../examples/conduit/conduit_db.on"),
    )])
    .with_data_store(SourceName::root(), DataStoreConfig::InMemory)
}

fn artist_and_instrument() -> TestPackages {
    TestPackages::with_sources([(
        SourceName::root(),
        include_str!("../../../examples/artist_and_instrument.on"),
    )])
    .with_data_store(SourceName::root(), DataStoreConfig::InMemory)
}

#[test(tokio::test)]
async fn test_conduit_db_in_memory_id_generation() {
    conduit_db()
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::builder(test_env.env.clone()).build();
            let [user, article, comment, tag_entity] =
                TypeBinding::new_n(&test_env, ["User", "Article", "Comment", "TagEntity"]);

            domain_engine
                .store_new_entity(
                    create_de(&user)
                        .value(json!({
                            "username": "u1",
                            "email": "a@b",
                            "password_hash": "s3cr3t",
                        }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap();

            let explicit_user_id = domain_engine
                .store_new_entity(
                    // Store with the Read processor which supports specifying ID upfront
                    read_de(&user)
                        .value(json!({
                            "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                            "username": "u2",
                            "email": "c@d",
                            "password_hash": "s3cr3t",
                        }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap();

            expect_eq!(
                actual = format!("{:?}", explicit_user_id.data),
                expected = "Uuid(67e55044-10b1-426f-9247-bb680e5fe0c8)"
            );

            let article_id: Uuid = domain_engine
                .store_new_entity(
                    create_de(&article)
                        .value(json!({
                            "slug": "foo",
                            "title": "Foo",
                            "description": "An article",
                            "body": "The body",
                            "author": {
                                "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                            }
                        }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap()
                .cast_into();

            domain_engine
                .store_new_entity(
                    create_de(&comment)
                        .value(json!({
                            "body": "Comment body",
                            "author": {
                                "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                            },
                            "comment_on": {
                                "article_id": article_id.to_string()
                            }
                        }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap();

            domain_engine
                .store_new_entity(
                    create_de(&tag_entity)
                        .value(json!({ "tag": "foo" }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap();
        })
        .await;
}

#[test(tokio::test)]
async fn test_conduit_db_store_entity_tree() {
    conduit_db()
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::builder(test_env.env.clone()).build();
            let [user_type, article_type, comment_type] =
                TypeBinding::new_n(&test_env, ["User", "Article", "Comment"]);

            let pre_existing_user_id: Uuid = domain_engine
                .store_new_entity(
                    create_de(&user_type)
                        .value(json!({
                            "username": "pre-existing",
                            "email": "pre@existing",
                            "password_hash": "s3cr3t",
                        }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap()
                .cast_into();

            let article_id: Uuid = domain_engine
                .store_new_entity(
                    create_de(&article_type)
                        .value(json!({
                            "slug": "foo",
                            "title": "Foo",
                            "description": "An article",
                            "body": "The body",
                            "author": {
                                "username": "new_user",
                                "email": "new@user",
                                "password_hash": "s3cr3t",
                                "bio": "New bio",
                                "following": [
                                    {
                                        "user_id": pre_existing_user_id.to_string(),
                                    }
                                ]
                            },
                            "comments": [
                                {
                                    "body": "First post!",
                                    "author": {
                                        "user_id": pre_existing_user_id.to_string()
                                    }
                                },
                            ]
                        }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap()
                .cast_into();

            let users = domain_engine
                .query_entities(user_type.struct_query([]).into())
                .await
                .unwrap();

            let new_user_id = users[1]
                .value
                .get_attribute(user_type.find_property("user_id").unwrap())
                .unwrap()
                .value
                .cast_ref::<Uuid>();

            expect_eq!(
                actual = read_ser(&user_type).json(
                    &domain_engine
                        .query_entities(
                            user_type
                                .struct_query([("authored_articles", Query::Leaf)])
                                .into(),
                        )
                        .await
                        .unwrap()[1]
                        .value
                ),
                expected = json!({
                    "user_id": new_user_id.to_string(),
                    "username": "new_user",
                    "email": "new@user",
                    "password_hash": "s3cr3t",
                    "bio": "New bio",
                    "authored_articles": [
                        { "article_id": article_id.to_string() }
                    ]
                })
            );

            expect_eq!(
                actual = read_ser(&user_type).json(
                    &domain_engine
                        .query_entities(
                            user_type
                                .struct_query([(
                                    "authored_articles",
                                    article_type
                                        .struct_query([(
                                            "comments",
                                            comment_type.struct_query([]).into()
                                        )])
                                        .into()
                                )])
                                .into(),
                        )
                        .await
                        .unwrap()[1]
                        .value
                ),
                expected = json!({
                    "user_id": new_user_id.to_string(),
                    "username": "new_user",
                    "email": "new@user",
                    "password_hash": "s3cr3t",
                    "bio": "New bio",
                    "authored_articles": [
                        {
                            "article_id": article_id.to_string(),
                            "slug": "foo",
                            "title": "Foo",
                            "description": "An article",
                            "body": "The body",
                            "comments": [
                                {
                                    "id": 0,
                                    "body": "First post!",
                                    "author": {
                                        "user_id": pre_existing_user_id.to_string(),
                                    }
                                }
                            ]
                        }
                    ]
                })
            );
        })
        .await;
}

#[test(tokio::test)]
async fn test_conduit_db_unresolved_foreign_key() {
    conduit_db()
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::builder(test_env.env.clone()).build();
            let article = TypeBinding::new(&test_env, "Article");

            assert_error_msg!(
                domain_engine
                    .store_new_entity(
                        create_de(&article)
                            .value(json!({
                                "slug": "foo",
                                "title": "Foo",
                                "description": "An article",
                                "body": "The body",
                                "author": {
                                    "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                                }
                            }))
                            .unwrap(),
                        Query::EntityId
                    )
                    .await,
                r#"Unresolved foreign key: "67e55044-10b1-426f-9247-bb680e5fe0c8""#
            );
        })
        .await;
}

#[test(tokio::test)]
async fn test_artist_and_instrument_fmt_id_generation() {
    artist_and_instrument()
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::builder(test_env.env.clone()).build();
            let artist = TypeBinding::new(&test_env, "artist");
            let artist_id = TypeBinding::from_def_id(
                artist
                    .type_info
                    .entity_info
                    .as_ref()
                    .unwrap()
                    .id_value_def_id,
                &test_env.env,
            );

            let generated_id = domain_engine
                .store_new_entity(
                    create_de(&artist)
                        .value(json!({"name": "Igor Stravinskij" }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap();

            let generated_id_json = read_ser(&artist_id).json(&generated_id);
            assert!(generated_id_json.as_str().unwrap().starts_with("artist/"));

            let explicit_id = domain_engine
                .store_new_entity(
                    read_de(&artist)
                        .value(json!({
                            "ID": "artist/67e55044-10b1-426f-9247-bb680e5fe0c8",
                            "name": "Karlheinz Stockhausen"
                        }))
                        .unwrap(),
                    Query::EntityId,
                )
                .await
                .unwrap();

            expect_eq!(
                actual = read_ser(&artist_id).json(&explicit_id),
                expected = json!("artist/67e55044-10b1-426f-9247-bb680e5fe0c8")
            );
        })
        .await;
}
