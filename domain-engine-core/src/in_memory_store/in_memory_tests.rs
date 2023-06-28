use ontol_runtime::config::DataStoreConfig;
use ontol_test_utils::{
    assert_error_msg, expect_eq, type_binding::TypeBinding, SourceName, TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;

use crate::{DomainEngine, EngineAPI};

const CONDUIT_DB: &str = include_str!("../../../examples/conduit/conduit_db.on");
const ARTIST_AND_INSTRUMENT: &str = include_str!("../../../examples/artist_and_instrument.on");

#[test(tokio::test)]
async fn test_conduit_db_in_memory_id_generation() {
    TestPackages::with_sources([(SourceName::root(), CONDUIT_DB)])
        .with_data_store(SourceName::root(), DataStoreConfig::InMemory)
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::new(test_env.env.clone());
            let [user, article, comment, tag_entity] =
                TypeBinding::new_n(&test_env, ["User", "Article", "Comment", "TagEntity"]);

            domain_engine
                .store_entity(
                    user.de_create()
                        .value(json!({
                            "username": "u1",
                            "email": "a@b",
                            "password_hash": "s3cr3t",
                        }))
                        .unwrap(),
                )
                .await
                .unwrap();

            let explicit_user_id = domain_engine
                .store_entity(
                    // Store with the Read processor which supports specifying ID upfront
                    user.de_read()
                        .value(json!({
                            "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                            "username": "u2",
                            "email": "c@d",
                            "password_hash": "s3cr3t",
                        }))
                        .unwrap(),
                )
                .await
                .unwrap();

            expect_eq!(
                actual = format!("{:?}", explicit_user_id.data),
                expected = "Uuid(67e55044-10b1-426f-9247-bb680e5fe0c8)"
            );

            domain_engine
                .store_entity(
                    article
                        .de_create()
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
                )
                .await
                .unwrap();

            domain_engine
                .store_entity(
                    comment
                        .de_create()
                        .value(json!({
                            "body": "Comment body",
                            "author": {
                                "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                            },
                            "createdAt": "2023-01-25T19:00:15.149284864+00:00",
                            "updatedAt": "2033-01-25T19:00:15.149284864+00:00",
                        }))
                        .unwrap(),
                )
                .await
                .unwrap();

            domain_engine
                .store_entity(tag_entity.de_create().value(json!({})).unwrap())
                .await
                .unwrap();
        })
        .await;
}

#[test(tokio::test)]
async fn test_conduit_db_unresolved_foreign_key() {
    TestPackages::with_sources([(SourceName::root(), CONDUIT_DB)])
        .with_data_store(SourceName::root(), DataStoreConfig::InMemory)
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::new(test_env.env.clone());
            let article = TypeBinding::new(&test_env, "Article");

            assert_error_msg!(
                domain_engine
                    .store_entity(
                        article
                            .de_create()
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
                    )
                    .await,
                r#"Unresolved foreign key: "67e55044-10b1-426f-9247-bb680e5fe0c8""#
            );
        })
        .await;
}

#[test(tokio::test)]
async fn test_artist_and_instrument_fmt_id_generation() {
    TestPackages::with_sources([(SourceName::root(), ARTIST_AND_INSTRUMENT)])
        .with_data_store(SourceName::root(), DataStoreConfig::InMemory)
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::new(test_env.env.clone());
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
                .store_entity(
                    artist
                        .de_create()
                        .value(json!({"name": "Igor Stravinskij" }))
                        .unwrap(),
                )
                .await
                .unwrap();

            let generated_id_json = artist_id.ser_read().json(&generated_id);
            assert!(generated_id_json.as_str().unwrap().starts_with("artist/"));

            let explicit_id = domain_engine
                .store_entity(
                    artist
                        .de_read()
                        .value(json!({
                            "ID": "artist/67e55044-10b1-426f-9247-bb680e5fe0c8",
                            "name": "Karlheinz Stockhausen"
                        }))
                        .unwrap(),
                )
                .await
                .unwrap();

            expect_eq!(
                actual = artist_id.ser_read().json(&explicit_id),
                expected = json!("artist/67e55044-10b1-426f-9247-bb680e5fe0c8")
            );
        })
        .await;
}
