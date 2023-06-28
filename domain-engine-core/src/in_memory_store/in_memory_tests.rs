use ontol_runtime::config::DataStoreConfig;
use ontol_test_utils::{
    expect_eq, type_binding::TypeBinding, SourceName, TestCompile, TestPackages,
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
async fn test_artist_and_instrument_fmt_id_generation() {
    TestPackages::with_sources([(SourceName::root(), ARTIST_AND_INSTRUMENT)])
        .with_data_store(SourceName::root(), DataStoreConfig::InMemory)
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::new(test_env.env.clone());
            let [artist, _instrument] = TypeBinding::new_n(&test_env, ["artist", "instrument"]);

            let _artist_id = domain_engine
                .store_entity(
                    artist
                        .de_create()
                        .value(json!({"name": "Igor Stravinskij" }))
                        .unwrap(),
                )
                .await
                .unwrap();
        })
        .await;
}
