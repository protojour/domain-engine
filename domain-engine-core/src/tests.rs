use ontol_runtime::config::DataSourceConfig;
use ontol_test_utils::{type_binding::TypeBinding, SourceName, TestCompile, TestPackages};
use serde_json::json;
use test_log::test;

use crate::{DomainEngine, EngineAPI};

const CONDUIT_DB: &str = include_str!("../../examples/conduit/conduit_db.on");

#[test(tokio::test)]
async fn test_conduit_db_in_memory_data_source() {
    TestPackages::with_sources([(SourceName::root(), CONDUIT_DB)])
        .with_data_source(SourceName::root(), DataSourceConfig::InMemory)
        .compile_ok_async(|test_env| async move {
            let domain_engine = DomainEngine::new(test_env.env.clone());
            let [user, article, comment, tag_entity] =
                TypeBinding::new_n(&test_env, ["User", "Article", "Comment", "TagEntity"]);

            let _user_id1 = domain_engine
                .store_entity(
                    user.de_create()
                        .value(json!({
                            "username": "u",
                            "email": "a@b",
                            "password_hash": "s3cr3t",
                        }))
                        .unwrap(),
                )
                .await
                .unwrap();

            // FIXME: Inherent ID?
            // domain_engine
            //     .store_entity(
            //         user.deserialize_value(json!({
            //             "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
            //             "username": "u",
            //             "email": "a@b",
            //             "password_hash": "s3cr3t",
            //         }))
            //         .unwrap(),
            //     )
            //     .await
            //     .unwrap();

            let _article_id1 = domain_engine
                .store_entity(
                    article
                        .de_create()
                        .value(json!({
                            "slug": "foo",
                            "title": "Foo",
                            "description": "An article",
                            "body": "The body",
                            "createdAt": "2023-01-25T19:00:15.149284864+00:00",
                            "updatedAt": "2033-01-25T19:00:15.149284864+00:00",
                        }))
                        .unwrap(),
                )
                .await
                .unwrap();

            let _comment_id1 = domain_engine
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

            let _tag_id1 = domain_engine
                .store_entity(tag_entity.de_create().value(json!({})).unwrap())
                .await
                .unwrap();
        })
        .await;
}
