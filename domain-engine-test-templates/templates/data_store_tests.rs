use domain_engine_core::DomainEngine;
use ontol_runtime::{config::DataStoreConfig, select::Select};
use ontol_test_utils::{
    assert_error_msg,
    examples::{conduit::CONDUIT_DB, Root, ARTIST_AND_INSTRUMENT},
    expect_eq,
    serde_utils::{create_de, read_de, read_ser},
    type_binding::TypeBinding,
    SourceName, TestCompile, TestPackages,
};
use serde_json::json;
use test_log::test;
use uuid::Uuid;

fn conduit_db() -> TestPackages {
    TestPackages::with_sources([CONDUIT_DB.root()])
        .with_data_store(SourceName::root(), DataStoreConfig::Default)
}

fn artist_and_instrument() -> TestPackages {
    TestPackages::with_sources([ARTIST_AND_INSTRUMENT.root()])
        .with_data_store(SourceName::root(), DataStoreConfig::Default)
}

#[test(tokio::test)]
async fn test_conduit_db_id_generation() {
    let test = conduit_db().compile();
    let domain_engine = DomainEngine::test_builder(test.ontology.clone())
        .build::<crate::TestDataStoreFactory>()
        .await;
    let [user, article, comment, tag_entity] =
        test.bind(["User", "Article", "Comment", "TagEntity"]);

    domain_engine
        .store_new_entity(
            create_de(&user)
                .to_value(json!({
                    "username": "u1",
                    "email": "a@b",
                    "password_hash": "s3cr3t",
                }))
                .unwrap(),
            Select::EntityId,
        )
        .await
        .unwrap();

    let explicit_user_id = domain_engine
        .store_new_entity(
            // Store with the Read processor which supports specifying ID upfront
            read_de(&user)
                .to_value(json!({
                    "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                    "username": "u2",
                    "email": "c@d",
                    "password_hash": "s3cr3t",
                }))
                .unwrap(),
            Select::EntityId,
        )
        .await
        .unwrap();

    expect_eq!(
                actual = format!("{:?}", explicit_user_id.data),
                expected = "OctetSequence([103, 229, 80, 68, 16, 177, 66, 111, 146, 71, 187, 104, 14, 95, 224, 200])"
            );

    let article_id: Uuid = domain_engine
        .store_new_entity(
            create_de(&article)
                .to_value(json!({
                    "slug": "foo",
                    "title": "Foo",
                    "description": "An article",
                    "body": "The body",
                    "author": {
                        "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                    }
                }))
                .unwrap(),
            Select::EntityId,
        )
        .await
        .unwrap()
        .cast_into();

    domain_engine
        .store_new_entity(
            create_de(&comment)
                .to_value(json!({
                    "body": "Comment body",
                    "author": {
                        "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                    },
                    "comment_on": {
                        "article_id": article_id.to_string()
                    }
                }))
                .unwrap(),
            Select::EntityId,
        )
        .await
        .unwrap();

    domain_engine
        .store_new_entity(
            create_de(&tag_entity)
                .to_value(json!({ "tag": "foo" }))
                .unwrap(),
            Select::EntityId,
        )
        .await
        .unwrap();
}

#[test(tokio::test)]
async fn test_conduit_db_store_entity_tree() {
    let test = conduit_db().compile();
    let domain_engine = DomainEngine::test_builder(test.ontology.clone())
        .build::<crate::TestDataStoreFactory>()
        .await;
    let [user_type, article_type, comment_type] = test.bind(["User", "Article", "Comment"]);

    let pre_existing_user_id: Uuid = domain_engine
        .store_new_entity(
            create_de(&user_type)
                .to_value(json!({
                    "username": "pre-existing",
                    "email": "pre@existing",
                    "password_hash": "s3cr3t",
                }))
                .unwrap(),
            Select::EntityId,
        )
        .await
        .unwrap()
        .cast_into();

    let article_id: Uuid = domain_engine
        .store_new_entity(
            create_de(&article_type)
                .to_value(json!({
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
            Select::EntityId,
        )
        .await
        .unwrap()
        .cast_into();

    let users = domain_engine
        .query_entities(user_type.struct_select([]).into())
        .await
        .unwrap();

    let new_user_id = users[1]
        .value
        .get_attribute(user_type.find_property("user_id").unwrap())
        .unwrap()
        .value
        .clone()
        .cast_into::<Uuid>();

    expect_eq!(
        actual = read_ser(&user_type).as_json(
            &domain_engine
                .query_entities(
                    user_type
                        .struct_select([("authored_articles", Select::Leaf)])
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
        actual = read_ser(&user_type).as_json(
            &domain_engine
                .query_entities(
                    user_type
                        .struct_select([(
                            "authored_articles",
                            article_type
                                .struct_select([(
                                    "comments",
                                    comment_type.struct_select([]).into()
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
                    "created_at": "1970-01-01T00:00:00+00:00",
                    "updated_at": "1970-01-01T00:00:00+00:00",
                    "comments": [
                        {
                            "id": 0,
                            "body": "First post!",
                            "author": {
                                "user_id": pre_existing_user_id.to_string(),
                            },
                            "created_at": "1970-01-01T00:00:00+00:00",
                            "updated_at": "1970-01-01T00:00:00+00:00",
                        }
                    ]
                }
            ]
        })
    );
}

#[test(tokio::test)]
async fn test_conduit_db_unresolved_foreign_key() {
    let test = conduit_db().compile();
    let domain_engine = DomainEngine::test_builder(test.ontology.clone())
        .build::<crate::TestDataStoreFactory>()
        .await;
    let [article] = test.bind(["Article"]);

    assert_error_msg!(
        domain_engine
            .store_new_entity(
                create_de(&article)
                    .to_value(json!({
                        "slug": "foo",
                        "title": "Foo",
                        "description": "An article",
                        "body": "The body",
                        "author": {
                            "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                        }
                    }))
                    .unwrap(),
                Select::EntityId
            )
            .await,
        r#"Unresolved foreign key: "67e55044-10b1-426f-9247-bb680e5fe0c8""#
    );
}

#[test(tokio::test)]
async fn test_artist_and_instrument_fmt_id_generation() {
    let test = artist_and_instrument().compile();
    let domain_engine = DomainEngine::test_builder(test.ontology.clone())
        .build::<crate::TestDataStoreFactory>()
        .await;
    let [artist] = test.bind(["artist"]);
    let artist_id = TypeBinding::from_def_id(
        artist
            .type_info
            .entity_info
            .as_ref()
            .unwrap()
            .id_value_def_id,
        &test.ontology,
    );

    let generated_id = domain_engine
        .store_new_entity(
            create_de(&artist)
                .to_value(json!({"name": "Igor Stravinskij" }))
                .unwrap(),
            Select::EntityId,
        )
        .await
        .unwrap();

    let generated_id_json = read_ser(&artist_id).as_json(&generated_id);
    assert!(generated_id_json.as_str().unwrap().starts_with("artist/"));

    let explicit_id = domain_engine
        .store_new_entity(
            read_de(&artist)
                .to_value(json!({
                    "ID": "artist/67e55044-10b1-426f-9247-bb680e5fe0c8",
                    "name": "Karlheinz Stockhausen"
                }))
                .unwrap(),
            Select::EntityId,
        )
        .await
        .unwrap();

    expect_eq!(
        actual = read_ser(&artist_id).as_json(&explicit_id),
        expected = json!("artist/67e55044-10b1-426f-9247-bb680e5fe0c8")
    );
}
