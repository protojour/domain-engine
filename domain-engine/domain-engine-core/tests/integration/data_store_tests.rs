use domain_engine_core::{DomainEngine, Session};
use domain_engine_test_utils::{
    dynamic_data_store::DynamicDataStoreFactory, system::mock_current_time_monotonic, unimock,
    DomainEngineTestExt, TestFindQuery,
};
use ontol_runtime::{
    attr::AttrRef,
    interface::serde::processor::{ProcessorProfile, ProcessorProfileFlags},
    ontology::Ontology,
    query::select::Select,
    value::Serial,
};
use ontol_test_utils::{
    assert_error_msg,
    def_binding::DefBinding,
    examples::{conduit::CONDUIT_DB, ARTIST_AND_INSTRUMENT},
    expect_eq,
    json_utils::{json_map, json_prop},
    serde_helper::{serde_create, serde_read},
    TestCompile, TestPackages,
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

fn conduit_db() -> TestPackages {
    TestPackages::with_static_sources([CONDUIT_DB])
}

fn artist_and_instrument() -> TestPackages {
    TestPackages::with_static_sources([ARTIST_AND_INSTRUMENT])
}

async fn make_domain_engine(
    ontology: Arc<Ontology>,
    mock_clause: impl unimock::Clause,
    data_store: &str,
) -> DomainEngine {
    DomainEngine::builder(ontology)
        .system(Box::new(unimock::Unimock::new(mock_clause)))
        .build(DynamicDataStoreFactory::new(data_store), Session::default())
        .await
        .unwrap()
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_db_remigrate_noop(ds: &str) {
    let test = conduit_db().compile();
    let domain_engine = DomainEngine::builder(test.ontology_owned())
        .system(Box::new(unimock::Unimock::new(())))
        .build(DynamicDataStoreFactory::new(ds), Session::default())
        .await
        .unwrap();

    drop(domain_engine);

    let domain_engine = DomainEngine::builder(test.ontology_owned())
        .system(Box::new(unimock::Unimock::new(())))
        .build(
            DynamicDataStoreFactory::new(ds).reuse_db(),
            Session::default(),
        )
        .await
        .unwrap();

    drop(domain_engine);
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_conduit_db_id_generation(ds: &str) {
    let test = conduit_db().compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [user, article, comment, tag_entity] =
        test.bind(["User", "Article", "Comment", "TagEntity"]);

    engine
        .store_new_entity(
            serde_create(&user)
                .to_value(json!({
                    "username": "u1",
                    "email": "a@b",
                    "password_hash": "s3cr3t",
                }))
                .unwrap(),
            Select::EntityId,
            Session::default(),
        )
        .await
        .unwrap();

    let explicit_user_id = engine
        .store_new_entity(
            // Store with the Read processor which supports specifying ID upfront
            serde_read(&user)
                .to_value(json!({
                    "user_id": "67e55044-10b1-426f-9247-bb680e5fe0c8",
                    "username": "u2",
                    "email": "c@d",
                    "password_hash": "s3cr3t",
                }))
                .unwrap(),
            Select::EntityId,
            Session::default(),
        )
        .await
        .unwrap();

    expect_eq!(
        actual = format!("{:?}", explicit_user_id),
        expected = "OctetSequence([103, 229, 80, 68, 16, 177, 66, 111, 146, 71, 187, 104, 14, 95, 224, 200], tag(def@1:1, Some(TagFlags(0x0))))"
    );

    let article_id: Uuid = engine
        .store_new_entity(
            serde_create(&article)
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
            Session::default(),
        )
        .await
        .unwrap()
        .cast_into();

    engine
        .store_new_entity(
            serde_create(&comment)
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
            Session::default(),
        )
        .await
        .unwrap();

    engine
        .store_new_entity(
            serde_create(&tag_entity)
                .to_value(json!({ "tag": "foo" }))
                .unwrap(),
            Select::EntityId,
            Session::default(),
        )
        .await
        .unwrap();
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_conduit_db_store_entity_tree(ds: &str) {
    let test = conduit_db().compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [user_def, article_def, comment_def] = test.bind(["User", "Article", "Comment"]);

    let pre_existing_user_id: Uuid = engine
        .store_new_entity(
            serde_create(&user_def)
                .to_value(json!({
                    "username": "pre-existing",
                    "email": "pre@existing",
                    "password_hash": "s3cr3t",
                }))
                .unwrap(),
            Select::EntityId,
            Session::default(),
        )
        .await
        .unwrap()
        .cast_into();

    let article_id: Uuid = engine
        .store_new_entity(
            serde_create(&article_def)
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
            Session::default(),
        )
        .await
        .unwrap()
        .cast_into();

    let users = engine
        .query_entities(user_def.struct_select([]).into(), Session::default())
        .await
        .unwrap();

    let new_user_id = users.elements()[1]
        .get_attribute(user_def.find_property("user_id").unwrap())
        .unwrap()
        .as_unit()
        .unwrap()
        .clone()
        .cast_into::<Uuid>();

    expect_eq!(
        actual = serde_read(&user_def).as_json(AttrRef::Unit(
            &engine
                .query_entities(
                    user_def
                        .struct_select([("authored_articles", Select::Leaf)])
                        .into(),
                    Session::default(),
                )
                .await
                .unwrap()
                .elements()[1]
        )),
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

    let comments = engine
        .query_entities(comment_def.struct_select([]).into(), Session::default())
        .await
        .unwrap();

    let comment_id = comments.elements()[0]
        .get_attribute(comment_def.find_property("id").unwrap())
        .unwrap()
        .as_unit()
        .unwrap()
        .clone()
        .cast_into::<Serial>();
    let comment_id = format!("{}", comment_id.0);

    expect_eq!(
        actual = serde_read(&user_def)
            // This query intentionally violates the domain's JSON schema:
            .with_profile(ProcessorProfile {
                flags: ProcessorProfileFlags::ALL_PROPS_OPTIONAL,
                ..Default::default()
            })
            .as_json(AttrRef::Unit(
                &engine
                    .query_entities(
                        user_def
                            .struct_select([(
                                "authored_articles",
                                article_def
                                    .struct_select([(
                                        "comments",
                                        comment_def.struct_select([]).into()
                                    )])
                                    .into()
                            )])
                            .into(),
                        Session::default(),
                    )
                    .await
                    .unwrap()
                    .elements()[1]
            )),
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
                    "created_at": "1971-01-01T00:00:00Z",
                    "updated_at": "1971-01-01T00:00:00Z",
                    "comments": [
                        {
                            "id": comment_id,
                            "body": "First post!",
                            "created_at": "1971-01-01T00:00:00Z",
                            "updated_at": "1971-01-01T00:00:00Z",
                        }
                    ]
                }
            ]
        })
    );
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_conduit_db_unresolved_foreign_key(ds: &str) {
    let test = conduit_db().compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [article] = test.bind(["Article"]);

    assert_error_msg!(
        engine
            .store_new_entity(
                serde_create(&article)
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
                Session::default(),
            )
            .await,
        r#"unresolved foreign key: "67e55044-10b1-426f-9247-bb680e5fe0c8""#
    );
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_artist_and_instrument_fmt_id_generation(ds: &str) {
    let test = artist_and_instrument().compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [artist] = test.bind(["artist"]);
    let artist_id = DefBinding::from_def_id(
        artist.def.entity().unwrap().id_value_def_id,
        test.ontology(),
    );

    let generated_id = engine
        .store_new_entity(
            serde_create(&artist)
                .to_value(json!({"name": "Igor Stravinskij" }))
                .unwrap(),
            Select::EntityId,
            Session::default(),
        )
        .await
        .unwrap();

    let generated_id_json = serde_read(&artist_id).as_json(AttrRef::Unit(&generated_id));
    assert!(generated_id_json.as_str().unwrap().starts_with("artist/"));

    let explicit_id = engine
        .store_new_entity(
            serde_read(&artist)
                .to_value(json!({
                    "ID": "artist/67e55044-10b1-426f-9247-bb680e5fe0c8",
                    "name": "Karlheinz Stockhausen"
                }))
                .unwrap(),
            Select::EntityId,
            Session::default(),
        )
        .await
        .unwrap();

    expect_eq!(
        actual = serde_read(&artist_id).as_json(AttrRef::Unit(&explicit_id)),
        expected = json!("artist/67e55044-10b1-426f-9247-bb680e5fe0c8")
    );
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_artist_and_instrument_pagination(ds: &str) {
    let test = artist_and_instrument().compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [artist] = test.bind(["artist"]);

    let entities = vec![
        json!({ "name": "Larry Young" }),
        json!({ "name": "Woody Shaw" }),
        json!({ "name": "Joe Henderson" }),
    ];

    for json in &entities {
        engine
            .store_new_entity(
                serde_create(&artist).to_value(json.clone()).unwrap(),
                Select::EntityId,
                Session::default(),
            )
            .await
            .unwrap();
    }

    fn extract_names(value: serde_json::Value) -> serde_json::Value {
        json_map(value, |value| json_prop(value, "name"))
    }

    let by_name = (artist.def_id().package_id(), "artists");

    expect_eq!(
        actual = extract_names(
            engine
                .exec_named_map_json(by_name, json!({}), TestFindQuery::default().limit(1))
                .await
        ),
        expected = json!(["Larry Young"])
    );
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_artist_and_instrument_filter_condition(ds: &str) {
    let test = artist_and_instrument().compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [artist] = test.bind(["artist"]);

    let entities = vec![
        json!({ "name": "Larry Young" }),
        json!({ "name": "Woody Shaw" }),
        json!({ "name": "Joe Henderson" }),
    ];

    for json in &entities {
        engine
            .store_new_entity(
                serde_create(&artist).to_value(json.clone()).unwrap(),
                Select::EntityId,
                Session::default(),
            )
            .await
            .unwrap();
    }

    fn extract_names(value: serde_json::Value) -> serde_json::Value {
        json_map(value, |value| json_prop(value, "name"))
    }

    let by_name = (artist.def_id().package_id(), "artists_by_name");

    expect_eq!(
        actual = extract_names(
            engine
                .exec_named_map_json(by_name, json!({ "name": "N/A" }), TestFindQuery::default())
                .await
        ),
        expected = json!([])
    );

    expect_eq!(
        actual = extract_names(
            engine
                .exec_named_map_json(
                    by_name,
                    json!({ "name": "Larry Young" }),
                    TestFindQuery::default()
                )
                .await
        ),
        expected = json!(["Larry Young"])
    );
}
