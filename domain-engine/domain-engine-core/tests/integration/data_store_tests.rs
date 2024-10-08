use domain_engine_core::{DomainEngine, Session};
use domain_engine_test_utils::{
    data_store_util, dynamic_data_store::DynamicDataStoreFactory,
    system::mock_current_time_monotonic, unimock, DomainEngineTestExt, TestFindQuery,
};
use ontol_examples as examples;
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
    expect_eq,
    json_utils::{json_map, json_prop},
    serde_helper::{serde_create, serde_read},
    TestCompile, TestPackages,
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

fn conduit_db() -> TestPackages {
    TestPackages::with_static_sources([examples::conduit::conduit_db()])
}

fn artist_and_instrument() -> TestPackages {
    TestPackages::with_static_sources([examples::artist_and_instrument()])
}

async fn make_domain_engine(
    ontology: Arc<Ontology>,
    mock_clause: impl unimock::Clause,
    data_store: &str,
) -> Arc<DomainEngine> {
    Arc::new(
        DomainEngine::builder(ontology)
            .system(Box::new(unimock::Unimock::new(mock_clause)))
            .build(DynamicDataStoreFactory::new(data_store), Session::default())
            .await
            .unwrap(),
    )
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_db_remigrate_noop(ds: &str) {
    let test = examples::stix::stix_bundle().compile();
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
async fn test_db_remove_one_domain(ds: &str) {
    let test1 = TestPackages::with_static_sources([
        examples::conduit::conduit_db(),
        examples::artist_and_instrument(),
    ])
    .with_roots([
        examples::conduit::conduit_db().0,
        examples::artist_and_instrument().0,
    ])
    .compile();

    let domain_engine = DomainEngine::builder(test1.ontology_owned())
        .system(Box::new(unimock::Unimock::new(())))
        .build(DynamicDataStoreFactory::new(ds), Session::default())
        .await
        .unwrap();

    drop(domain_engine);

    // This time without ARTIST_AND_INSTRUMENT
    let test2 = TestPackages::with_static_sources([examples::conduit::conduit_db()])
        .with_roots([examples::conduit::conduit_db().0])
        .compile();

    let domain_engine = DomainEngine::builder(test2.ontology_owned())
        .system(Box::new(unimock::Unimock::new(())))
        .build(
            // important: reuse DB
            DynamicDataStoreFactory::new(ds).reuse_db(),
            Session::default(),
        )
        .await
        .unwrap();

    drop(domain_engine);
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_db_multiple_persistent_domains(ds: &str) {
    let test = TestPackages::with_static_sources([
        examples::conduit::conduit_db(),
        examples::artist_and_instrument(),
    ])
    .with_roots([
        examples::conduit::conduit_db().0,
        examples::artist_and_instrument().0,
    ])
    .compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [conduit_user, ai_artist] = test.bind(["conduit_db.User", "artist_and_instrument.artist"]);

    data_store_util::insert_entity_select_entityid(
        &engine,
        serde_create(&conduit_user)
            .to_value(json!({
                "username": "u1",
                "email": "a@b",
                "password_hash": "s3cr3t",
            }))
            .unwrap(),
    )
    .await
    .unwrap();

    data_store_util::insert_entity_select_entityid(
        &engine,
        serde_create(&ai_artist)
            .to_value(json!({
                "name": "Some Artist"
            }))
            .unwrap(),
    )
    .await
    .unwrap();
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_conduit_db_id_generation(ds: &str) {
    let test = conduit_db().compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [user, article, comment, tag_entity] =
        test.bind(["User", "Article", "Comment", "TagEntity"]);

    data_store_util::insert_entity_select_entityid(
        &engine,
        serde_create(&user)
            .to_value(json!({
                "username": "u1",
                "email": "a@b",
                "password_hash": "s3cr3t",
            }))
            .unwrap(),
    )
    .await
    .unwrap();

    let explicit_user_id = data_store_util::insert_entity_select_entityid(
        &engine,
        // Store with the Read processor which supports specifying ID upfront
        serde_read(&user)
            .to_value(json!({
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
        actual = format!("{:?}", explicit_user_id),
        expected =
            "OctetSequence(67e5504410b1426f9247bb680e5fe0c8, tag(def@1:1, Some(TagFlags(0x0))))"
    );

    let article_id: Uuid = data_store_util::insert_entity_select_entityid(
        &engine,
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
    )
    .await
    .unwrap()
    .cast_into();

    data_store_util::insert_entity_select_entityid(
        &engine,
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
    )
    .await
    .unwrap();

    data_store_util::insert_entity_select_entityid(
        &engine,
        serde_create(&tag_entity)
            .to_value(json!({ "tag": "foo" }))
            .unwrap(),
    )
    .await
    .unwrap();
}

#[ontol_macros::datastore_test(tokio::test)]
async fn test_conduit_db_store_entity_tree(ds: &str) {
    let test = conduit_db().compile();
    let engine = make_domain_engine(test.ontology_owned(), mock_current_time_monotonic(), ds).await;
    let [user_def, article_def, comment_def] = test.bind(["User", "Article", "Comment"]);

    let pre_existing_user_id: Uuid = data_store_util::insert_entity_select_entityid(
        &engine,
        serde_create(&user_def)
            .to_value(json!({
                "username": "pre-existing",
                "email": "pre@existing",
                "password_hash": "s3cr3t",
            }))
            .unwrap(),
    )
    .await
    .unwrap()
    .cast_into();

    let article_id: Uuid = data_store_util::insert_entity_select_entityid(
        &engine,
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
    )
    .await
    .unwrap()
    .cast_into();

    let users = data_store_util::query_entities(
        &engine,
        user_def
            .struct_select([("user_id", Select::Unit)])
            .into_default_domain_entity_select(),
    )
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
            &data_store_util::query_entities(
                &engine,
                user_def
                    .struct_select([
                        ("user_id", Select::Unit),
                        ("username", Select::Unit),
                        ("email", Select::Unit),
                        ("password_hash", Select::Unit),
                        ("bio", Select::Unit),
                        ("authored_articles", Select::EntityId)
                    ])
                    .into_default_domain_entity_select()
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

    let comments = data_store_util::query_entities(
        &engine,
        comment_def
            .struct_select([("id", Select::Unit)])
            .into_default_domain_entity_select(),
    )
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
                &data_store_util::query_entities(
                    &engine,
                    user_def
                        .struct_select([
                            ("user_id", Select::Unit),
                            ("username", Select::Unit),
                            ("password_hash", Select::Unit),
                            ("email", Select::Unit),
                            ("bio", Select::Unit),
                            (
                                "authored_articles",
                                article_def
                                    .struct_select([
                                        ("article_id", Select::Unit),
                                        ("title", Select::Unit),
                                        ("description", Select::Unit),
                                        ("body", Select::Unit),
                                        ("slug", Select::Unit),
                                        ("created_at", Select::Unit),
                                        ("updated_at", Select::Unit),
                                        (
                                            "comments",
                                            comment_def
                                                .struct_select([
                                                    ("id", Select::Unit),
                                                    ("body", Select::Unit),
                                                    ("created_at", Select::Unit),
                                                    ("updated_at", Select::Unit)
                                                ])
                                                .into()
                                        )
                                    ])
                                    .into()
                            )
                        ])
                        .into_default_domain_entity_select(),
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
                    "created_at": "1974-01-01T00:00:00Z",
                    "updated_at": "1974-01-01T00:00:00Z",
                    "comments": [
                        {
                            "id": comment_id,
                            "body": "First post!",
                            "created_at": "1974-01-01T00:00:00Z",
                            "updated_at": "1974-01-01T00:00:00Z",
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
        data_store_util::insert_entity_select_entityid(
            &engine,
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

    let generated_id = data_store_util::insert_entity_select_entityid(
        &engine,
        serde_create(&artist)
            .to_value(json!({"name": "Igor Stravinskij" }))
            .unwrap(),
    )
    .await
    .unwrap();

    let generated_id_json = serde_read(&artist_id).as_json(AttrRef::Unit(&generated_id));
    assert!(generated_id_json.as_str().unwrap().starts_with("artist/"));

    let explicit_id = data_store_util::insert_entity_select_entityid(
        &engine,
        serde_read(&artist)
            .to_value(json!({
                "ID": "artist/67e55044-10b1-426f-9247-bb680e5fe0c8",
                "name": "Karlheinz Stockhausen"
            }))
            .unwrap(),
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
        data_store_util::insert_entity_select_entityid(
            &engine,
            serde_create(&artist).to_value(json.clone()).unwrap(),
        )
        .await
        .unwrap();
    }

    fn extract_names(value: serde_json::Value) -> serde_json::Value {
        json_map(value, |value| json_prop(value, "name"))
    }

    let by_name = (artist.def_id().domain_index(), "artists");

    expect_eq!(
        actual = extract_names(
            engine
                .exec_named_map_json(
                    by_name,
                    json!({}),
                    TestFindQuery::new(test.ontology_owned()).limit(1)
                )
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
        data_store_util::insert_entity_select_entityid(
            &engine,
            serde_create(&artist).to_value(json.clone()).unwrap(),
        )
        .await
        .unwrap();
    }

    fn extract_names(value: serde_json::Value) -> serde_json::Value {
        json_map(value, |value| json_prop(value, "name"))
    }

    let by_name = (artist.def_id().domain_index(), "artists_by_name");

    expect_eq!(
        actual = extract_names(
            engine
                .exec_named_map_json(
                    by_name,
                    json!({ "name": "N/A" }),
                    TestFindQuery::new(test.ontology_owned())
                )
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
                    TestFindQuery::new(test.ontology_owned())
                )
                .await
        ),
        expected = json!(["Larry Young"])
    );
}
