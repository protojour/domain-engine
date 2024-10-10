use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use domain_engine_core::{DomainEngine, Session};
use domain_engine_test_utils::{
    data_store_util, dynamic_data_store::DynamicDataStoreFactory,
    system::mock_current_time_monotonic, unimock,
};
use ontol_examples::artist_and_instrument;
use ontol_macros::datastore_test;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::{serde_helper::serde_create, TestCompile, TestPackages};
use serde_json::json;

use crate::{DomainEngineAPI, OntologyCatalogProvider};

#[datastore_test(tokio::test)]
async fn datastore_test(ds: &str) {
    let test = TestPackages::with_static_sources([artist_and_instrument()]).compile();
    let [artist] = test.bind(["artist"]);
    let engine = make_domain_engine(test.ontology_owned(), ds).await;

    let mut config = SessionConfig::new();
    config.set_extension(Arc::new(Session::default()));
    let ctx = SessionContext::new_with_config(config);
    let api: Arc<dyn DomainEngineAPI + Send + Sync> = Arc::new(engine.clone());
    ctx.register_catalog("ontology", Arc::new(OntologyCatalogProvider::from(api)));

    data_store_util::insert_entity_select_entityid(
        &engine,
        serde_create(&artist)
            .to_value(json!({
                "ID": "artist/88832e20-8c6e-46b4-af79-27b19b889a58",
                "name": "Beach Boys",
            }))
            .unwrap(),
    )
    .await
    .unwrap();

    let dataframe = ctx
        .sql("SELECT \"ID\", name FROM ontology.artist_and_instrument.artist")
        .await
        .unwrap();

    let results = dataframe.collect().await.unwrap();

    let pretty_results = datafusion::arrow::util::pretty::pretty_format_batches(&results)
        .unwrap()
        .to_string();

    let expected = vec![
        "+---------------------------------------------+------------+",
        "| ID                                          | name       |",
        "+---------------------------------------------+------------+",
        "| artist/88832e20-8c6e-46b4-af79-27b19b889a58 | Beach Boys |",
        "+---------------------------------------------+------------+",
    ];

    assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
}

async fn make_domain_engine(ontology: Arc<Ontology>, datastore: &str) -> Arc<DomainEngine> {
    Arc::new(
        DomainEngine::builder(ontology)
            .system(Box::new(unimock::Unimock::new(
                mock_current_time_monotonic(),
            )))
            .build(DynamicDataStoreFactory::new(datastore), Session::default())
            .await
            .unwrap(),
    )
}
