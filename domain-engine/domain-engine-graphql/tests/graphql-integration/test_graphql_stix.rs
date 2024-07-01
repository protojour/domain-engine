use std::sync::Arc;

use domain_engine_core::{DomainEngine, Session};
use domain_engine_graphql::{context::ServiceCtx, juniper::graphql_value};
use domain_engine_test_utils::dynamic_data_store::DynamicDataStoreFactory;
use domain_engine_test_utils::{
    graphql_test_utils::{Exec, TestCompileSchema},
    system::mock_current_time_monotonic,
    unimock,
};
use ontol_macros::datastore_test;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::{
    examples::stix::{stix_bundle, STIX},
    expect_eq,
};

async fn make_domain_engine(ontology: Arc<Ontology>, datastore: &str) -> DomainEngine {
    DomainEngine::builder(ontology)
        .system(Box::new(unimock::Unimock::new(
            mock_current_time_monotonic(),
        )))
        .build(DynamicDataStoreFactory::new(datastore), Session::default())
        .await
        .unwrap()
}

/// There should only be one stix test since the domain is so big
#[datastore_test(tokio::test)]
async fn test_graphql_stix(ds: &str) {
    let (test, [schema]) = stix_bundle().compile_schemas([STIX.0]);
    let ctx: ServiceCtx = make_domain_engine(test.ontology_owned(), ds).await.into();

    expect_eq!(
        actual = r#"mutation {
            url(create:[
                {
                    type: "url"
                    value: "http://jøkkagnork"
                    defanged:true
                    object_marking_refs: []
                    granular_markings: []
                }
            ]) {
                node { defanged }
            }
        }
        "#
        .exec([], &schema, &ctx)
        .await,
        expected = Ok(graphql_value!({
            "url": [{
                "node": {
                    "defanged": true
                }
            }]
        })),
    );
}
