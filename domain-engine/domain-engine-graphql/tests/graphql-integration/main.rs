use std::sync::Arc;

use domain_engine_core::{DomainEngine, Session};
use domain_engine_test_utils::{
    dynamic_data_store::DynamicDataStoreClient, system::mock_current_time_monotonic,
};
use ontol_runtime::ontology::Ontology;

mod test_graphql_conduit;
mod test_graphql_findings;
mod test_graphql_gitmesh;
mod test_graphql_input;
mod test_graphql_misc;
mod test_graphql_ontology;
mod test_graphql_search;
mod test_graphql_smoke;
mod test_graphql_stix;
mod test_graphql_unit;

fn main() {}

async fn mk_engine_default(ontology: Arc<Ontology>, ds: &str) -> Arc<DomainEngine> {
    mk_engine_with_mock(ontology, mock_current_time_monotonic(), ds).await
}

async fn mk_engine_with_mock(
    ontology: Arc<Ontology>,
    system_mock: impl unimock::Clause,
    ds: &str,
) -> Arc<DomainEngine> {
    Arc::new(
        DomainEngine::builder(ontology)
            .system(Box::new(unimock::Unimock::new(system_mock)))
            .build(
                DynamicDataStoreClient::new(ds).connect().await.unwrap(),
                Session::default(),
            )
            .await
            .unwrap(),
    )
}
