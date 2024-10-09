use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use domain_engine_core::{transact::ReqMessage, DomainEngine, Session};
use domain_engine_test_utils::{
    mock_datastore::{LinearDataStoreAdapter, LinearTransactMock},
    unimock::{self, matching, MockFn, Unimock},
};
use indoc::indoc;
use ontol_examples::artist_and_instrument;
use ontol_macros::test;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::{TestCompile, TestPackages};
use pretty_assertions::assert_eq;

use crate::OntologyCatalogProvider;

#[test(tokio::test)]
async fn test_where_text_equals() {
    let test = TestPackages::with_static_sources([artist_and_instrument()]).compile();
    let unimock = Unimock::new(verify_transact_filter(indoc! { "
        (root $a)
        (match-prop $a p@1:1:1 (element-in $b))
        (member $b (_ 'foo'))
    "}));

    let dataframe = datafusion_ctx(mock_engine(&unimock, test.ontology_owned()))
        .sql(r#"SELECT "ID", name FROM ontology.artist_and_instrument.artist WHERE name = 'foo'"#)
        .await
        .unwrap();

    dataframe.collect().await.unwrap();
}

fn verify_transact_filter(expected: &'static str) -> impl unimock::Clause {
    LinearTransactMock::transact
        .next_call(matching!(_))
        .answers_arc(Arc::new(move |_, _, messages, _| {
            let Some(Ok(ReqMessage::Query(_, entity_select))) = messages.first() else {
                panic!()
            };
            assert_eq!(expected, entity_select.filter.condition().to_string());

            Ok(vec![])
        }))
}

fn mock_engine(unimock: &Unimock, ontology: Arc<Ontology>) -> Arc<DomainEngine> {
    Arc::new(
        DomainEngine::builder(ontology)
            .system(Box::new(unimock.clone()))
            .build_sync(
                LinearDataStoreAdapter::new(unimock.clone()),
                Session::default(),
            )
            .unwrap(),
    )
}

fn datafusion_ctx(engine: Arc<DomainEngine>) -> SessionContext {
    let mut config = SessionConfig::new();
    config.set_extension(Arc::new(Session::default()));
    let ctx = SessionContext::new_with_config(config);
    ctx.register_catalog(
        "ontology",
        Arc::new(OntologyCatalogProvider::from(engine.clone())),
    );
    ctx
}
