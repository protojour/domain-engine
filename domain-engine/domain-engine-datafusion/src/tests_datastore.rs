use std::sync::Arc;

use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use bytes::Bytes;
use datafusion::prelude::{SessionConfig, SessionContext};
use domain_engine_arrow::{
    arrow_http::{http_stream_to_resp_msg_stream, resp_msg_to_http_stream},
    ArrowConfig, ArrowReqMessage, ArrowRespMessage, ArrowTransactAPI,
};
use domain_engine_core::{DomainEngine, DomainResult, Session};
use domain_engine_test_utils::{
    data_store_util, dynamic_data_store::DynamicDataStoreFactory,
    system::mock_current_time_monotonic, unimock,
};
use futures_util::{stream::BoxStream, StreamExt};
use indoc::indoc;
use ontol_examples::artist_and_instrument;
use ontol_macros::datastore_test;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::{file_url, serde_helper::serde_create, TestCompile, TestPackages};
use serde_json::json;
use tracing::info;

use crate::{DomainEngineAPI, OntologyCatalogProvider};

#[datastore_test(tokio::test)]
async fn datastore_test_artist_with_chunking(ds: &str) {
    let test = TestPackages::with_static_sources([artist_and_instrument()]).compile();
    let [artist] = test.bind(["artist"]);
    let engine = make_domain_engine(test.ontology_owned(), ds).await;

    let mut config = SessionConfig::new();
    config.set_extension(Arc::new(Session::default()));
    let ctx = SessionContext::new_with_config(config);

    // testing with the HTTP stream
    let api: Arc<dyn DomainEngineAPI + Send + Sync> = Arc::new(IPCStreamer {
        domain_engine: engine.clone(),
        rechunk_size: 100,
    });

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

    assert_eq!(
        prettify(&dataframe.collect().await.unwrap()),
        indoc! { "
            +---------------------------------------------+------------+
            | ID                                          | name       |
            +---------------------------------------------+------------+
            | artist/88832e20-8c6e-46b4-af79-27b19b889a58 | Beach Boys |
            +---------------------------------------------+------------+
        "}
    );

    info!("column subset");

    let dataframe = ctx
        .sql("SELECT \"ID\" FROM ontology.artist_and_instrument.artist")
        .await
        .unwrap();

    assert_eq!(
        prettify(&dataframe.collect().await.unwrap()),
        indoc! { "
            +---------------------------------------------+
            | ID                                          |
            +---------------------------------------------+
            | artist/88832e20-8c6e-46b4-af79-27b19b889a58 |
            +---------------------------------------------+
        "}
    );

    let dataframe = ctx
        .sql("SELECT name FROM ontology.artist_and_instrument.artist")
        .await
        .unwrap();

    assert_eq!(
        prettify(&dataframe.collect().await.unwrap()),
        indoc! { "
            +------------+
            | name       |
            +------------+
            | Beach Boys |
            +------------+
        "}
    );
}

#[datastore_test(tokio::test)]
async fn datastore_test_arrow_encoding(ds: &str) {
    let ontol_source = (
        file_url("test"),
        "
        domain ZZZZZZZZZZZTESTZZZZZZZZZZZ ()

        def entity (
            rel. 'id': (rel* is: text)
            rel* 'const': 'CONST'
        )
        ",
    );

    let test = TestPackages::with_static_sources([ontol_source]).compile();
    let [entity] = test.bind(["entity"]);
    let engine = make_domain_engine(test.ontology_owned(), ds).await;

    let mut config = SessionConfig::new();
    config.set_extension(Arc::new(Session::default()));
    let ctx = SessionContext::new_with_config(config);

    // testing with the HTTP stream
    let api: Arc<dyn DomainEngineAPI + Send + Sync> = Arc::new(IPCStreamer {
        domain_engine: engine.clone(),
        rechunk_size: 100,
    });

    ctx.register_catalog("ontology", Arc::new(OntologyCatalogProvider::from(api)));

    data_store_util::insert_entity_select_entityid(
        &engine,
        serde_create(&entity)
            .to_value(json!({
                "id": "test",
                "const": "CONST",
            }))
            .unwrap(),
    )
    .await
    .unwrap();

    let dataframe = ctx.sql("SELECT * FROM ontology.test.entity").await.unwrap();

    assert_eq!(
        prettify(&dataframe.collect().await.unwrap()),
        indoc! { "
            +------+-------+
            | id   | const |
            +------+-------+
            | test | CONST |
            +------+-------+
        "}
    );
}

fn prettify(results: &[RecordBatch]) -> String {
    let mut pretty = pretty_format_batches(results).unwrap().to_string();
    pretty.push('\n');
    pretty
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

struct IPCStreamer {
    domain_engine: Arc<DomainEngine>,
    rechunk_size: usize,
}

impl ArrowTransactAPI for IPCStreamer {
    fn arrow_transact(
        &self,
        req: ArrowReqMessage,
        config: ArrowConfig,
        session: Session,
    ) -> BoxStream<'static, DomainResult<ArrowRespMessage>> {
        let source_stream = self.domain_engine.arrow_transact(req, config, session);
        let http_stream = resp_msg_to_http_stream(source_stream);
        let rechunk_size = self.rechunk_size;

        // this is all for testing purposes, simulating re-chunking in HTTP
        fn rechunk_bytes(bytes: Bytes, rechunk_size: usize) -> Vec<Bytes> {
            let mut vec = vec![];
            for chunk in bytes.chunks(rechunk_size) {
                vec.push(Bytes::copy_from_slice(chunk))
            }

            vec
        }

        let rechunked_stream = async_stream::try_stream! {
            for await result in http_stream {
                let bytes = result?;
                for bytes in rechunk_bytes(bytes, rechunk_size) {
                    yield bytes;
                }
            }
        };
        http_stream_to_resp_msg_stream(rechunked_stream).boxed()
    }
}

impl DomainEngineAPI for IPCStreamer {
    fn ontology_defs(&self) -> &ontol_runtime::ontology::aspects::DefsAspect {
        self.domain_engine.ontology_defs()
    }
}
