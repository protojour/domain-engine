use std::sync::Arc;

use axum::body::Body;
use domain_engine_core::{DomainEngine, Session};
use domain_engine_httpjson::create_httpjson_router;
use domain_engine_test_utils::{
    dummy_session::DummySession, dynamic_data_store::DynamicDataStoreFactory,
    system::mock_current_time_monotonic, unimock,
};
use http::StatusCode;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::{OntolTest, SrcName};

mod test_httpjson_stix;

trait MakeTestRouter {
    fn make_test_router(
        &self,
        domain_engine: Arc<DomainEngine>,
        source_name: SrcName,
    ) -> axum::Router;
}

impl MakeTestRouter for OntolTest {
    fn make_test_router(
        &self,
        domain_engine: Arc<DomainEngine>,
        source_name: SrcName,
    ) -> axum::Router {
        create_httpjson_router::<(), DummySession>(
            domain_engine.clone(),
            self.get_package_id(source_name.as_str()),
        )
        .unwrap()
        .with_state(())
    }
}

fn json_body(json: serde_json::Value) -> axum::body::Body {
    axum::body::Body::from(serde_json::to_vec(&json).unwrap())
}

fn jsonlines_body(documents: Vec<serde_json::Value>) -> axum::body::Body {
    let mut buffer: Vec<u8> = vec![];

    for json in documents {
        buffer.extend(serde_json::to_vec(&json).unwrap());
        buffer.extend(b"\n");
    }

    axum::body::Body::from(buffer)
}

async fn assert_status(
    response: http::Response<Body>,
    expected: StatusCode,
) -> Result<http::Response<Body>, String> {
    let status = response.status();

    if status != expected {
        // let body = response.into_body();
        use http_body_util::BodyExt;

        let binary_body = response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        let string_body = std::str::from_utf8(&binary_body).unwrap();

        return Err(format!("expected {expected}, was {status}: {string_body}"));
    }

    Ok(response)
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
