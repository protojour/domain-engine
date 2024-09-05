use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::body::Body;
use bytes::Bytes;
use domain_engine_core::{DomainEngine, Session};
use domain_engine_httpjson::create_httpjson_router;
use domain_engine_test_utils::{
    dummy_session::DummySession, dynamic_data_store::DynamicDataStoreFactory,
    system::mock_current_time_monotonic, unimock,
};
use futures_util::{Stream, StreamExt};
use http::StatusCode;
use http_body_util::StreamBody;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::{OntolTest, SrcName};
use tracing::debug;

mod test_httpjson_error;
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

fn jsonlines_stream(
    documents: Vec<serde_json::Value>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    futures_util::stream::iter(documents)
        .enumerate()
        .then(|(idx, json)| async move {
            // simulate transmission
            tokio::time::sleep(Duration::from_millis(1)).await;
            debug!("next jsonlines document (#{idx})");

            let mut buffer = serde_json::to_vec(&json).unwrap();
            buffer.extend(b"\n");

            Ok::<_, Infallible>(Bytes::from(buffer))
        })
}

fn streaming_axum_body(
    stream: impl Stream<Item = Result<Bytes, Infallible>> + Send + 'static,
) -> axum::body::Body {
    axum::body::Body::new(StreamBody::new(
        stream.map(|result| result.map(http_body::Frame::data)),
    ))
}

async fn fetch_body_assert_status(
    response: http::Response<Body>,
    expected: StatusCode,
) -> Result<String, String> {
    let status = response.status();

    use http_body_util::BodyExt;
    let binary_body = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    let string_body = std::str::from_utf8(&binary_body).unwrap().to_string();

    if status != expected {
        return Err(format!("expected {expected}, was {status}: {string_body}"));
    }

    Ok(string_body)
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
