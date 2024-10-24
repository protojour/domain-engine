use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::body::Body;
use bytes::Bytes;
use domain_engine_core::{system::SystemAPI, DomainEngine, Session};
use domain_engine_httpjson::DomainRouterBuilder;
use domain_engine_test_utils::{
    dummy_session::DummySession, dynamic_data_store::DynamicDataStoreFactory,
};
use futures_util::{Stream, StreamExt};
use http::StatusCode;
use http_body_util::StreamBody;
use ontol_runtime::ontology::Ontology;
use ontol_test_utils::OntolTest;
use tracing::debug;

mod test_httpjson_error;
mod test_httpjson_stix;
mod test_httpjson_workspace;

trait MakeTestRouter {
    fn make_test_router(&self, domain_engine: Arc<DomainEngine>, short_name: &str) -> axum::Router;
}

impl MakeTestRouter for OntolTest {
    fn make_test_router(&self, domain_engine: Arc<DomainEngine>, short_name: &str) -> axum::Router {
        DomainRouterBuilder::default()
            .create_httpjson_router::<(), DummySession>(
                domain_engine.clone(),
                self.get_domain_index(short_name),
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

trait FromBytes {
    fn from_bytes(bytes: Bytes) -> Self;
}

impl FromBytes for serde_json::Value {
    fn from_bytes(bytes: Bytes) -> Self {
        serde_json::from_slice(&bytes).unwrap()
    }
}

impl FromBytes for String {
    fn from_bytes(bytes: Bytes) -> Self {
        std::str::from_utf8(&bytes).unwrap().to_string()
    }
}

async fn fetch_body_assert_status<B: FromBytes>(
    response: http::Response<Body>,
    expected: StatusCode,
) -> Result<B, String> {
    let status = response.status();

    use http_body_util::BodyExt;
    let binary_body = response.into_body().collect().await.unwrap().to_bytes();

    if status != expected {
        let string_body = std::str::from_utf8(&binary_body).unwrap().to_string();
        Err(format!("expected {expected}, was {status}: {string_body}"))
    } else {
        Ok(B::from_bytes(binary_body))
    }
}

async fn make_domain_engine(
    ontology: Arc<Ontology>,
    datastore: &str,
    system: Box<dyn SystemAPI + Send + Sync>,
) -> Arc<DomainEngine> {
    Arc::new(
        DomainEngine::builder(ontology)
            .system(system)
            .build(DynamicDataStoreFactory::new(datastore), Session::default())
            .await
            .unwrap(),
    )
}
