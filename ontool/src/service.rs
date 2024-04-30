#![forbid(unsafe_code)]

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::graphql::{domain_graphql_handler, graphiql_handler, GraphqlService};

use ::juniper::{EmptyMutation, EmptySubscription};
use axum::{routing::post, Extension};
use domain_engine_core::{DomainEngine, DomainError, DomainResult, Session};
use domain_engine_in_memory_store::InMemoryDataStoreFactory;
use domain_engine_juniper::ontology_schema::{Context, Query, Schema};
use domain_engine_juniper::{juniper, CreateSchemaError};
use juniper_axum::extract::JuniperRequest;
use ontol_runtime::{ontology::Ontology, PackageId};
use reqwest::header::HeaderName;
use tracing::info;

/// All the domains routed by domain name
pub async fn domains_router(ontology: Arc<Ontology>, base_url: &str) -> axum::Router {
    let engine = Arc::new(
        DomainEngine::builder(ontology.clone())
            .system(Box::<System>::default())
            .build(InMemoryDataStoreFactory, Session::default())
            .await
            .unwrap(),
    );

    let mut router: axum::Router = axum::Router::new();

    for (package_id, domain) in ontology
        .domains()
        .filter(|(package_id, _)| **package_id != PackageId(0))
    {
        let domain_path = format!(
            "/{unique_name}",
            unique_name = &ontology[domain.unique_name()]
        );
        router = router.nest(
            &domain_path,
            domain_router(engine.clone(), &domain_path, *package_id).unwrap(),
        );

        info!("Domain {package_id:?} served under {base_url}{domain_path}/graphql");
    }
    router.layer(tower_http::trace::TraceLayer::new_for_http())
    // let schema = ontology_schema::new(&ontology);
}
pub fn ontology_router(ontology: Arc<Ontology>, base_url: &str) -> axum::Router {
    let schema = Schema::new(
        Query,
        EmptyMutation::<Context>::new(),
        EmptySubscription::<Context>::new(),
    );

    pub async fn ontology_schema_graphql_handler(
        Extension(schema): Extension<Arc<Schema>>,
        Extension(context): Extension<Context>,
        JuniperRequest(req): JuniperRequest,
    ) -> (
        axum::http::StatusCode,
        axum::Json<juniper::http::GraphQLBatchResponse>,
    ) {
        let response = req.execute(&schema, &context).await;

        (
            if response.is_ok() {
                axum::http::StatusCode::OK
            } else {
                axum::http::StatusCode::BAD_REQUEST
            },
            axum::Json(response),
        )
    }

    info!("Ontology schema served under {base_url}/graphql");

    axum::Router::new()
        .route(
            "/graphql",
            post(ontology_schema_graphql_handler).get(graphiql_handler),
        )
        .layer(Extension(Arc::new(schema)))
        .layer(Extension(Context { ontology }))
        .layer(tower_http::trace::TraceLayer::new_for_http())
}

struct State {
    http_client: reqwest::Client,
}

impl Default for State {
    fn default() -> Self {
        Self {
            http_client: reqwest::Client::new(),
        }
    }
}
#[derive(Default)]
pub struct System {
    state: State,
}

#[async_trait::async_trait]
impl domain_engine_core::system::SystemAPI for System {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        domain_engine_core::system::current_time()
    }

    async fn call_http_json_hook(
        &self,
        url: &str,
        _session: Session,
        input: Vec<u8>,
    ) -> DomainResult<Vec<u8>> {
        let output_bytes = self
            .state
            .http_client
            .post(url)
            .header(
                HeaderName::from_lowercase(b"content-type").unwrap(),
                "application/json",
            )
            .header(
                HeaderName::from_lowercase(b"accept").unwrap(),
                "application/json",
            )
            .body(input)
            .send()
            .await
            .map_err(|err| {
                info!("json hook network error: {err}");

                DomainError::DeserializationFailed
            })?
            .error_for_status()
            .map_err(|err| {
                info!("json hook HTTP error: {err}");

                DomainError::DeserializationFailed
            })?
            .bytes()
            .await
            .map_err(|err| {
                info!("json hook response error: {err}");

                DomainError::DeserializationFailed
            })?;

        Ok(output_bytes.into())
    }
}

fn domain_router(
    engine: Arc<DomainEngine>,
    domain_path: &str,
    package_id: PackageId,
) -> anyhow::Result<axum::Router> {
    let mut router: axum::Router = axum::Router::new();

    match domain_engine_juniper::create_graphql_schema(engine.ontology_owned(), package_id) {
        Err(CreateSchemaError::GraphqlInterfaceNotFound) => {
            // Don't create the graphql endpoints
        }
        Ok(schema) => {
            router = router
                .route(
                    "/graphql",
                    post(domain_graphql_handler).get(graphiql_handler),
                )
                .layer(Extension(Arc::new(GraphqlService {
                    schema,
                    domain_engine: engine,
                    endpoint_url: format!("{domain_path}/graphql"),
                })));
        }
    };

    Ok(router)
}

pub struct Detach {
    pub router: Arc<Mutex<Option<axum::Router<()>>>>,
}

impl Clone for Detach {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
        }
    }
}

impl<B> tower_service::Service<axum::http::Request<B>> for Detach
where
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<axum::BoxError>,
{
    type Response = axum::response::Response;
    type Error = Infallible;
    type Future = <axum::Router<()> as tower_service::Service<axum::http::Request<B>>>::Future;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: axum::http::Request<B>) -> Self::Future {
        let mut router = {
            let router = self.router.lock().unwrap();
            match router.as_ref() {
                Some(router) => router.clone(),
                None => axum::Router::new(),
            }
        };

        router.call(req)
    }
}
