#![forbid(unsafe_code)]

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::graphql::{domain_graphql_handler, graphiql_handler, GraphqlService};

use axum::{extract::FromRequestParts, routing::post, Extension};
use domain_engine_core::{
    domain_error::DomainErrorKind, CrdtActor, DomainEngine, DomainResult, Session,
};
use domain_engine_graphql::{
    domain::CreateSchemaError,
    gql_scalar::GqlScalar,
    juniper,
    ontology::{OntologyCtx, OntologySchema},
};
use juniper_axum::extract::JuniperRequest;
use ontol_runtime::DomainIndex;
use reqwest::header::HeaderName;
use tracing::info;

/// All the domains routed by domain name
pub async fn domains_router(domain_engine: Arc<DomainEngine>, base_url: &str) -> axum::Router {
    let mut router: axum::Router = axum::Router::new();
    let ontology = domain_engine.ontology();

    for (domain_index, domain) in ontology
        .domains()
        .filter(|(domain_index, _)| *domain_index != DomainIndex::ontol())
    {
        let domain_path = format!(
            "/{unique_name}",
            unique_name = &ontology[domain.unique_name()]
        );
        router = router.nest(
            &domain_path,
            domain_router(domain_engine.clone(), &domain_path, domain_index).unwrap(),
        );

        info!("Domain {domain_index:?} served under {base_url}{domain_path}/graphql");
    }
    router.layer(tower_http::trace::TraceLayer::new_for_http())
    // let schema = ontology_schema::new(&ontology);
}

pub fn ontology_router(domain_engine: Arc<DomainEngine>, base_url: &str) -> axum::Router {
    let ontology_schema = OntologySchema::new_with_scalar_value(
        Default::default(),
        Default::default(),
        Default::default(),
    );

    pub async fn ontology_schema_graphql_handler(
        Extension(schema): Extension<Arc<OntologySchema>>,
        Extension(domain_engine): Extension<Arc<DomainEngine>>,
        JuniperRequest(req): JuniperRequest<GqlScalar>,
    ) -> (
        axum::http::StatusCode,
        axum::Json<juniper::http::GraphQLBatchResponse<GqlScalar>>,
    ) {
        let response = req
            .execute(
                schema.as_ref(),
                &OntologyCtx::new(domain_engine, Session::default()),
            )
            .await;

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
        .layer(Extension(Arc::new(ontology_schema)))
        .layer(Extension(domain_engine))
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

    #[expect(unused)]
    fn get_user_id(&self, session: Session) -> DomainResult<String> {
        Ok("ontooluser".to_string())
    }

    #[expect(unused)]
    fn verify_session_user_id(&self, user_id: &str, session: Session) -> DomainResult<()> {
        Ok(())
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

                DomainErrorKind::DeserializationFailed.into_error()
            })?
            .error_for_status()
            .map_err(|err| {
                info!("json hook HTTP error: {err}");

                DomainErrorKind::DeserializationFailed.into_error()
            })?
            .bytes()
            .await
            .map_err(|err| {
                info!("json hook response error: {err}");

                DomainErrorKind::DeserializationFailed.into_error()
            })?;

        Ok(output_bytes.into())
    }

    fn crdt_system_actor(&self) -> CrdtActor {
        CrdtActor {
            user_id: "ontool".to_string(),
            actor_id: Default::default(),
        }
    }
}

fn domain_router(
    engine: Arc<DomainEngine>,
    _domain_path: &str,
    domain_index: DomainIndex,
) -> anyhow::Result<axum::Router> {
    let mut router: axum::Router = axum::Router::new();

    match domain_engine_graphql::domain::create_graphql_schema(
        engine.ontology_owned(),
        domain_index,
    ) {
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
                    domain_engine: engine.clone(),
                })));
        }
    };

    if let Some(httpjson_router) =
        domain_engine_httpjson::create_httpjson_router::<(), DummyAuth>(engine, domain_index)
    {
        router = router.nest("/api", httpjson_router);
    }

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

struct DummyAuth;

#[async_trait::async_trait]
impl FromRequestParts<()> for DummyAuth {
    type Rejection = Infallible;

    async fn from_request_parts(
        _req: &mut axum::http::request::Parts,
        _state: &(),
    ) -> Result<Self, Self::Rejection> {
        Ok(Self)
    }
}

impl From<DummyAuth> for Session {
    fn from(_value: DummyAuth) -> Self {
        Session::default()
    }
}
