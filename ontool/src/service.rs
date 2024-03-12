#![forbid(unsafe_code)]

use std::{
    convert::Infallible,
    path::PathBuf,
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::graphql::{graphiql_handler, graphql_handler, GraphqlService};

use axum::Extension;
use clap::Parser;
use domain_engine_core::{DomainEngine, DomainError, DomainResult, Session};
use domain_engine_in_memory_store::InMemoryDataStoreFactory;
use domain_engine_juniper::CreateSchemaError;
use ontol_runtime::{ontology::Ontology, PackageId};

use tracing::info;

/// This environment variable is used to control logs.
// const LOG_ENV_VAR: &str = "LOG";

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The ontology file to load
    #[arg(short, long)]
    ontology: PathBuf,
}

// app -> router
pub async fn app(ontology: Ontology) -> axum::Router {
    let ontology = Arc::new(ontology);
    let engine = Arc::new(
        DomainEngine::builder(ontology.clone())
            .system(Box::new(System))
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

        info!("domain {package_id:?} served under {domain_path}");
    }

    router.layer(tower_http::trace::TraceLayer::new_for_http())
}

struct System;

#[async_trait::async_trait]
impl domain_engine_core::system::SystemAPI for System {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        domain_engine_core::system::current_time()
    }

    async fn call_http_json_hook(&self, _: &str, _: Session, _: Vec<u8>) -> DomainResult<Vec<u8>> {
        Err(DomainError::NotImplemented)
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
                    axum::routing::post(graphql_handler).get(graphiql_handler),
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

pub struct Detach<B> {
    pub router: Arc<Mutex<Option<axum::Router<(), B>>>>,
}

impl<B> Clone for Detach<B> {
    fn clone(&self) -> Self {
        Self {
            router: self.router.clone(),
        }
    }
}

impl<B> tower_service::Service<axum::http::Request<B>> for Detach<B>
where
    B: axum::body::HttpBody + Send + 'static,
{
    type Response = axum::response::Response;
    type Error = Infallible;
    type Future = <axum::Router<(), B> as tower_service::Service<axum::http::Request<B>>>::Future;

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
