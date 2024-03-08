#![forbid(unsafe_code)]

use std::{fs::File, path::PathBuf, sync::Arc};

use crate::graphql::{graphiql_handler, graphql_handler, GraphqlService};
use anyhow::Context;
use axum::Extension;
use clap::Parser;
use domain_engine_core::{DomainEngine, DomainError, DomainResult, Session};
use domain_engine_in_memory_store::InMemoryDataStoreFactory;
use domain_engine_juniper::CreateSchemaError;
use ontol_runtime::{ontology::Ontology, PackageId};
use tracing::info;
use tracing_subscriber::{filter::LevelFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// This environment variable is used to control logs.
const LOG_ENV_VAR: &str = "LOG";

const SERVER_SOCKET_ADDR: &str = "0.0.0.0:8080";

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The ontology file to load
    #[arg(short, long)]
    ontology: PathBuf,
}

// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     tracing_subscriber::registry()
//         .with(tracing_subscriber::fmt::layer())
//         .with(tracing_subscriber::filter::FilterFn::new(|metadata| {
//             let target = metadata.target();
//             target.starts_with("domain_")
//                 || target.starts_with("ontol")
//                 || target.starts_with("tower_http")
//         }))
//         .with(
//             tracing_subscriber::EnvFilter::builder()
//                 .with_default_directive(LevelFilter::INFO.into())
//                 .with_env_var(LOG_ENV_VAR)
//                 .from_env_lossy(),
//         )
//         .init();

//     let args: Args = Args::parse();
//     let ontology = Arc::new(load_ontology(args.ontology)?);
//     let engine = Arc::new(
//         DomainEngine::builder(ontology.clone())
//             .system(Box::new(System))
//             .build(InMemoryDataStoreFactory, Session::default())
//             .await
//             .unwrap(),
//     );

//     let mut router: axum::Router = axum::Router::new();

//     for (package_id, domain) in ontology
//         .domains()
//         .filter(|(package_id, _)| **package_id != PackageId(0))
//     {
//         let domain_path = format!(
//             "/{unique_name}",
//             unique_name = &ontology[domain.unique_name()]
//         );
//         router = router.nest(
//             &domain_path,
//             domain_router(engine.clone(), &domain_path, *package_id)?,
//         );

//         info!("domain {package_id:?} served under {domain_path}");
//     }

//     router = router.layer(tower_http::trace::TraceLayer::new_for_http());

//     info!("binding server to {SERVER_SOCKET_ADDR}");

//     axum::Server::bind(&SERVER_SOCKET_ADDR.parse()?)
//         .serve(router.into_make_service())
//         .await
//         .context("error running HTTP server")?;

//     Ok(())
// }

// pub async fn serve(ontology: Ontology, cancellation_token: CancellationToken) {
//     todo!();
// }

fn load_ontology(path: PathBuf) -> anyhow::Result<Ontology> {
    let ontology_file = File::open(path).context("Ontology file not found")?;
    Ontology::try_from_bincode(ontology_file).context("Problem reading ontology")
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
