use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::Context;
use axum::Extension;
use clap::Parser;
use domain_engine_core::DomainEngine;
use domain_engine_juniper::create_graphql_schema;
use graphql::{graphiql_handler, GraphqlService};
use ontol_runtime::{ontology::Ontology, PackageId};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::graphql::graphql_handler;

mod graphql;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The ontology file to load
    #[arg(short, long)]
    ontology: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::filter::FilterFn::new(|metadata| {
            let target = metadata.target();
            target.starts_with("domain_engine")
                || target.starts_with("ontol")
                || target.starts_with("tower_http")
        }))
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Args = Args::parse();
    let ontology = Arc::new(load_ontology(args.ontology)?);
    let engine = Arc::new(
        DomainEngine::builder(ontology.clone())
            .system(Box::new(System))
            .build(),
    );

    let mut router: axum::Router = axum::Router::new();

    for (index, (package_id, _domain)) in ontology
        .domains()
        .filter(|(package_id, _)| **package_id != PackageId(0))
        .enumerate()
    {
        let domain_path = format!("/{index}");
        router = router.nest(
            &domain_path,
            domain_router(engine.clone(), &domain_path, *package_id)?,
        );
    }

    router = router.layer(tower_http::trace::TraceLayer::new_for_http());

    axum::Server::bind(&"0.0.0.0:8080".parse()?)
        .serve(router.into_make_service())
        .await
        .context("error running HTTP server")?;

    Ok(())
}

fn load_ontology(path: PathBuf) -> anyhow::Result<Ontology> {
    let ontology_file = File::open(path).context("Ontology file not found")?;
    bincode::deserialize_from(&ontology_file).context("Problem reading ontology")
}

struct System;

impl domain_engine_core::system::SystemAPI for System {
    fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

fn domain_router(
    engine: Arc<DomainEngine>,
    domain_path: &str,
    package_id: PackageId,
) -> anyhow::Result<axum::Router> {
    let graphql_service = GraphqlService {
        schema: create_graphql_schema(package_id, engine.ontology_owned())
            .context("Problem creating graphql schema")?,
        gql_context: engine.into(),
        endpoint_url: format!("{domain_path}/graphql"),
    };

    Ok(axum::Router::new()
        .route(
            "/graphql",
            axum::routing::post(graphql_handler).get(graphiql_handler),
        )
        .layer(Extension(Arc::new(graphql_service))))
}
