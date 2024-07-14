#![forbid(unsafe_code)]

use ariadne::{ColorGenerator, Label, Report, ReportKind, Source};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        WebSocketUpgrade,
    },
    response::Response,
    routing::get,
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use crossterm::{
    cursor, queue,
    terminal::{Clear, ClearType},
};
use notify_debouncer_full::{
    new_debouncer,
    notify::{RecursiveMode, Watcher},
};
use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    ontol_syntax::OntolTreeSyntax,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    SourceCodeRegistry, Sources,
};
use ontol_lsp::Backend;
use ontol_parser::cst_parse;
use ontol_runtime::{
    interface::json_schema::build_openapi_schemas,
    ontology::{
        config::{DataStoreConfig, PackageConfig},
        Ontology,
    },
};
use service::{domains_router, ontology_router};
use std::{
    collections::HashMap,
    fs::{self, read_dir, File},
    io::{stdout, Stdout, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::broadcast::{self, Sender};
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tower_lsp::{LspService, Server};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use ulid::Ulid;

use crate::service::Detach;

mod graphql;
mod service;

pub use service::System;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// This environment variable is used to control logs.
const LOG_ENV_VAR: &str = "LOG";

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// ontool – ONTOlogy Language tool
#[derive(Parser)]
#[command(version, about, arg_required_else_help(true))]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Check ONTOL (.on) files for errors
    Check(Check),
    /// Compile ONTOL (.on) files to an ontology
    Compile(Compile),
    /// Generate JSON schema from ONTOL (.on) files
    Generate(Generate),
    /// Run ontool in development server mode
    Serve(Serve),
    /// Auto-generate a new domain id
    GenDomainId,
    /// Run ontool in language server mode
    Lsp,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Check {
    /// ONTOL (.on) files to check for errors
    files: Vec<PathBuf>,

    /// Search directory for ONTOL files
    #[arg(short('w'), long, default_value = ".")]
    dir: PathBuf,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Compile {
    /// ONTOL (.on) files to compile
    files: Vec<PathBuf>,

    /// Search directory for ONTOL files
    #[arg(short('w'), long, default_value = ".")]
    dir: PathBuf,

    /// Specify a data store backend [inmemory, arangodb, ..]
    #[arg(short('b'), long)]
    backend: Option<String>,

    /// Ontology output file
    #[arg(short, long, default_value = "ontology")]
    output: PathBuf,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Generate {
    /// ONTOL (.on) files to generate schemas from
    files: Vec<PathBuf>,

    /// Search directory for ONTOL files
    #[arg(short('w'), long, default_value = ".")]
    dir: PathBuf,

    /// Output format for JSON schema
    #[arg(short('f'), long, default_value = "json")]
    format: Format,
}

#[derive(Clone, ValueEnum)]
enum Format {
    Json,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Serve {
    /// Root file(s) of the ontology
    files: Vec<PathBuf>,

    /// Search directory for ONTOL files
    #[arg(short('w'), long, default_value = ".")]
    dir: PathBuf,

    /// Specify a port for the server
    #[arg(short('p'), long, default_value = "5000")]
    port: u16,
}

#[derive(Debug, Error)]
pub enum OntoolError {
    #[error("No input files")]
    NoInputFiles,
    #[error("Parse error")]
    Parse,
    #[error("Compile error")]
    Compile,
    #[error("{0:?}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("{0}")]
    IO(#[from] std::io::Error),
}

pub async fn run() -> Result<(), OntoolError> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Check(args)) => {
            init_tracing();

            compile(args.dir, args.files, None)?;
            println!("No errors found.");

            Ok(())
        }
        Some(Commands::Compile(args)) => {
            init_tracing();

            let output_file = File::create(args.output)?;

            let ontology = compile(args.dir, args.files, args.backend)?;
            ontology.try_serialize_to_bincode(output_file).unwrap();

            Ok(())
        }
        Some(Commands::Generate(args)) => {
            init_tracing();

            let first_domain =
                get_source_name(args.files.as_slice().iter().next().expect("no input files"));

            let ontology = compile(args.dir, args.files, None)?;

            let (package_id, domain) = ontology
                .domains()
                .find(|domain| {
                    let name = &ontology[domain.1.unique_name()];
                    name == first_domain
                })
                .expect("domain not found");

            let schemas = build_openapi_schemas(&ontology, *package_id, domain);

            match args.format {
                Format::Json => {
                    let schemas_json = serde_json::to_string_pretty(&schemas)?;
                    println!("{}", schemas_json);
                }
            }

            Ok(())
        }
        Some(Commands::Serve(args)) => {
            init_tracing();

            serve(args.dir, args.files, args.port).await?;

            Ok(())
        }
        Some(Commands::GenDomainId) => {
            let ulid = Ulid::new();
            println!("{ulid}");

            Ok(())
        }
        Some(Commands::Lsp) => {
            init_tracing_stderr();

            lsp().await
        }
        None => Ok(()),
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .with_env_var(LOG_ENV_VAR)
                .from_env_lossy(),
        )
        .init();
}

fn init_tracing_stderr() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
}

fn compile(
    root_dir: PathBuf,
    root_files: Vec<PathBuf>,
    backend: Option<String>,
) -> Result<Ontology, OntoolError> {
    if root_files.is_empty() {
        return Err(OntoolError::NoInputFiles);
    }

    let mut ontol_sources = Sources::default();
    let mut sources_by_name: HashMap<String, Rc<String>> = Default::default();
    let mut paths_by_name: HashMap<String, PathBuf> = Default::default();

    for entry in read_dir(root_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        // TODO: only insert needed ontol files
        if matches!(path.extension(), Some(ext) if ext == "on") {
            let source = fs::read_to_string(&path)?;
            let source_name = get_source_name(&path);

            sources_by_name.insert(source_name.clone(), Rc::new(source));
            paths_by_name.insert(source_name, path);
        }
    }

    let mut source_code_registry = SourceCodeRegistry::default();
    let mut package_graph_builder =
        PackageGraphBuilder::with_roots(root_files.iter().map(|path| get_source_name(path)));

    let topology = loop {
        let graph_state = package_graph_builder.transition().map_err(|err| {
            print_unified_compile_error(err, &ontol_sources, &source_code_registry).unwrap();
            OntoolError::Compile
        })?;

        match graph_state {
            GraphState::RequestPackages { builder, requests } => {
                package_graph_builder = builder;

                for request in requests {
                    let source_name = match &request.reference {
                        PackageReference::Named(source_name) => source_name.as_str(),
                    };

                    if let Some(source_text) = sources_by_name.remove(source_name) {
                        let (flat_tree, errors) = cst_parse(&source_text);

                        let parsed = ParsedPackage::new(
                            request,
                            Box::new(OntolTreeSyntax {
                                tree: flat_tree.unflatten(),
                                source_text: source_text.clone(),
                            }),
                            errors,
                            PackageConfig::default(),
                            &mut ontol_sources,
                        );
                        source_code_registry
                            .registry
                            .insert(parsed.src.id, source_text);
                        package_graph_builder.provide_package(parsed);
                    } else {
                        eprintln!("Could not load `{source_name}`");
                    }
                }
            }
            GraphState::Built(topology) => break topology,
        }
    };

    let mem = Mem::default();
    ontol_compiler::compile(topology, ontol_sources.clone(), &mem)
        .map(|mut compiled| {
            if let Some(backend) = backend {
                compiled.override_data_store(DataStoreConfig::ByName(backend));
            }

            compiled.into_ontology()
        })
        .or_else(|err| {
            print_unified_compile_error(err, &ontol_sources, &source_code_registry)?;
            Err(OntoolError::Compile)
        })
}

/// Get a source name from a path.
/// Strips away the path and removes the `.on` suffix (if present)
fn get_source_name(path: &Path) -> String {
    let file_name = path.file_name().unwrap().to_str().unwrap().to_string();

    match file_name.strip_suffix(".on") {
        Some(stripped) => stripped.to_string(),
        None => file_name,
    }
}

fn print_unified_compile_error(
    unified_error: UnifiedCompileError,
    ontol_sources: &Sources,
    source_code_registry: &SourceCodeRegistry,
) -> Result<(), OntoolError> {
    let mut colors = ColorGenerator::new();
    for error in unified_error.errors.into_iter() {
        let err_span = error.span();
        let span = err_span.span.start as usize..err_span.span.end as usize;
        let message = error.error.to_string();

        let ontol_source = ontol_sources.get_source(err_span.source_id).unwrap();
        let literal_source = source_code_registry
            .registry
            .get(&err_span.source_id)
            .unwrap();

        Report::build(ReportKind::Error, ontol_source.name(), span.start)
            .with_label(
                Label::new((ontol_source.name(), span))
                    .with_message(message)
                    .with_color(colors.next()),
            )
            .finish()
            .eprint((ontol_source.name(), Source::from(literal_source.as_ref())))?;

        for note in error.notes {
            let span = note.span().span.start as usize..note.span().span.end as usize;
            let message = note.into_note().to_string();

            let ontol_source = ontol_sources.get_source(err_span.source_id).unwrap();
            let literal_source = source_code_registry
                .registry
                .get(&err_span.source_id)
                .unwrap();

            Report::build(ReportKind::Advice, ontol_source.name(), span.start)
                .with_label(
                    Label::new((ontol_source.name(), span))
                        .with_message(message)
                        .with_color(colors.next()),
                )
                .finish()
                .eprint((ontol_source.name(), Source::from(literal_source.as_ref())))?;
        }
    }

    Ok(())
}

#[derive(Clone)]
enum ChannelMessage {
    Reload,
}

fn clear_term(stdout: &mut Stdout) {
    queue!(stdout, Clear(ClearType::All), cursor::MoveTo(0, 0))
        .expect("Terminal should be able to be cleared");
    stdout.flush().unwrap();
}

async fn serve(root_dir: PathBuf, root_files: Vec<PathBuf>, port: u16) -> Result<(), OntoolError> {
    let mut stdout = stdout();
    let (tx, rx) = std::sync::mpsc::channel();
    let (reload_tx, _reload_rx) = broadcast::channel::<ChannelMessage>(16);
    let mut debouncer = new_debouncer(Duration::from_secs(1), None, tx).unwrap();
    clear_term(&mut stdout);

    debouncer
        .watcher()
        .watch(&root_dir, RecursiveMode::NonRecursive)
        .unwrap();

    let bind_addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    let base_url = format!("http://localhost:{port}");

    let dynamic_routers = DynamicRouters::default();

    tokio::spawn({
        let reload_tx = reload_tx.clone();
        let outer_router = axum::Router::new()
            .nest_service(
                "/d",
                Detach {
                    router: dynamic_routers.domains.clone(),
                },
            )
            .nest_service(
                "/o",
                Detach {
                    router: dynamic_routers.ontology.clone(),
                },
            )
            .route("/ws", get(|socket| ws_upgrade_handler(socket, reload_tx)))
            .layer(CorsLayer::permissive());

        info!("Serving on {base_url}");

        async move {
            let listener = tokio::net::TcpListener::bind(&bind_addr).await.unwrap();
            axum::serve(listener, outer_router).await.unwrap();
        }
    });

    // initial compilation
    let backend = Some(String::from("inmemory"));
    let ontology_result = compile(root_dir.clone(), root_files.clone(), backend.clone());
    match ontology_result {
        Ok(ontology) => {
            reload_routes(ontology, &dynamic_routers, &base_url).await;
            let _ = reload_tx.send(ChannelMessage::Reload);
        }
        Err(error) => println!("{:?}", error),
    }

    let mut cancellation_token = CancellationToken::new();

    for res in rx {
        match res {
            Ok(debounced_event_vec) => {
                for debounced_event in debounced_event_vec {
                    if debounced_event.event.kind.is_modify()
                        || debounced_event.event.kind.is_create()
                    {
                        cancellation_token.cancel();
                        cancellation_token = CancellationToken::new();
                        clear_term(&mut stdout);

                        let ontology_result =
                            compile(root_dir.clone(), root_files.clone(), backend.clone());
                        match ontology_result {
                            Ok(ontology) => {
                                reload_routes(ontology, &dynamic_routers, &base_url).await;
                                let _ = reload_tx.send(ChannelMessage::Reload);
                            }
                            Err(error) => {
                                dynamic_routers.domains.lock().unwrap().take();
                                dynamic_routers.ontology.lock().unwrap().take();
                                println!("{:?}", error);
                            }
                        }
                    }
                }
            }
            Err(error) => println!("{:?}", error),
        }
    }

    Ok(())
}

#[derive(Default)]
struct DynamicRouters {
    ontology: Arc<Mutex<Option<axum::Router>>>,
    domains: Arc<Mutex<Option<axum::Router>>>,
}

async fn reload_routes(ontology: Ontology, dyn_routers: &DynamicRouters, base_url: &str) {
    let ontology = Arc::new(ontology);
    let o = ontology_router(ontology.clone(), &format!("{base_url}/o"));
    let d = domains_router(ontology, &format!("{base_url}/d")).await;

    *(dyn_routers.ontology.lock().unwrap()) = Some(o);
    *(dyn_routers.domains.lock().unwrap()) = Some(d);
}

async fn ws_upgrade_handler(ws: WebSocketUpgrade, tx: Sender<ChannelMessage>) -> Response {
    ws.on_upgrade(|socket| ws_handler(socket, tx))
}

async fn ws_handler(mut socket: WebSocket, tx: Sender<ChannelMessage>) {
    let mut rx = tx.subscribe();
    while let Ok(msg) = rx.recv().await {
        match msg {
            ChannelMessage::Reload => {
                if socket.send(Message::Text("reload".into())).await.is_err() {
                    return;
                }
            }
        }
    }
}

async fn lsp() -> Result<(), OntoolError> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(Backend::new);
    Server::new(stdin, stdout, socket).serve(service).await;

    Ok(())
}
