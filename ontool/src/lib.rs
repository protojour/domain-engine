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
use domain_engine_core::{DomainEngine, Session};
use domain_engine_store_inmemory::InMemoryDataStoreFactory;
use notify_debouncer_full::{
    new_debouncer,
    notify::{RecursiveMode, Watcher},
};
use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    ontol_syntax::OntolTreeSyntax,
    topology::{DepGraphBuilder, DomainUrl, DomainUrlParser, GraphState, ParsedDomain},
    SourceCodeRegistry, SourceId, Sources,
};
use ontol_examples::FakeAtlasServer;
use ontol_lsp::Backend;
use ontol_parser::cst_parse;
use ontol_runtime::{
    interface::json_schema::build_openapi_schemas,
    ontology::{
        config::{DataStoreConfig, DomainConfig},
        Ontology,
    },
};
use service::{domains_router, ontology_router};
use std::{
    collections::HashMap,
    fmt::Display,
    fs::{self, read_dir, File},
    io::{stdout, Stdout, Write},
    net::SocketAddr,
    path::PathBuf,
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
    command: Option<Command>,
}

#[derive(Subcommand)]
pub enum Command {
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
pub struct Check {
    /// ONTOL domain URLs to compile. Base URL is file:///, i.e. default directory.
    pub domains: Vec<String>,

    /// Search directory for ONTOL files
    #[arg(short('w'), long, default_value = ".")]
    pub dir: PathBuf,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
pub struct Compile {
    /// ONTOL domain URLs to compile. Base URL is file:///, i.e. default directory.
    pub domains: Vec<String>,

    /// Search directory for ONTOL files
    #[arg(short('w'), long, default_value = ".")]
    pub dir: PathBuf,

    /// Specify a data store backend [inmemory, arangodb, ..]
    #[arg(short('b'), long)]
    pub backend: Option<String>,

    /// Ontology output file
    #[arg(short, long, default_value = "ontology")]
    pub output: PathBuf,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
pub struct Generate {
    /// ONTOL domain URIs to generate schemas from. Base URL is file:///, i.e. default directory.
    pub domains: Vec<String>,

    /// Search directory for ONTOL files
    #[arg(short('w'), long, default_value = ".")]
    pub dir: PathBuf,

    /// Output format for JSON schema
    #[arg(short('f'), long, default_value = "json")]
    pub format: Format,
}

#[derive(Clone, ValueEnum)]
pub enum Format {
    Json,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
pub struct Serve {
    /// ONTOL domain URIs to serve.
    pub domains: Vec<String>,

    /// Search directory for ONTOL files
    #[arg(short('w'), long, default_value = ".")]
    pub dir: PathBuf,

    /// Specify a port for the server
    #[arg(short('p'), long, default_value = "5000")]
    pub port: u16,
}

#[derive(Debug, Error)]
pub enum OntoolError {
    #[error("No input files")]
    NoInputFiles,
    #[error("Invalid domain reference")]
    InvalidDomainReference,
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

    if let Some(Command::Lsp) = &cli.command {
        init_tracing_stderr();
    } else {
        init_tracing();
    }

    if let Some(command) = cli.command {
        run_command(command).await
    } else {
        Ok(())
    }
}

pub async fn run_command(command: Command) -> Result<(), OntoolError> {
    match command {
        Command::Check(args) => {
            compile(args.dir, args.domains, None)?;
            println!("No errors found.");

            Ok(())
        }
        Command::Compile(args) => {
            let output_file = File::create(args.output)?;

            let ontology = compile(args.dir, args.domains, args.backend)?;
            ontology.try_serialize_to_bincode(output_file).unwrap();

            Ok(())
        }
        Command::Generate(args) => {
            let first_domain = get_source_url(
                args.domains
                    .as_slice()
                    .iter()
                    .next()
                    .expect("no input files"),
            )?;

            let ontology = compile(args.dir, args.domains, None)?;

            let (domain_index, domain) = ontology
                .domains()
                .find(|domain| {
                    let name = &ontology[domain.1.unique_name()];
                    name == first_domain.short_name()
                })
                .expect("domain not found");

            let schemas = build_openapi_schemas(&ontology, domain_index, domain);

            match args.format {
                Format::Json => {
                    let schemas_json = serde_json::to_string_pretty(&schemas)?;
                    println!("{}", schemas_json);
                }
            }

            Ok(())
        }
        Command::Serve(args) => {
            serve(args.dir, args.domains, args.port).await?;

            Ok(())
        }
        Command::GenDomainId => {
            let ulid = Ulid::new();
            println!("{ulid}");

            Ok(())
        }
        Command::Lsp => lsp().await,
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
    domains: Vec<String>,
    backend: Option<String>,
) -> Result<Ontology, OntoolError> {
    if domains.is_empty() {
        return Err(OntoolError::NoInputFiles);
    }

    let mut ontol_sources = Sources::default();
    let mut sources_by_url: HashMap<DomainUrl, Rc<String>> = Default::default();
    let mut paths_by_url: HashMap<DomainUrl, PathBuf> = Default::default();

    let fake_atlas_server = FakeAtlasServer::default();

    for entry in read_dir(root_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            continue;
        }
        // TODO: only insert needed ontol files
        if matches!(path.extension(), Some(ext) if ext == "on") {
            let source = fs::read_to_string(&path)?;
            let source_url = get_source_url(path.to_str().unwrap())?;

            sources_by_url.insert(source_url.clone(), Rc::new(source));
            paths_by_url.insert(source_url, path);
        }
    }

    let mut roots = vec![];
    for domain_ref in domains {
        roots.push(get_source_url(&domain_ref)?);
    }

    let mut source_code_registry = SourceCodeRegistry::default();
    let mut package_graph_builder = DepGraphBuilder::with_roots(roots);

    let topology = loop {
        let graph_state = package_graph_builder.transition().map_err(|err| {
            print_unified_compile_error(err, &ontol_sources, &source_code_registry).unwrap();
            OntoolError::Compile
        })?;

        match graph_state {
            GraphState::RequestPackages { builder, requests } => {
                package_graph_builder = builder;

                for request in requests {
                    if let Some(source_text) =
                        sources_by_url.remove(&request.url).or(fake_atlas_server
                            .lookup(&request.url)
                            .map(|src| Rc::new(src.to_string())))
                    {
                        let (flat_tree, errors) = cst_parse(&source_text);

                        let parsed = ParsedDomain::new(
                            request,
                            Box::new(OntolTreeSyntax {
                                tree: flat_tree.unflatten(),
                                source_text: source_text.clone(),
                            }),
                            errors,
                            DomainConfig::default(),
                            &mut ontol_sources,
                        );
                        source_code_registry
                            .registry
                            .insert(parsed.src.id, source_text);
                        package_graph_builder.provide_domain(parsed);
                    } else {
                        eprintln!("Could not load `{}`", request.url);
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
fn get_source_url(uri: &str) -> Result<DomainUrl, OntoolError> {
    let parser = DomainUrlParser::default();
    let Ok(domain_url) = parser.parse(uri) else {
        return Err(OntoolError::InvalidDomainReference);
    };
    match domain_url.url().scheme() {
        "file" => {
            let mut path_segments = domain_url.url().path_segments().expect("invalid URI");
            if path_segments.clone().count() != 1 {
                return Err(OntoolError::InvalidDomainReference);
            }

            let file_name = path_segments.next_back().unwrap();

            match file_name.strip_suffix(".on") {
                Some(stripped) => Ok(DomainUrl::parse(stripped)),
                None => Ok(DomainUrl::parse(file_name)),
            }
        }
        _ => Ok(domain_url),
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

        let (origin, literal_source) =
            report_source_name(err_span.source_id, ontol_sources, source_code_registry);

        Report::build(ReportKind::Error, &origin, span.start)
            .with_label(
                Label::new((&origin, span))
                    .with_message(message)
                    .with_color(colors.next()),
            )
            .finish()
            .eprint((&origin, Source::from(literal_source.as_ref())))?;

        for note in error.notes {
            let note_span = note.span();
            let span = note.span().span.start as usize..note.span().span.end as usize;
            let message = note.into_note().to_string();

            let (origin, literal_source) =
                report_source_name(note_span.source_id, ontol_sources, source_code_registry);

            Report::build(ReportKind::Advice, &origin, span.start)
                .with_label(
                    Label::new((&origin, span))
                        .with_message(message)
                        .with_color(colors.next()),
                )
                .finish()
                .eprint((&origin, Source::from(literal_source.as_ref())))?;
        }
    }

    Ok(())
}

#[derive(PartialEq, Eq, Hash, Debug)]
enum DomainUrlOrigin {
    Domain(Rc<DomainUrl>),
    CliArg,
}

impl Display for DomainUrlOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Domain(url) => write!(f, "{url}"),
            Self::CliArg => write!(f, "<arg>"),
        }
    }
}

fn report_source_name(
    source_id: SourceId,
    ontol_sources: &Sources,
    registry: &SourceCodeRegistry,
) -> (DomainUrlOrigin, Rc<String>) {
    // FIXME: If the error can't be mapped to a source file,
    // things will look quite strange. Fix later..
    match ontol_sources.get_source(source_id) {
        Some(ontol_source) => {
            let literal_source = registry.registry.get(&source_id);

            (
                DomainUrlOrigin::Domain(ontol_source.url.clone()),
                literal_source
                    .cloned()
                    .unwrap_or_else(|| Rc::new("<ontol>".to_string())),
            )
        }
        None => {
            let ontol = Rc::new("<arg>".to_string());
            (DomainUrlOrigin::CliArg, ontol)
        }
    }
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

async fn serve(root_dir: PathBuf, domains: Vec<String>, port: u16) -> Result<(), OntoolError> {
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
    let ontology_result = compile(root_dir.clone(), domains.clone(), backend.clone());
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
                            compile(root_dir.clone(), domains.clone(), backend.clone());
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
    let domain_engine = Arc::new(
        DomainEngine::builder(ontology.clone())
            .system(Box::<System>::default())
            .build(InMemoryDataStoreFactory, Session::default())
            .await
            .unwrap(),
    );

    let o = ontology_router(domain_engine.clone(), &format!("{base_url}/o"));
    let d = domains_router(domain_engine, &format!("{base_url}/d")).await;

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
