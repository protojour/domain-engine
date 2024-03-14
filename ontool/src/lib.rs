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
use clap::{Args, Parser, Subcommand};
use notify_debouncer_full::{
    new_debouncer,
    notify::{RecursiveMode, Watcher},
};
use ontol_compiler::{
    error::UnifiedCompileError,
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    Compiler, SourceCodeRegistry, Sources,
};
use ontol_lsp::Backend;
use ontol_parser::parse_statements;
use ontol_runtime::{
    config::{DataStoreConfig, PackageConfig},
    interface::json_schema::build_openapi_schemas,
    ontology::Ontology,
    PackageId,
};
use service::app;
use std::{
    collections::HashMap,
    fs::{self, read_dir, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::broadcast::{self, Sender};
use tokio_util::sync::CancellationToken;
use tower_lsp::{LspService, Server};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::service::Detach;

mod graphql;
mod service;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// This environment variable is used to control logs.
const LOG_ENV_VAR: &str = "LOG";

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
    /// Compile ONTOL (.on) files
    Compile(Compile),
    /// Generate schemas from ONTOL (.on) files
    Generate(Generate),
    /// Run ontool in language server mode
    Lsp,
    /// Run ontool in serve mode
    Serve(Serve),
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Check {
    /// ONTOL (.on) files to check for errors
    files: Vec<String>,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Compile {
    /// Output file (default = "ontology")
    #[arg(short)]
    output: Option<PathBuf>,

    /// Specify a domain to be backed by a data store
    #[arg(short('d'), long)]
    data_store: Option<String>,

    /// Specify a data store backend (e.g. "arangodb")
    #[arg(short('b'), long)]
    backend: Option<String>,

    /// Directory of ontol files to compile
    #[arg(short('w'), long, default_value = ".")]
    dir: PathBuf,

    /// ONTOL (.on) files to compile
    files: Vec<PathBuf>,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Serve {
    /// Output file (default = "ontology")
    #[arg(short)]
    output: Option<PathBuf>,

    /// Specify a domain to be backed by a data store
    #[arg(short('d'), long)]
    data_store: Option<String>,

    /// Specify a data store backend (e.g. "arangodb")
    #[arg(short('b'), long)]
    backend: Option<String>,

    /// Directory of ontol files to compile
    #[arg(short('w'), long, default_value = ".")]
    dir: PathBuf,

    /// The root files of the ontology
    root_files: Vec<PathBuf>,
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Generate {
    /// Generate JSON schema from public types
    #[arg(short, long)]
    json_schema: bool,
    /// Generate JSON schema in YAML format from public types
    #[arg(short, long)]
    yaml_schema: bool,
    /// Directory of ontol files to compile
    #[arg(short('w'), long, default_value = ".")]
    dir: PathBuf,
    /// ONTOL (.on) files to generate schemas from
    files: Vec<PathBuf>,
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
    #[error("{0:?}")]
    SerdeYaml(#[from] serde_yaml::Error),
    #[error("{0}")]
    IO(#[from] std::io::Error),
}

#[derive(Clone)]
pub enum ChannelMessage {
    Reload,
}

async fn ws_upgrade_handler(ws: WebSocketUpgrade, tx: Sender<ChannelMessage>) -> Response {
    ws.on_upgrade(|socket| handle_ws(socket, tx))
}

async fn handle_ws(mut socket: WebSocket, tx: Sender<ChannelMessage>) {
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

pub async fn run() -> Result<(), OntoolError> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Check(args)) => {
            init_tracing();
            check(&args)
        }
        Some(Commands::Compile(args)) => {
            init_tracing();

            let compile_output = compile(args.dir, args.files, args.data_store, args.backend)?;

            let output_path = args.output.unwrap_or_else(|| PathBuf::from("ontology"));

            let output_file = File::create(output_path).unwrap();
            compile_output
                .ontology
                .try_serialize_to_bincode(output_file)
                .unwrap();

            Ok(())
        }
        Some(Commands::Serve(args)) => {
            init_tracing();
            let (tx, rx) = std::sync::mpsc::channel();
            let (reload_tx, _reload_rx) = broadcast::channel::<ChannelMessage>(16);
            let mut debouncer = new_debouncer(Duration::from_secs(1), None, tx).unwrap();

            debouncer
                .watcher()
                .watch(&args.dir, RecursiveMode::NonRecursive)
                .unwrap();
            let router_cell: Arc<Mutex<Option<axum::Router>>> = Arc::new(Mutex::new(None));
            tokio::spawn({
                let app = router_cell.clone();
                let reload_tx = reload_tx.clone();
                async move {
                    const SERVER_SOCKET_ADDR: &str = "0.0.0.0:5000";
                    let detach = Detach { router: app };

                    let outer_router: axum::Router = axum::Router::new()
                        .nest_service("/d", detach)
                        .route("/ws", get(|socket| ws_upgrade_handler(socket, reload_tx)));
                    info!("Binding server to {SERVER_SOCKET_ADDR}");
                    let _ = axum::Server::bind(&SERVER_SOCKET_ADDR.parse().unwrap())
                        .serve(outer_router.into_make_service())
                        .await;
                }
            });

            let mut cancellation_token = CancellationToken::new();
            // initial compilation
            let compile_output = compile(
                args.dir.clone(),
                args.root_files.clone(),
                args.data_store.clone(),
                args.backend.clone(),
            );
            match compile_output {
                Ok(output) => {
                    let new_app = app(output.ontology).await;
                    let mut lock = router_cell.lock().unwrap();
                    *lock = Some(new_app);
                }
                Err(error) => println!("{:?}", error),
            }

            for res in rx {
                match res {
                    Ok(debounced_event_vec) => {
                        for debounced_event in debounced_event_vec {
                            if debounced_event.event.kind.is_modify()
                                || debounced_event.event.kind.is_create()
                            {
                                cancellation_token.cancel();
                                cancellation_token = CancellationToken::new();
                                let compile_output = compile(
                                    args.dir.clone(),
                                    args.root_files.clone(),
                                    args.data_store.clone(),
                                    args.backend.clone(),
                                );
                                match compile_output {
                                    Ok(output) => {
                                        let new_app = app(output.ontology).await;
                                        let mut lock = router_cell.lock().unwrap();
                                        *lock = Some(new_app);
                                        let _ = reload_tx.send(ChannelMessage::Reload);
                                    }
                                    Err(error) => {
                                        let mut lock = router_cell.lock().unwrap();
                                        *lock = None;
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
        Some(Commands::Generate(args)) => {
            init_tracing();

            let CompileOutput {
                ontology,
                root_package,
            } = compile(args.dir, args.files, None, None)?;

            let domain = ontology.find_domain(root_package).unwrap();
            let schemas = build_openapi_schemas(&ontology, root_package, domain);

            if args.json_schema {
                let schemas_json = serde_json::to_string_pretty(&schemas)?;
                println!("{}", schemas_json);
            }

            if args.yaml_schema {
                let schemas_yaml = serde_yaml::to_string(&schemas)?;
                println!("{}", schemas_yaml);
            }

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
        .with(tracing_subscriber::fmt::layer())
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

fn check(args: &Check) -> Result<(), OntoolError> {
    for filename in &args.files {
        let source = fs::read_to_string(filename)?;
        let (_stmts, errors) = parse_statements(&source);

        if errors.is_empty() {
            println!("{}: no errors found.", filename);
        } else {
            let mut colors = ColorGenerator::new();
            for error in errors {
                let (span, message) = match error {
                    ontol_parser::Error::Lex(err) => (err.span(), format!("lex error: {}", err)),
                    ontol_parser::Error::Parse(err) => {
                        (err.span(), format!("parse error: {}", err))
                    }
                };
                Report::build(ReportKind::Error, filename, span.start)
                    .with_label(
                        Label::new((filename, span))
                            .with_message(message)
                            .with_color(colors.next()),
                    )
                    .finish()
                    .eprint((filename, Source::from(&source)))?;
            }
            return Err(OntoolError::Parse);
        }
    }
    Ok(())
}

struct CompileOutput {
    ontology: Ontology,
    root_package: PackageId,
}

fn compile(
    root_dir: PathBuf,
    root_paths: Vec<PathBuf>,
    data_store_domain: Option<String>,
    backend: Option<String>,
) -> Result<CompileOutput, OntoolError> {
    if root_paths.is_empty() {
        return Err(OntoolError::NoInputFiles);
    }

    let root_file_name = get_source_name(root_paths.first().unwrap());
    let mut ontol_sources = Sources::default();
    let mut sources_by_name: HashMap<String, String> = Default::default();
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

            sources_by_name.insert(source_name.clone(), source);
            paths_by_name.insert(source_name, path);
        }
    }

    let mut source_code_registry = SourceCodeRegistry::default();
    let mut package_graph_builder =
        PackageGraphBuilder::with_roots([root_file_name.clone().into()]);
    let mut root_package = None;

    let topology = loop {
        let graph_state = package_graph_builder.transition().map_err(|err| {
            print_unified_compile_error(err, &ontol_sources, &sources_by_name).unwrap();
            OntoolError::Compile
        })?;

        match graph_state {
            GraphState::RequestPackages { builder, requests } => {
                package_graph_builder = builder;

                for request in requests {
                    let source_name = match &request.reference {
                        PackageReference::Named(source_name) => source_name.as_str(),
                    };

                    if source_name == root_file_name {
                        root_package = Some(request.package_id);
                    }

                    let mut package_config = PackageConfig::default();

                    if Some(source_name) == data_store_domain.as_deref() {
                        package_config.data_store = match &backend {
                            Some(backend) => Some(DataStoreConfig::ByName(backend.into())),
                            None => Some(DataStoreConfig::Default),
                        }
                    }
                    if let Some(source_text) = sources_by_name.get(source_name) {
                        package_graph_builder.provide_package(ParsedPackage::parse(
                            request,
                            source_text,
                            package_config,
                            &mut ontol_sources,
                            &mut source_code_registry,
                        ));
                    } else {
                        eprintln!("Could not load `{source_name}`");
                    }
                }
            }
            GraphState::Built(topology) => break topology,
        }
    };

    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, ontol_sources.clone()).with_ontol();
    match compiler.compile_package_topology(topology) {
        Ok(()) => Ok(CompileOutput {
            ontology: compiler.into_ontology(),
            root_package: root_package.expect("No root package"),
        }),
        Err(err) => {
            print_unified_compile_error(err, &ontol_sources, &sources_by_name)?;
            Err(OntoolError::Compile)
        }
    }
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
    sources_by_name: &HashMap<String, String>,
) -> Result<(), OntoolError> {
    let mut colors = ColorGenerator::new();
    for error in unified_error.errors.iter() {
        let span = error.span.start as usize..error.span.end as usize;
        let message = error.error.to_string();

        let ontol_source = ontol_sources.get_source(error.span.source_id).unwrap();
        let literal_source = sources_by_name.get(ontol_source.name.as_ref()).unwrap();

        Report::build(ReportKind::Error, ontol_source.name(), span.start)
            .with_label(
                Label::new((ontol_source.name(), span))
                    .with_message(message)
                    .with_color(colors.next()),
            )
            .finish()
            .eprint((ontol_source.name(), Source::from(&literal_source)))?;

        for note in &error.notes {
            let span = note.span.start as usize..note.span.end as usize;
            let message = note.note.to_string();

            let ontol_source = ontol_sources.get_source(error.span.source_id).unwrap();
            let literal_source = sources_by_name.get(ontol_source.name.as_ref()).unwrap();

            Report::build(ReportKind::Advice, ontol_source.name(), span.start)
                .with_label(
                    Label::new((ontol_source.name(), span))
                        .with_message(message)
                        .with_color(colors.next()),
                )
                .finish()
                .eprint((ontol_source.name(), Source::from(&literal_source)))?;
        }
    }

    Ok(())
}

async fn lsp() -> Result<(), OntoolError> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(Backend::new);
    Server::new(stdin, stdout, socket).serve(service).await;
    Ok(())
}
