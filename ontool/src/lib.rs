#![forbid(unsafe_code)]

use ariadne::{ColorGenerator, Label, Report, ReportKind, Source};
use clap::{Args, Parser, Subcommand};
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
use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
};
use thiserror::Error;
use tower_lsp::{LspService, Server};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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

    /// Specify a domain file to be backed by a data store
    #[arg(short('d'), long)]
    data_store: Option<PathBuf>,

    /// Specify a data store backend (e.g. "arangodb")
    #[arg(short('b'), long)]
    backend: Option<String>,

    /// ONTOL (.on) files to compile
    files: Vec<PathBuf>,
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

pub async fn run() -> Result<(), OntoolError> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Check(args)) => {
            init_tracing();
            check(&args)
        }
        Some(Commands::Compile(args)) => {
            init_tracing();

            let compile_output = compile(args.files, args.data_store, args.backend)?;

            let output_path = args.output.unwrap_or_else(|| PathBuf::from("ontology"));

            let output_file = File::create(output_path).unwrap();
            compile_output
                .ontology
                .try_serialize_to_bincode(output_file)
                .unwrap();

            Ok(())
        }
        Some(Commands::Generate(args)) => {
            init_tracing();

            let CompileOutput {
                ontology,
                root_package,
            } = compile(args.files, None, None)?;

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
    paths: Vec<PathBuf>,
    data_store_path: Option<PathBuf>,
    backend: Option<String>,
) -> Result<CompileOutput, OntoolError> {
    if paths.is_empty() {
        return Err(OntoolError::NoInputFiles);
    }

    let root_file_name = get_source_name(paths.first().unwrap());
    let mut ontol_sources = Sources::default();
    let mut sources_by_name: HashMap<String, String> = Default::default();
    let mut paths_by_name: HashMap<String, PathBuf> = Default::default();

    for path in paths {
        let source = fs::read_to_string(&path)?;
        let source_name = get_source_name(&path);

        sources_by_name.insert(source_name.clone(), source);
        paths_by_name.insert(source_name, path);
    }

    let mut source_code_registry = SourceCodeRegistry::default();
    let mut package_graph_builder = PackageGraphBuilder::new(root_file_name.clone().into());
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

                    if let Some(path) = paths_by_name.get(source_name) {
                        if Some(path) == data_store_path.as_ref() {
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
