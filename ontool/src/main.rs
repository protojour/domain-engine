use ariadne::{ColorGenerator, Label, Report, ReportKind, Source};
use clap::{Args, Parser, Subcommand};
use ontol_compiler::{
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    Compiler, SourceCodeRegistry, Sources,
};
use ontol_parser::parse_statements;
use ontol_runtime::json_schema::build_openapi_schemas;
use std::{collections::HashMap, fs};
use thiserror::Error;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

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
    /// Generate schemas from ONTOL (.on) files
    Generate(Generate),
}

#[derive(Args)]
#[command(arg_required_else_help(true))]
struct Check {
    /// ONTOL (.on) files to check for errors
    files: Vec<String>,
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
    files: Vec<String>,
}

#[derive(Debug, Error)]
enum OntoolError {
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

fn main() -> Result<(), OntoolError> {
    tracing_subscriber::fmt().with_target(false).init();

    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Check(args)) => check(args),
        Some(Commands::Generate(args)) => generate(args),
        None => Ok(()),
    }
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

fn generate(args: &Generate) -> Result<(), OntoolError> {
    for filename in &args.files {
        let source = fs::read_to_string(filename)?;

        let sources_by_name: HashMap<String, String> = [(filename.clone(), source.clone())].into();
        let mut sources = Sources::default();
        let mut source_code_registry = SourceCodeRegistry::default();
        let mut package_graph_builder = PackageGraphBuilder::new(filename.clone().into());
        let mut root_package = None;

        let topology = loop {
            match package_graph_builder.transition().unwrap() {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        let source_name = match &request.reference {
                            PackageReference::Named(source_name) => source_name.as_str(),
                        };

                        if source_name == filename {
                            root_package = Some(request.package_id);
                        }

                        if let Some(source_text) = sources_by_name.get(source_name) {
                            package_graph_builder.provide_package(ParsedPackage::parse(
                                request,
                                source_text,
                                &mut sources,
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
        let mut compiler = Compiler::new(&mem, sources.clone()).with_core();
        match compiler.compile_package_topology(topology) {
            Ok(_) => {
                let env = compiler.into_env();
                if let Some(package_id) = root_package {
                    let domain = env.find_domain(package_id).unwrap();
                    let schemas = build_openapi_schemas(&env, package_id, domain);

                    if args.json_schema {
                        let schemas_json = serde_json::to_string_pretty(&schemas)?;
                        println!("{}", schemas_json);
                    }

                    if args.yaml_schema {
                        let schemas_yaml = serde_yaml::to_string(&schemas)?;
                        println!("{}", schemas_yaml);
                    }
                }
            }
            Err(err) => {
                let mut colors = ColorGenerator::new();
                for error in err.errors.iter() {
                    let span = error.span.start as usize..error.span.end as usize;
                    let message = error.error.to_string();
                    Report::build(ReportKind::Error, filename, span.start)
                        .with_label(
                            Label::new((filename, span))
                                .with_message(message)
                                .with_color(colors.next()),
                        )
                        .finish()
                        .eprint((filename, Source::from(&source)))?;
                }
                return Err(OntoolError::Compile);
            }
        }
    }
    Ok(())
}
