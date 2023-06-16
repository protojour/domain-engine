use clap::{Args, Parser, Subcommand};
use ontol_compiler::{
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, PackageReference, ParsedPackage},
    Compiler, SourceCodeRegistry, Sources,
};
use ontol_parser::parse_statements;
use std::{collections::HashMap, fs};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// ONTOL – ONTOlogy Language compiler
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
    /// Generate GraphQL schema from public types
    #[arg(short, long)]
    graphql: bool,
    /// Generate JSON schema from public types
    #[arg(short, long)]
    json_schema: bool,
    /// Generate JSON schema in YAML format from public types
    #[arg(short, long)]
    yaml_schema: bool,
    /// ONTOL (.on) files to generate schemas from
    files: Vec<String>,
}

fn main() {
    tracing_subscriber::fmt().with_target(false).init();

    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Check(args)) => check(args),
        Some(Commands::Generate(args)) => generate(args),
        None => {}
    }
}

fn check(args: &Check) {
    for file in &args.files {
        let source = fs::read_to_string(file).expect("File not found");

        let (_statements, errors) = parse_statements(&source);
        if errors.len() == 0 {
            println!("No errors found!");
        } else {
            for _error in errors {
                println!("{}", "1 error");
            }
        }
    }
}

fn generate(args: &Generate) {
    for filename in &args.files {
        let source = fs::read_to_string(filename).expect("File not found");

        let sources_by_name: HashMap<String, String> = [(filename.clone(), source)].into();
        let mut sources = Sources::default();
        let mut source_code_registry = SourceCodeRegistry::default();
        let mut package_graph_builder = PackageGraphBuilder::new(filename.clone().into());
        let mut _root_package = None;

        let topology = loop {
            match package_graph_builder.transition().unwrap() {
                GraphState::RequestPackages { builder, requests } => {
                    package_graph_builder = builder;

                    for request in requests {
                        let source_name = match &request.reference {
                            PackageReference::Named(source_name) => source_name.as_str(),
                        };

                        if source_name == filename {
                            _root_package = Some(request.package_id);
                        }

                        if let Some(source_text) = sources_by_name.get(source_name) {
                            package_graph_builder.provide_package(ParsedPackage::parse(
                                request,
                                source_text,
                                &mut sources,
                                &mut source_code_registry,
                            ));
                        } else {
                            println!("Could not load `{source_name}`");
                        }
                    }
                }
                GraphState::Built(topology) => break topology,
            }
        };

        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, sources.clone()).with_core();
        compiler.compile_package_topology(topology).unwrap();
        let _env = compiler.into_env();
    }
}
