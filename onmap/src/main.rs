#![forbid(unsafe_code)]

use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use clap::{Parser, ValueEnum};
use clap_stdin::FileOrStdin;
use domain_engine_core::system::SystemAPI;
use ontol_runtime::{
    interface::serde::processor::ProcessorMode, ontology::Ontology, vm::VmState, MapKey, PackageId,
};
use serde::de::DeserializeSeed;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// onmap – ONTOL domain mapper tool
///
/// Command-line tool for Memoriam Domain Engine conversion of
/// structured input data in JSON or YAML format.
///
/// --from and --to must correspond to mapped defs in the
/// compiled ontology binary.
///
/// Data in INPUT must correspond to the structure of the
/// --from def.
///
/// onmap accepts input from stdin by replacing INPUT with,
/// -- -,  and outputs to stdout.
#[derive(Parser)]
#[command(version, about, arg_required_else_help(true))]
struct Args {
    /// Compiled ontology binary
    #[arg(short, long)]
    ontology: PathBuf,

    /// Input/output format
    #[arg(long, default_value = "json")]
    format: Format,

    /// Input data
    input: FileOrStdin,

    /// Input type def
    #[arg(short, long)]
    from: String,

    /// Output type def
    #[arg(short, long)]
    to: String,
}

#[derive(Clone, ValueEnum)]
enum Format {
    Json,
    Yaml,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let args = Args::parse();
    let ontology = Arc::new(load_ontology(args.ontology)?);
    let domain = ontology.find_domain(PackageId(1)).unwrap();
    let from = MapKey {
        def_id: *domain
            .type_names
            .get(args.from.as_str())
            .expect("--from def not found in domain"),
        seq: false,
    };
    let to = MapKey {
        def_id: *domain
            .type_names
            .get(args.to.as_str())
            .expect("--to def not found in domain"),
        seq: false,
    };
    let proc = ontology
        .get_mapper_proc([from, to])
        .ok_or_else(|| anyhow!(format!("No map from {} to {}", args.from, args.to)))?;

    let from_type = ontology.get_type_info(from.def_id);
    let from_processor = ontology.new_serde_processor(
        from_type.operator_addr.expect("No deserializer found"),
        ProcessorMode::Read,
    );
    let data = match args.format {
        Format::Json => from_processor
            .deserialize(&mut serde_json::Deserializer::from_str(&args.input))
            .expect("Deserialization failed"),
        Format::Yaml => from_processor
            .deserialize(serde_yaml::Deserializer::from_str(&args.input))
            .expect("Deserialization failed"),
    };
    let value = match ontology.new_vm(proc).run([data.value]) {
        VmState::Complete(value) => value,
        VmState::Yielded(_) => return Err(anyhow!("ONTOL-VM yielded!")),
    };
    let to_type = ontology.get_type_info(to.def_id);
    let to_processor = ontology.new_serde_processor(
        to_type.operator_addr.expect("No deserializer found"),
        ProcessorMode::Create,
    );
    let mut buf: Vec<u8> = vec![];
    match args.format {
        Format::Json => {
            to_processor
                .serialize_value(&value, None, &mut serde_json::Serializer::new(&mut buf))
                .expect("Serialization failed");
            let output: serde_json::Value = serde_json::from_slice(&buf).unwrap();
            println!("{}", serde_json::to_string_pretty(&output).unwrap());
        }
        Format::Yaml => {
            to_processor
                .serialize_value(&value, None, &mut serde_yaml::Serializer::new(&mut buf))
                .expect("Serialization failed");
            let output: serde_yaml::Value = serde_yaml::from_slice(&buf).unwrap();
            print!("{}", serde_yaml::to_string(&output).unwrap());
        }
    }
    Ok(())
}

fn load_ontology(path: PathBuf) -> anyhow::Result<Ontology> {
    let ontology_file = File::open(path).context("Ontology file not found")?;
    bincode::deserialize_from(&ontology_file).context("Problem reading ontology")
}

struct System;

impl SystemAPI for System {
    fn current_time(&self) -> DateTime<Utc> {
        Utc::now()
    }
}
