#![forbid(unsafe_code)]

use std::{fs::File, path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context};
use clap::{Parser, ValueEnum};
use clap_stdin::FileOrStdin;
use ontol_runtime::{
    attr::{Attr, AttrRef},
    interface::serde::processor::ProcessorMode,
    ontology::Ontology,
    vm::VmState,
    MapDef, MapDefFlags, MapFlags, MapKey, PackageId,
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
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let args = Args::parse();
    let ontology = Arc::new(load_ontology(args.ontology)?);
    let Ok(pkg_id) = PackageId::from_u16(1) else {
        return Err(anyhow!("invalid package"));
    };
    let domain = ontology.find_domain(pkg_id).unwrap();
    let from_name = ontology.find_text_constant(args.from.as_str()).unwrap();
    let to_name = ontology.find_text_constant(args.to.as_str()).unwrap();
    let input = MapDef {
        def_id: domain
            .find_def_by_name(from_name)
            .expect("--from def not found in domain")
            .id,
        flags: MapDefFlags::empty(),
    };
    let output = MapDef {
        def_id: domain
            .find_def_by_name(to_name)
            .expect("--to def not found in domain")
            .id,
        flags: MapDefFlags::empty(),
    };
    let proc = ontology
        .get_mapper_proc(&MapKey {
            input,
            output,
            flags: MapFlags::empty(),
        })
        .ok_or_else(|| anyhow!(format!("No map from {} to {}", args.from, args.to)))?;

    let from_def = ontology.def(input.def_id);
    let from_processor = ontology.new_serde_processor(
        from_def.operator_addr.expect("No deserializer found"),
        ProcessorMode::Read,
    );
    let attr = match args.format {
        Format::Json => from_processor
            .deserialize(&mut serde_json::Deserializer::from_reader(
                args.input.into_reader()?,
            ))
            .expect("Deserialization failed"),
    };
    let Attr::Unit(value) = attr else {
        return Err(anyhow!("not a unit attribute"));
    };
    let value = match ontology.new_vm(proc).run([value])? {
        VmState::Complete(value) => value,
        VmState::Yield(_) => return Err(anyhow!("ONTOL-VM yielded!")),
    };
    let to_def = ontology.def(output.def_id);
    let to_processor = ontology.new_serde_processor(
        to_def.operator_addr.expect("No deserializer found"),
        ProcessorMode::Create,
    );
    let mut buf: Vec<u8> = vec![];
    match args.format {
        Format::Json => {
            to_processor
                .serialize_attr(
                    AttrRef::Unit(&value),
                    &mut serde_json::Serializer::new(&mut buf),
                )
                .expect("Serialization failed");
            let output: serde_json::Value = serde_json::from_slice(&buf).unwrap();
            println!("{}", serde_json::to_string_pretty(&output).unwrap());
        }
    }
    Ok(())
}

fn load_ontology(path: PathBuf) -> anyhow::Result<Ontology> {
    let ontology_file = File::open(path).context("Ontology file not found")?;
    Ontology::try_from_bincode(ontology_file).context("Problem reading ontology")
}
