#![allow(unused)]

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ontol_compiler::{
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, ParsedPackage},
    Compiler, SourceCodeRegistry, Sources,
};
use ontol_runtime::{
    interface::serde::operator::SerdeOperatorAddr,
    ontology::{config::PackageConfig, Ontology},
};
use ontol_test_utils::TestCompile;
use serde::de::DeserializeSeed;

const BENCH_DOMAIN: &str = r#"
def created (
    rel .'created'[rel .gen: create_time]?: datetime
)
def foo_id (
    fmt '' => 'foos/' => text => .
)
def foo (
    rel .'_id'[rel .gen: auto]|id: foo_id
    rel .is: created
    rel .'name': text
)

map foos (
    (),
    foo { ..@match foo() }
)
"#;

fn bench_compile() -> Ontology {
    let mut package_graph_builder = PackageGraphBuilder::with_roots(["bench.on".into()]);

    let mut ontol_sources = Sources::default();
    let mut source_code_registry = SourceCodeRegistry::default();

    let topology = loop {
        let graph_state = package_graph_builder.transition().unwrap();

        match graph_state {
            GraphState::RequestPackages { builder, requests } => {
                package_graph_builder = builder;

                for request in requests {
                    package_graph_builder.provide_package(ParsedPackage::parse(
                        request,
                        black_box(BENCH_DOMAIN),
                        PackageConfig::default(),
                        &mut ontol_sources,
                        &mut source_code_registry,
                    ));
                }
            }
            GraphState::Built(topology) => break topology,
        }
    };

    let mem = Mem::default();
    let mut compiler = Compiler::new(&mem, ontol_sources.clone()).with_ontol();
    compiler.compile_package_topology(topology).unwrap();
    compiler.into_ontology()
}

fn foo_operator_addr(ontology: &Ontology) -> SerdeOperatorAddr {
    let (_, domain) = ontology
        .domains()
        .find(|domain| {
            let name = &ontology[domain.1.unique_name()];
            name == "bench.on"
        })
        .unwrap();
    let type_info = domain
        .find_type_info_by_name(ontology.find_text_constant("foo").unwrap())
        .unwrap();
    type_info.operator_addr.unwrap()
}

pub fn compile_benchmark(c: &mut Criterion) {
    c.bench_function("compile", |b| b.iter(bench_compile));

    c.bench_function("serialize_ontology_bincode", |b| {
        let ontology = bench_compile();
        b.iter(|| {
            let mut binary_ontology: Vec<u8> = Vec::new();
            ontology
                .try_serialize_to_bincode(&mut binary_ontology)
                .unwrap();
            Ontology::try_from_bincode(binary_ontology.as_slice()).unwrap()
        })
    });

    c.bench_function("serde_json_deserialize", |b| {
        let ontology = bench_compile();
        let processor = ontology.new_serde_processor(
            foo_operator_addr(&ontology),
            ontol_runtime::interface::serde::processor::ProcessorMode::Create,
        );

        let json = r#"{"name": ""}"#;

        b.iter(|| {
            processor
                .deserialize(&mut serde_json::Deserializer::from_str(black_box(json)))
                .unwrap();
        });
    });

    c.bench_function("serde_json_serialize", |b| {
        let ontology = bench_compile();
        let processor = ontology.new_serde_processor(
            foo_operator_addr(&ontology),
            ontol_runtime::interface::serde::processor::ProcessorMode::Raw,
        );

        let json = r#"{"_id": "foos/foo","name": ""}"#;

        let ontol_value = processor
            .deserialize(&mut serde_json::Deserializer::from_str(json))
            .unwrap()
            .val;

        b.iter(|| {
            let mut buf: Vec<u8> = vec![];
            processor
                .serialize_value(
                    black_box(&ontol_value),
                    None,
                    &mut serde_json::Serializer::new(&mut buf),
                )
                .unwrap();
        });
    });
}

criterion_group!(benches, compile_benchmark);
criterion_main!(benches);
