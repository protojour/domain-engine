#![allow(unused)]

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use domain_engine_core::{DomainEngine, Session};
use domain_engine_graphql::context::ServiceCtx;
use domain_engine_in_memory_store::InMemoryDataStoreFactory;
use domain_engine_test_utils::graphql_test_utils::Exec;
use indoc::indoc;
use ontol_compiler::{
    mem::Mem,
    package::{GraphState, PackageGraphBuilder, ParsedPackage},
    SourceCodeRegistry, Sources,
};
use ontol_runtime::{
    interface::serde::operator::SerdeOperatorAddr,
    ontology::{
        config::{DataStoreConfig, PackageConfig},
        Ontology,
    },
    PackageId,
};
use ontol_test_utils::{
    examples::conduit::{BLOG_POST_PUBLIC, CONDUIT_DB, FEED_PUBLIC},
    src_name, SrcName, TestCompile, TestPackages,
};
use ontool::System;
use serde::de::DeserializeSeed;
use tokio::runtime::Runtime;

const TINY: (SrcName, &str) = (
    src_name("tiny"),
    indoc! {r#"
        def created (
            rel .'created'[rel .gen: create_time]?: datetime
        )

        def foo_id (
            fmt '' => 'foos/' => uuid => .
        )

        /// This is the documentation string for...
        /// .. foo!
        def foo (
            rel .'_id'[rel .gen: auto]|id: foo_id
            rel .is: created
            rel .'name': text
        )

        map foos (
            (),
            foo { ..@match foo() }
        )
        "#,
    },
);

fn foo_operator_addr(ontology: &Ontology) -> SerdeOperatorAddr {
    let (_, domain) = ontology
        .domains()
        .find(|domain| {
            let name = &ontology[domain.1.unique_name()];
            name == TINY.0.as_str()
        })
        .unwrap();
    let type_info = domain
        .find_type_info_by_name(ontology.find_text_constant("foo").unwrap())
        .unwrap();
    type_info.operator_addr.unwrap()
}

pub fn compile_benchmark(c: &mut Criterion) {
    c.bench_function("compile_tiny", |b| {
        b.iter(|| {
            TestPackages::with_static_sources(black_box([TINY]))
                .bench_disable_ontology_serde()
                .compile();
        })
    });
    c.bench_function("compile_conduit", |b| {
        b.iter(|| {
            TestPackages::with_static_sources(black_box([
                BLOG_POST_PUBLIC,
                FEED_PUBLIC,
                CONDUIT_DB,
            ]))
            .bench_disable_ontology_serde()
            .compile();
        })
    });

    c.bench_function("serialize_ontology_bincode", |b| {
        let test = TestPackages::with_static_sources(black_box([TINY])).compile();
        b.iter(|| {
            let mut binary_ontology: Vec<u8> = Vec::new();
            test.ontology()
                .try_serialize_to_bincode(&mut binary_ontology)
                .unwrap();
            Ontology::try_from_bincode(binary_ontology.as_slice()).unwrap()
        })
    });

    c.bench_function("serde_json_deserialize", |b| {
        let test = TestPackages::with_static_sources(black_box([TINY])).compile();
        let processor = test.ontology().new_serde_processor(
            foo_operator_addr(test.ontology()),
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
        let test = TestPackages::with_static_sources(black_box([TINY])).compile();
        let processor = test.ontology().new_serde_processor(
            foo_operator_addr(test.ontology()),
            ontol_runtime::interface::serde::processor::ProcessorMode::Raw,
        );

        let json = r#"{"_id": "foos/77a7c1dd-09d9-4bb1-bc3b-949d931cbdd6","name": ""}"#;

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

    c.bench_function("graphql_create_schema", |b| {
        let test = TestPackages::with_static_sources(black_box([TINY])).compile();
        let (package_id, _) = test
            .ontology()
            .domains()
            .find(|domain| {
                let name = &test.ontology()[domain.1.unique_name()];
                name == TINY.0.as_str()
            })
            .unwrap();
        b.iter(|| {
            domain_engine_graphql::create_graphql_schema(
                black_box(test.ontology_owned()),
                black_box(*package_id),
            )
            .unwrap()
        });
    });

    c.bench_function("graphql_create_entity", |b| {
        let test = TestPackages::with_static_sources(black_box([TINY])).compile();
        let rt = Runtime::new().unwrap();

        let engine = rt.block_on(async {
            Arc::new(
                DomainEngine::builder(test.ontology_owned())
                    .system(Box::<System>::default())
                    .build(InMemoryDataStoreFactory, Session::default())
                    .await
                    .unwrap(),
            )
        });
        let (package_id, _) = test
            .ontology()
            .domains()
            .find(|domain| {
                let name = &test.ontology()[domain.1.unique_name()];
                name == TINY.0.as_str()
            })
            .unwrap();
        let schema =
            domain_engine_graphql::create_graphql_schema(test.ontology_owned(), *package_id)
                .unwrap();
        let service_context: ServiceCtx = engine.into();
        b.iter(|| {
            rt.block_on(async {
                r#"mutation {
                    foo(create: [{name: "heihei"}]) {
                        node {
                            _id
                            name
                        }
                    }
                }"#
                .exec([], &schema, &service_context)
                .await
                .unwrap();
            });
        });
    });
}

criterion_group!(benches, compile_benchmark);
criterion_main!(benches);
