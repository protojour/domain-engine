use std::{hint::black_box, sync::Arc};

use criterion::{Criterion, criterion_group, criterion_main};
use domain_engine_core::{DomainEngine, Session};
use domain_engine_graphql::domain::context::ServiceCtx;
use domain_engine_store_inmemory::InMemoryConnection;
use domain_engine_test_utils::graphql_test_utils::Exec;
use indoc::indoc;
use ontol_core::url::DomainUrl;
use ontol_examples::{
    AsAtlas,
    conduit::{blog_post_public, conduit_db, feed_public},
};
use ontol_runtime::{
    attr::AttrRef, interface::serde::operator::SerdeOperatorAddr, ontology::Ontology,
};
use ontol_test_utils::{TestCompile, TestPackages, file_url};
use ontool::System;
use serde::de::DeserializeSeed;
use tokio::runtime::Runtime;

fn tiny() -> (DomainUrl, &'static str) {
    (
        file_url("tiny"),
        indoc! {r#"
            domain ZZZZZZZZZZZTESTZZZZZZZZZZZ (
                rel. name: 'tiny'
            )
            def @macro created (
                rel* 'created'[rel* gen: create_time]?: datetime
            )

            def foo_id (
                fmt '' => 'foos/' => uuid => .
            )

            /// This is the documentation string for...
            /// .. foo!
            def foo (
                rel. '_id'[rel* gen: auto]: foo_id
                rel* is: created
                rel* 'name': text
            )

            map foos (
                (),
                foo { ..@match foo() }
            )
            "#,
        },
    )
}

fn foo_operator_addr(ontology: &Ontology) -> SerdeOperatorAddr {
    let (_, domain) = ontology
        .domains()
        .find(|domain| {
            let name = &ontology[domain.1.unique_name()];
            name == "tiny"
        })
        .unwrap();
    let def = domain
        .find_def_by_name(ontology.find_text_constant("foo").unwrap())
        .unwrap();
    def.operator_addr.unwrap()
}

pub fn compile_benchmark(c: &mut Criterion) {
    c.bench_function("compile_tiny", |b| {
        b.iter(|| {
            TestPackages::with_static_sources(black_box([tiny()]))
                .bench_disable_ontology_serde()
                .compile();
        })
    });
    c.bench_function("compile_conduit", |b| {
        b.iter(|| {
            TestPackages::with_sources(black_box([
                blog_post_public(),
                feed_public(),
                conduit_db().as_atlas("conduit"),
            ]))
            .bench_disable_ontology_serde()
            .compile();
        })
    });

    c.bench_function("serialize_ontology_postcard", |b| {
        let test = TestPackages::with_static_sources(black_box([tiny()])).compile();
        b.iter(|| {
            let mut binary_ontology: Vec<u8> = Vec::new();
            test.ontology()
                .try_serialize_to_postcard(&mut binary_ontology)
                .unwrap();
            Ontology::try_from_postcard(binary_ontology.as_slice()).unwrap()
        })
    });

    c.bench_function("serde_json_deserialize", |b| {
        let test = TestPackages::with_static_sources(black_box([tiny()])).compile();
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
        let test = TestPackages::with_static_sources(black_box([tiny()])).compile();
        let processor = test.ontology().new_serde_processor(
            foo_operator_addr(test.ontology()),
            ontol_runtime::interface::serde::processor::ProcessorMode::Raw,
        );

        let json = r#"{"_id": "foos/77a7c1dd-09d9-4bb1-bc3b-949d931cbdd6","name": ""}"#;

        let ontol_value = processor
            .deserialize(&mut serde_json::Deserializer::from_str(json))
            .unwrap()
            .into_unit()
            .unwrap();

        b.iter(|| {
            let mut buf: Vec<u8> = vec![];
            processor
                .serialize_attr(
                    black_box(AttrRef::Unit(&ontol_value)),
                    &mut serde_json::Serializer::new(&mut buf),
                )
                .unwrap();
        });
    });

    c.bench_function("graphql_create_schema", |b| {
        let test = TestPackages::with_static_sources(black_box([tiny()])).compile();
        let (domain_index, _) = test
            .ontology()
            .domains()
            .find(|domain| {
                let name = &test.ontology()[domain.1.unique_name()];
                name == "tiny"
            })
            .unwrap();
        b.iter(|| {
            domain_engine_graphql::domain::create_graphql_schema(
                black_box(test.ontology_owned()),
                black_box(domain_index),
            )
            .unwrap()
        });
    });

    c.bench_function("graphql_create_entity", |b| {
        let test = TestPackages::with_static_sources(black_box([tiny()])).compile();
        let rt = Runtime::new().unwrap();

        let engine = rt.block_on(async {
            Arc::new(
                DomainEngine::builder(test.ontology_owned())
                    .system(Box::<System>::default())
                    .build(InMemoryConnection, Session::default())
                    .await
                    .unwrap(),
            )
        });
        let (domain_index, _) = test
            .ontology()
            .domains()
            .find(|domain| {
                let name = &test.ontology()[domain.1.unique_name()];
                name == "tiny"
            })
            .unwrap();
        let schema = domain_engine_graphql::domain::create_graphql_schema(
            test.ontology_owned(),
            domain_index,
        )
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
