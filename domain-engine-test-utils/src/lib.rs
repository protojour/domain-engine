#![forbid(unsafe_code)]

use domain_engine_core::DomainEngine;
use ontol_runtime::{
    condition::{CondTerm, Condition},
    interface::serde::processor::ProcessorMode,
    select::{EntitySelect, StructOrUnionSelect, StructSelect},
    value::{Data, Value},
    var::Var,
    PackageId,
};
use serde::de::DeserializeSeed;

pub mod graphql_test_utils;
pub mod parser_document_utils;

#[async_trait::async_trait]
pub trait DomainEngineTestExt {
    async fn exec_named_map_json(
        &self,
        key: (PackageId, &str),
        input_json: serde_json::Value,
        find_query: TestFindQuery,
    ) -> serde_json::Value;
}

#[async_trait::async_trait]
impl DomainEngineTestExt for DomainEngine {
    async fn exec_named_map_json(
        &self,
        (package_id, name): (PackageId, &str),
        input_json: serde_json::Value,
        find_query: TestFindQuery,
    ) -> serde_json::Value {
        let value = test_exec_named_map(self, (package_id, name), input_json, find_query).await;

        let ontology = self.ontology();
        let [_, to] = ontology
            .get_named_forward_map_meta(package_id, name)
            .expect("Named map not found");

        let output_type_info = ontology.get_type_info(to.def_id);

        let mut json_buf: Vec<u8> = vec![];
        let mut serializer = serde_json::Serializer::new(&mut json_buf);

        let processor = match &value.data {
            Data::Sequence(_) => ontology.new_serde_processor(
                ontology.dynamic_sequence_operator_addr(),
                ProcessorMode::Raw,
            ),
            _ => ontology
                .new_serde_processor(output_type_info.operator_addr.unwrap(), ProcessorMode::Raw),
        };

        processor
            .serialize_value(&value, None, &mut serializer)
            .expect("Serialize output failed");

        serde_json::from_slice(&json_buf).unwrap()
    }
}

async fn test_exec_named_map(
    engine: &DomainEngine,
    (package_id, name): (PackageId, &str),
    input_json: serde_json::Value,
    mut find_query: TestFindQuery,
) -> Value {
    let ontology = engine.ontology();
    let [from, to] = ontology
        .get_named_forward_map_meta(package_id, name)
        .expect("Named map not found");

    let input_type_info = ontology.get_type_info(from.def_id);

    let input = ontology
        .new_serde_processor(input_type_info.operator_addr.unwrap(), ProcessorMode::Raw)
        .deserialize(input_json)
        .expect("Deserialize input failed");

    engine
        .exec_map([from, to], input.value, &mut find_query)
        .await
        .expect("Exec map failed")
}

pub struct TestFindQuery {
    limit: usize,
    include_total_len: bool,
}

impl TestFindQuery {
    pub fn limit(self, limit: usize) -> Self {
        Self { limit, ..self }
    }

    pub fn with_total_len(self) -> Self {
        Self {
            include_total_len: true,
            ..self
        }
    }
}

impl Default for TestFindQuery {
    fn default() -> Self {
        Self {
            limit: 20,
            include_total_len: false,
        }
    }
}

impl domain_engine_core::FindEntitySelect for TestFindQuery {
    fn find_select(&mut self, match_var: Var, condition: &Condition<CondTerm>) -> EntitySelect {
        let def_id = domain_engine_core::match_utils::find_entity_id_in_condition_for_var(
            condition, match_var,
        )
        .expect("Unable to detect an entity being queried");

        EntitySelect {
            source: StructOrUnionSelect::Struct(StructSelect {
                def_id,
                properties: Default::default(),
            }),
            condition: Default::default(),
            limit: self.limit,
            after_cursor: None,
            include_total_len: self.include_total_len,
        }
    }
}
