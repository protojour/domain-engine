#![forbid(unsafe_code)]

use std::sync::Arc;

use domain_engine_core::{DomainEngine, SelectMode, Session};
use ontol_runtime::{
    attr::AttrRef,
    interface::serde::processor::ProcessorMode,
    query::{
        condition::Condition,
        filter::Filter,
        select::{EntitySelect, StructOrUnionSelect, StructSelect},
    },
    value::Value,
    var::Var,
    DomainIndex,
};
use serde::de::DeserializeSeed;

pub mod dummy_session;
pub mod dynamic_data_store;
pub mod graphql_test_utils;
pub mod mock_datastore;
pub mod parser_document_utils;

pub use unimock;

#[async_trait::async_trait]
pub trait DomainEngineTestExt {
    async fn exec_named_map_json(
        self: &Arc<Self>,
        key: (DomainIndex, &str),
        input_json: serde_json::Value,
        find_query: TestFindQuery,
    ) -> serde_json::Value;
}

#[async_trait::async_trait]
impl DomainEngineTestExt for DomainEngine {
    async fn exec_named_map_json(
        self: &Arc<Self>,
        (domain_index, name): (DomainIndex, &str),
        input_json: serde_json::Value,
        find_query: TestFindQuery,
    ) -> serde_json::Value {
        let value = test_exec_named_map(self, (domain_index, name), input_json, find_query).await;

        let ontology = self.ontology();
        let key = ontology
            .find_named_downmap_meta(domain_index, name)
            .expect("Named map not found");

        let output_def = ontology.def(key.output.def_id);

        let mut json_buf: Vec<u8> = vec![];
        let mut serializer = serde_json::Serializer::new(&mut json_buf);

        let processor = match &value {
            Value::Sequence(..) => ontology.new_serde_processor(
                ontology.dynamic_sequence_operator_addr(),
                ProcessorMode::Raw,
            ),
            _ => {
                ontology.new_serde_processor(output_def.operator_addr.unwrap(), ProcessorMode::Raw)
            }
        };

        processor
            .serialize_attr(AttrRef::Unit(&value), &mut serializer)
            .expect("Serialize output failed");

        serde_json::from_slice(&json_buf).unwrap()
    }
}

async fn test_exec_named_map(
    engine: &Arc<DomainEngine>,
    (domain_index, name): (DomainIndex, &str),
    input_json: serde_json::Value,
    mut find_query: TestFindQuery,
) -> Value {
    let ontology = engine.ontology();
    let key = ontology
        .find_named_downmap_meta(domain_index, name)
        .expect("Named map not found");

    let input_def = ontology.def(key.input.def_id);

    let input_value = ontology
        .new_serde_processor(input_def.operator_addr.unwrap(), ProcessorMode::Raw)
        .deserialize(input_json)
        .expect("Deserialize input failed")
        .into_unit()
        .expect("input is not a unit attr");

    engine
        .exec_map(key, input_value, &mut find_query, Session::default())
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
    fn find_select(&mut self, _match_var: Var, condition: &Condition) -> SelectMode {
        let def_id = condition
            .root_def_id()
            .expect("Unable to detect an entity being queried");

        SelectMode::Dynamic(EntitySelect {
            source: StructOrUnionSelect::Struct(StructSelect {
                def_id,
                properties: Default::default(),
            }),
            filter: Filter::default_for_domain(),
            limit: self.limit,
            after_cursor: None,
            include_total_len: self.include_total_len,
        })
    }
}

pub mod system {
    use std::sync::{Arc, Mutex};

    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use domain_engine_core::system::SystemApiMock;
    use unimock::*;

    /// Mock [domain_engine_core::system::SystemApi::current_time] in a way that increases the year with every call.
    /// Starts at 1970 (January 1st, 00:00).
    pub fn mock_current_time_monotonic() -> impl unimock::Clause {
        let year_counter: Mutex<i32> = Mutex::new(1970);

        SystemApiMock::current_time
            .each_call(matching!())
            .answers_arc(Arc::new(move |_| {
                let year = {
                    let mut lock = year_counter.lock().unwrap();
                    let year = *lock;
                    *lock += 1;
                    year
                };

                DateTime::<Utc>::from_naive_utc_and_offset(
                    NaiveDateTime::new(
                        NaiveDate::from_ymd_opt(year, 1, 1).unwrap(),
                        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                    ),
                    Utc,
                )
            }))
    }
}
