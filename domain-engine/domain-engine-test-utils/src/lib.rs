#![forbid(unsafe_code)]

use std::sync::Arc;

use domain_engine_core::{domain_select::domain_select, DomainEngine, SelectMode, Session};
use ontol_runtime::{
    attr::AttrRef,
    interface::serde::processor::ProcessorMode,
    ontology::Ontology,
    query::{
        condition::Condition,
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect},
    },
    value::Value,
    var::Var,
    DomainIndex,
};
use serde::de::DeserializeSeed;

pub mod data_store_util;
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
    ontology: Arc<Ontology>,
    limit: usize,
    include_total_len: bool,
}

impl TestFindQuery {
    pub fn new(ontology: Arc<Ontology>) -> Self {
        Self {
            ontology,
            limit: 20,
            include_total_len: false,
        }
    }

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

impl domain_engine_core::FindEntitySelect for TestFindQuery {
    fn find_select(&mut self, _match_var: Var, condition: &Condition) -> SelectMode {
        let def_id = condition
            .root_def_id()
            .expect("Unable to detect an entity being queried");

        let struct_select = match domain_select(def_id, self.ontology.as_ref().as_ref()) {
            Select::Struct(struct_select) => struct_select,
            _ => panic!("must be struct select"),
        };

        SelectMode::Dynamic(EntitySelect {
            source: StructOrUnionSelect::Struct(struct_select),
            filter: Filter::default_for_domain(),
            limit: Some(self.limit),
            after_cursor: None,
            include_total_len: self.include_total_len,
        })
    }
}

pub async fn await_search_indexer_queue_empty(domain_engine: &DomainEngine) {
    while domain_engine
        .get_data_store()
        .unwrap()
        .api()
        .background_search_indexer_running()
    {
        let mut signal = domain_engine.index_mutated_signal();
        signal.mark_unchanged();
        signal.changed().await.unwrap();
    }
}

pub mod system {
    use std::sync::{Arc, Mutex};

    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
    use domain_engine_core::{
        system::{SystemAPI, SystemApiMock},
        DomainResult, Session,
    };
    use tracing::trace;
    use unimock::*;

    /// Mock [domain_engine_core::system::SystemApi::current_time] in a way that increases the year with every call.
    /// Starts at 1970 (January 1st, 00:00).
    pub fn mock_current_time_monotonic() -> impl unimock::Clause {
        let monotonic = MonotonicClockSystemApi::default();

        SystemApiMock::current_time
            .each_call(matching!())
            .answers_arc(Arc::new(move |_| monotonic.current_time()))
    }

    #[derive(Clone)]
    pub struct MonotonicClockSystemApi {
        year_counter: Arc<Mutex<i32>>,
    }

    impl Default for MonotonicClockSystemApi {
        fn default() -> Self {
            Self {
                year_counter: Arc::new(Mutex::new(1970)),
            }
        }
    }

    #[async_trait::async_trait]
    impl SystemAPI for MonotonicClockSystemApi {
        fn current_time(&self) -> chrono::DateTime<chrono::Utc> {
            let year = {
                let mut lock = self.year_counter.lock().unwrap();
                let year = *lock;
                *lock += 1;
                year
            };

            let dt = DateTime::<Utc>::from_naive_utc_and_offset(
                NaiveDateTime::new(
                    NaiveDate::from_ymd_opt(year, 1, 1).unwrap(),
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                ),
                Utc,
            );

            trace!(%dt, "monotonic year current time");

            dt
        }

        #[expect(unused)]
        fn get_user_id(&self, session: Session) -> DomainResult<String> {
            Ok("testuser".to_string())
        }

        #[expect(unused)]
        fn verify_session_user_id(&self, user_id: &str, session: Session) -> DomainResult<()> {
            Ok(())
        }

        async fn call_http_json_hook(
            &self,
            _url: &str,
            _session: Session,
            _input: Vec<u8>,
        ) -> DomainResult<Vec<u8>> {
            unimplemented!()
        }

        fn automerge_system_actor(&self) -> Vec<u8> {
            b"domainengine".to_vec()
        }
    }
}
