use domain_engine_core::DomainEngine;
use fnv::FnvHashMap;
use ontol_runtime::{interface::serde::processor::ProcessorMode, value::Data, PackageId};
use serde::de::DeserializeSeed;

pub mod graphql;
pub mod parser_document_utils;

pub async fn exec_named_map_json(
    (package_id, name): (PackageId, &str),
    input_json: serde_json::Value,
    domain_engine: &DomainEngine,
) -> serde_json::Value {
    let ontology = domain_engine.ontology();
    let [from, to] = ontology
        .get_named_forward_map_meta(package_id, name)
        .expect("Named map not found");

    let input_type_info = ontology.get_type_info(from.def_id);
    let output_type_info = ontology.get_type_info(to.def_id);

    let input = ontology
        .new_serde_processor(input_type_info.operator_addr.unwrap(), ProcessorMode::Raw)
        .deserialize(input_json)
        .expect("Deserialize input failed");

    let output = domain_engine
        .exec_map([from, to], input, FnvHashMap::default())
        .await
        .expect("Exec map failed");

    let mut json_buf: Vec<u8> = vec![];
    let mut serializer = serde_json::Serializer::new(&mut json_buf);

    let processor = match &output.value.data {
        Data::Sequence(_) => ontology.new_serde_processor(
            ontology.dynamic_sequence_operator_addr(),
            ProcessorMode::Raw,
        ),
        _ => ontology
            .new_serde_processor(output_type_info.operator_addr.unwrap(), ProcessorMode::Raw),
    };

    processor
        .serialize_value(&output.value, None, &mut serializer)
        .expect("Serialize output failed");

    serde_json::from_slice(&json_buf).unwrap()
}
