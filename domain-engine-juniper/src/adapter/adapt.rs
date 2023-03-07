use std::sync::Arc;

use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::Discriminant,
    env::{Env, TypeInfo},
    serde::{MapType, SerdeOperator, SerdeOperatorId},
    smart_format, PackageId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    adapter::data::{
        DomainData, EdgeData, EntityData, Field, FieldCardinality, FieldKind, MutationKind,
        TypeData,
    },
    SchemaBuildError,
};

use super::DomainAdapter;

pub fn adapt_domain(
    env: Arc<Env>,
    package_id: PackageId,
) -> Result<DomainAdapter, SchemaBuildError> {
    let domain = env
        .get_domain(&package_id)
        .ok_or(SchemaBuildError::UnknownPackage)?;

    let mut domain_data = DomainData {
        env: env.clone(),
        package_id,
        types: Default::default(),
        entities: Default::default(),
        root_edges: Default::default(),
        query_type_name: "Query".into(),
        mutation_type_name: "Mutation".into(),
        queries: Default::default(),
        mutations: Default::default(),
    };

    for (typename, type_info) in &domain.types {
        if let Some(operator_id) = type_info.serde_operator_id {
            register_type(&env, &mut domain_data, typename, type_info, operator_id);
        }
    }

    Ok(DomainAdapter {
        domain_data: Arc::new(domain_data),
    })
}

fn register_type(
    env: &Env,
    domain_data: &mut DomainData,
    typename: &String,
    type_info: &TypeInfo,
    operator_id: SerdeOperatorId,
) {
    match env.get_serde_operator(operator_id) {
        SerdeOperator::MapType(map_type) => {
            register_node_type(env, domain_data, typename, type_info, operator_id, map_type);
        }
        SerdeOperator::ValueUnionType(value_union_type) => {
            let mut found_id = None;
            let mut found_map_fallback = None;
            for variant in &value_union_type.variants {
                match &variant.discriminator.discriminant {
                    Discriminant::IsSingletonProperty(_, prop) if prop == "_id" => {
                        found_id = Some(variant);
                    }
                    Discriminant::MapFallback => {
                        found_map_fallback = Some(variant);
                    }
                    _ => {}
                }
            }

            match (found_id, found_map_fallback) {
                (Some(_id), Some(map_fallback)) => register_type(
                    env,
                    domain_data,
                    typename,
                    type_info,
                    map_fallback.operator_id,
                ),
                _ => {
                    panic!("Unprocessed union");
                }
            }
        }
        _other => {
            // panic!("other operator: {other:?}");
        }
    };
}

fn register_node_type(
    _env: &Env,
    domain_data: &mut DomainData,
    typename: &String,
    type_info: &TypeInfo,
    serde_operator_id: SerdeOperatorId,
    map_type: &MapType,
) {
    let env = domain_data.env.as_ref();

    debug!("register_map_type {type_info:?}");

    let mut fields: IndexMap<String, Field> = Default::default();

    for (property_name, property) in &map_type.properties {
        match env.get_serde_operator(property.value_operator_id) {
            SerdeOperator::ValueUnionType(_) => {
                // let object_def_id = value_union_type.union_def_variant.id();
                // let object_type_info = env.find_type_info(object_def_id);
            }
            SerdeOperator::MapType(_) => {
                fields.insert(
                    property_name.clone(),
                    Field {
                        cardinality: if property.optional {
                            FieldCardinality::UnitOptional
                        } else {
                            FieldCardinality::UnitMandatory
                        },
                        kind: FieldKind::Node {
                            value: property.value_operator_id,
                            rel: property.rel_params_operator_id,
                        },
                    },
                );
            }
            SerdeOperator::RelationSequence(_sequence_type) => {
                fields.insert(
                    property_name.clone(),
                    Field {
                        cardinality: if property.optional {
                            FieldCardinality::ManyOptional
                        } else {
                            FieldCardinality::ManyMandatory
                        },
                        kind: FieldKind::Node {
                            value: property.value_operator_id,
                            rel: property.rel_params_operator_id,
                        },
                    },
                );
            }
            _ => {
                fields.insert(
                    property_name.clone(),
                    Field {
                        cardinality: if property.optional {
                            FieldCardinality::UnitOptional
                        } else {
                            FieldCardinality::UnitMandatory
                        },
                        kind: FieldKind::Scalar(property.value_operator_id),
                    },
                );
            }
        }
    }

    if let Some(entity_id) = type_info.entity_id {
        let (_, id_type_info) = env
            .find_type_info(entity_id)
            .expect("Id definition not found");
        let id_operator_id = id_type_info.serde_operator_id.expect("No id_operator_id");

        let data = EntityData {
            def_id: type_info.def_id,
            id_operator_id,
            query_field_name: smart_format!("{typename}List"),
            create_mutation_field_name: smart_format!("create{typename}"),
            update_mutation_field_name: smart_format!("update{typename}"),
            delete_mutation_field_name: smart_format!("delete{typename}"),
        };

        domain_data
            .queries
            .insert(data.query_field_name.clone(), serde_operator_id);
        domain_data.mutations.insert(
            data.create_mutation_field_name.clone(),
            MutationKind::Create {
                input_operator_id: serde_operator_id,
            },
        );
        domain_data.mutations.insert(
            data.update_mutation_field_name.clone(),
            MutationKind::Update {
                id_operator_id,
                input_operator_id: serde_operator_id, // BUG: Partial input
            },
        );
        domain_data.mutations.insert(
            data.delete_mutation_field_name.clone(),
            MutationKind::Delete { id_operator_id },
        );

        domain_data.entities.insert(serde_operator_id, data);
        domain_data.root_edges.insert(
            serde_operator_id,
            EdgeData {
                edge_type_name: smart_format!("{typename}ConnectionEdge"),
                connection_type_name: smart_format!("{typename}Connection"),
            },
        );
    }

    domain_data.types.insert(
        serde_operator_id,
        TypeData {
            type_name: typename.clone(),
            input_type_name: smart_format!("{typename}Input"),
            operator_id: serde_operator_id,
            fields,
        },
    );
}
