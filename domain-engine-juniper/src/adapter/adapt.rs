use std::sync::Arc;

use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::Discriminant,
    env::{Env, TypeInfo},
    serde::{MapType, SerdeOperator, SerdeOperatorId},
    smart_format, DefId, PackageId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    adapter::data::{
        DomainData, EdgeData, EntityData, Field, FieldCardinality, FieldKind, MutationData,
        MutationKind, TypeData, UnionData,
    },
    SchemaBuildError,
};

use super::DomainAdapter;

pub fn adapt_domain(
    env: Arc<Env>,
    package_id: PackageId,
) -> Result<DomainAdapter, SchemaBuildError> {
    let domain = env
        .find_domain(&package_id)
        .ok_or(SchemaBuildError::UnknownPackage)?;

    let mut domain_data = DomainData {
        env: env.clone(),
        package_id,
        types: Default::default(),
        entities: Default::default(),
        unions: Default::default(),
        edges: Default::default(),
        types_by_def: Default::default(),
        entities_by_def: Default::default(),
        unions_by_def: Default::default(),
        edges_by_def: Default::default(),
        query_type_name: "Query".into(),
        mutation_type_name: "Mutation".into(),
        queries: Default::default(),
        mutations: Default::default(),
    };

    for (_, def_id) in &domain.type_names {
        let type_info = domain.type_info(*def_id);

        if let Some(operator_id) = type_info.serde_operator_id {
            debug!("type `{}`", type_info.name);

            adapt_type(&env, &mut domain_data, type_info, operator_id);
        }
    }

    Ok(Arc::new(domain_data))
}

fn adapt_type(
    env: &Env,
    domain_data: &mut DomainData,
    type_info: &TypeInfo,
    operator_id: SerdeOperatorId,
) {
    match env.get_serde_operator(operator_id) {
        SerdeOperator::MapType(map_type) => {
            adapt_node_type(env, domain_data, type_info, operator_id, map_type);
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
                (Some(_id), Some(map_fallback)) => {
                    debug!("found _id and map fallback");
                    adapt_type(env, domain_data, type_info, map_fallback.operator_id)
                }
                _ => {
                    let mut union_variants = vec![];

                    for variant in &value_union_type.variants {
                        match classify_type(env, variant.operator_id) {
                            TypeClassification::Entity(def_id, operator_id)
                            | TypeClassification::Node(def_id, operator_id) => {
                                union_variants.push((def_id, operator_id));
                            }
                            TypeClassification::Id => {}
                            TypeClassification::Scalar => {
                                panic!("BUG: Scalar in union");
                            }
                        }
                    }

                    debug!("created a union for `{type_name}`: {operator_id:?} variants={union_variants:?}", type_name = type_info.name);

                    domain_data.unions.insert(
                        operator_id,
                        UnionData {
                            type_name: type_info.name.clone(),
                            def_id: value_union_type.union_def_variant.id(),
                            operator_id,
                            variants: union_variants,
                        },
                    );
                }
            }
        }
        _other => {
            // panic!("other operator: {other:?}");
        }
    };
}

fn adapt_node_type(
    _env: &Env,
    domain_data: &mut DomainData,
    type_info: &TypeInfo,
    serde_operator_id: SerdeOperatorId,
    map_type: &MapType,
) {
    let env = domain_data.env.as_ref();
    let type_name = &type_info.name;

    debug!("node type `{type_name}` {type_info:?}");

    let mut fields: IndexMap<String, Field> = Default::default();

    if let Some(entity_id) = type_info.entity_id {
        let id_type_info = env.get_type_info(entity_id);
        let id_operator_id = id_type_info.serde_operator_id.expect("No id_operator_id");

        fields.insert(
            "_id".into(),
            Field {
                cardinality: FieldCardinality::UnitMandatory,
                kind: FieldKind::Scalar(id_type_info.def_id, id_operator_id),
            },
        );

        domain_data
            .queries
            .insert(smart_format!("{type_name}List"), type_info.def_id);

        domain_data.mutations.insert(
            smart_format!("create{type_name}"),
            MutationData {
                entity: type_info.def_id,
                entity_operator_id: serde_operator_id,
                kind: MutationKind::Create {
                    input: type_info.def_id,
                },
            },
        );
        domain_data.mutations.insert(
            smart_format!("update{type_name}"),
            MutationData {
                entity: type_info.def_id,
                entity_operator_id: serde_operator_id,
                kind: MutationKind::Update {
                    id: id_type_info.def_id,
                    input: type_info.def_id,
                },
            },
        );
        domain_data.mutations.insert(
            smart_format!("delete{type_name}"),
            MutationData {
                entity: type_info.def_id,
                entity_operator_id: serde_operator_id,
                kind: MutationKind::Delete {
                    id: id_type_info.def_id,
                },
            },
        );

        domain_data.entities.insert(
            serde_operator_id,
            EntityData {
                def_id: type_info.def_id,
                id_operator_id,
            },
        );
        domain_data.entities_by_def.insert(
            type_info.def_id,
            EntityData {
                def_id: type_info.def_id,
                id_operator_id,
            },
        );
        domain_data.edges.insert(
            (None, serde_operator_id),
            EdgeData {
                edge_type_name: smart_format!("{type_name}ConnectionEdge"),
                connection_type_name: smart_format!("{type_name}Connection"),
            },
        );
        domain_data.edges_by_def.insert(
            (None, type_info.def_id),
            EdgeData {
                edge_type_name: smart_format!("{type_name}ConnectionEdge"),
                connection_type_name: smart_format!("{type_name}Connection"),
            },
        );
    }

    for (property_name, property) in &map_type.properties {
        debug!("  adapting property {property_name}");

        match env.get_serde_operator(property.value_operator_id) {
            SerdeOperator::ValueUnionType(_) => {
                // let object_def_id = value_union_type.union_def_variant.id();
                // let object_type_info = env.find_type_info(object_def_id);
            }
            SerdeOperator::MapType(map_type) => {
                let cardinality = if property.optional {
                    FieldCardinality::UnitOptional
                } else {
                    FieldCardinality::UnitMandatory
                };

                fields.insert(
                    property_name.clone(),
                    Field {
                        cardinality,
                        kind: FieldKind::Edge {
                            node: map_type.def_variant.id(),
                            node_operator: property.value_operator_id,
                            rel: property.rel_params_operator_id,
                        },
                    },
                );
            }
            SerdeOperator::RelationSequence(sequence_type) => {
                let cardinality = if property.optional {
                    FieldCardinality::ManyOptional
                } else {
                    FieldCardinality::ManyMandatory
                };

                match classify_type(env, sequence_type.ranges[0].operator_id) {
                    TypeClassification::Entity(entity_def_id, entity_operator_id) => {
                        domain_data.edges.insert(
                            (Some(type_info.def_id), entity_operator_id),
                            EdgeData {
                                edge_type_name: smart_format!(
                                    "{type_name}{property_name}ConnectionEdge"
                                ),
                                connection_type_name: smart_format!(
                                    "{type_name}{property_name}Connection"
                                ),
                            },
                        );
                        fields.insert(
                            property_name.clone(),
                            Field {
                                cardinality,
                                kind: FieldKind::EntityRelationship {
                                    subject: type_info.def_id,
                                    node: entity_def_id,
                                    node_operator: entity_operator_id,
                                    rel: property.rel_params_operator_id,
                                },
                            },
                        );
                    }
                    TypeClassification::Node(node_def_id, node_operator_id) => {
                        fields.insert(
                            property_name.clone(),
                            Field {
                                cardinality,
                                kind: FieldKind::Edge {
                                    node: node_def_id,
                                    node_operator: node_operator_id,
                                    rel: property.rel_params_operator_id,
                                },
                            },
                        );
                    }
                    TypeClassification::Id => {
                        debug!("Id not handled here");
                    }
                    TypeClassification::Scalar => {
                        panic!(
                            "Unhandled scalar: {:?}",
                            env.get_serde_operator(sequence_type.ranges[0].operator_id)
                        )
                    }
                }
            }
            operator => {
                fields.insert(
                    property_name.clone(),
                    Field {
                        cardinality: if property.optional {
                            FieldCardinality::UnitOptional
                        } else {
                            FieldCardinality::UnitMandatory
                        },
                        kind: FieldKind::Scalar(operator.type_def_id(), property.value_operator_id),
                    },
                );
            }
        }
    }

    domain_data.types.insert(
        serde_operator_id,
        TypeData {
            type_name: type_name.clone(),
            def_id: type_info.def_id,
            input_type_name: smart_format!("{type_name}Input"),
            operator_id: serde_operator_id,
            fields: fields.clone(),
        },
    );

    domain_data.types_by_def.insert(
        type_info.def_id,
        TypeData {
            type_name: type_name.clone(),
            def_id: type_info.def_id,
            input_type_name: smart_format!("{type_name}Input"),
            operator_id: serde_operator_id,
            fields,
        },
    );
}

enum TypeClassification {
    Entity(DefId, SerdeOperatorId),
    Node(DefId, SerdeOperatorId),
    Id,
    Scalar,
}

fn classify_type(env: &Env, operator_id: SerdeOperatorId) -> TypeClassification {
    let operator = env.get_serde_operator(operator_id);
    // debug!("    classify operator: {operator:?}");

    match operator {
        SerdeOperator::MapType(map_type) => {
            let type_info = env.get_type_info(map_type.def_variant.id());
            if type_info.entity_id.is_some() {
                TypeClassification::Entity(map_type.def_variant.id(), operator_id)
            } else {
                TypeClassification::Node(map_type.def_variant.id(), operator_id)
            }
        }
        SerdeOperator::ValueUnionType(union_type) => {
            for variant in &union_type.variants {
                if variant.discriminator.discriminant == Discriminant::MapFallback {
                    return classify_type(env, variant.operator_id);
                }
            }

            debug!("   no MapFallback");

            // start with the "highest" classification and downgrade as "lower" variants are found.
            let mut classification =
                TypeClassification::Entity(union_type.union_def_variant.id(), operator_id);

            for variant in &union_type.variants {
                let variant_classification = classify_type(env, variant.operator_id);
                match (&classification, variant_classification) {
                    (TypeClassification::Entity(..), TypeClassification::Node(..)) => {
                        // downgrade
                        debug!("    Downgrade to Node");
                        classification = TypeClassification::Node(
                            union_type.union_def_variant.id(),
                            operator_id,
                        );
                    }
                    (_, TypeClassification::Scalar) => {
                        // downgrade
                        debug!("    Downgrade to Scalar");
                        classification = TypeClassification::Scalar;
                    }
                    _ => {}
                }
            }

            classification
        }
        SerdeOperator::Id(_) => TypeClassification::Id,
        operator => {
            debug!("    operator interpreted as Scalar: {operator:?}");
            TypeClassification::Scalar
        }
    }
}
