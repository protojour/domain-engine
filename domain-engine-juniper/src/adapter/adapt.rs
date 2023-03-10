use std::sync::Arc;

use indexmap::IndexMap;
use ontol_runtime::{
    discriminator::Discriminant,
    env::{Env, TypeInfo},
    serde::{MapType, SerdeOperator, SerdeOperatorId},
    DefId, PackageId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    adapter::data::{
        DomainData, EntityData, Field, FieldCardinality, FieldKind, MutationData, MutationKind,
        TypeData, UnionData,
    },
    SchemaBuildError,
};

use super::{data::EdgeData, namespace::Namespace, DomainAdapter};

pub fn adapt_domain(
    env: Arc<Env>,
    package_id: PackageId,
) -> Result<DomainAdapter, SchemaBuildError> {
    let domain = env
        .find_domain(&package_id)
        .ok_or(SchemaBuildError::UnknownPackage)?;

    let mut namespace = Namespace::new();

    let mut domain_data = DomainData {
        env: env.clone(),
        package_id,
        types: Default::default(),
        entities: Default::default(),
        unions: Default::default(),
        edges: Default::default(),
        query_type_name: "Query".into(),
        mutation_type_name: "Mutation".into(),
        queries: Default::default(),
        mutations: Default::default(),
    };

    for (_, def_id) in &domain.type_names {
        let type_info = domain.type_info(*def_id);

        if let Some(operator_id) = type_info.graphql_operator_id {
            debug!("adapt type `{}` {:?}", type_info.name, operator_id);

            adapt_type(
                &env,
                &mut domain_data,
                &mut namespace,
                type_info,
                operator_id,
            );
        }
    }

    Ok(Arc::new(domain_data))
}

fn adapt_type(
    env: &Env,
    domain_data: &mut DomainData,
    namespace: &mut Namespace,
    type_info: &TypeInfo,
    operator_id: SerdeOperatorId,
) {
    match env.get_serde_operator(operator_id) {
        SerdeOperator::MapType(map_type) => {
            adapt_node_type(
                env,
                domain_data,
                namespace,
                type_info,
                operator_id,
                map_type,
            );
        }
        SerdeOperator::ValueUnionType(value_union_type) => {
            let mut union_variants = vec![];

            for variant in &value_union_type.variants {
                match classify_type(env, variant.operator_id) {
                    TypeClassification::Type(_, def_id, operator_id) => {
                        union_variants.push((def_id, operator_id));
                    }
                    TypeClassification::Id => {}
                    TypeClassification::Scalar => {
                        panic!("BUG: Scalar in union");
                    }
                }
            }

            debug!(
                "created a union for `{type_name}`: {operator_id:?} variants={union_variants:?}",
                type_name = type_info.name
            );

            domain_data.unions.insert(
                type_info.def_id,
                UnionData {
                    type_name: type_info.name.clone(),
                    def_id: value_union_type.union_def_variant.def_id,
                    operator_id,
                    variants: union_variants,
                },
            );
        }
        _other => {
            // panic!("other operator: {other:?}");
        }
    };
}

fn adapt_node_type(
    _env: &Env,
    domain_data: &mut DomainData,
    namespace: &mut Namespace,
    type_info: &TypeInfo,
    serde_operator_id: SerdeOperatorId,
    map_type: &MapType,
) {
    let env = domain_data.env.as_ref();
    let type_name = &type_info.name;

    // debug!("node type `{type_name}` {type_info:?}");

    let mut fields: IndexMap<String, Field> = Default::default();

    if let Some(entity_id) = type_info.entity_id {
        let id_type_info = env.get_type_info(entity_id);
        let id_operator_id = id_type_info.rest_operator_id.expect("No id_operator_id");

        fields.insert(
            "_id".into(),
            Field {
                cardinality: FieldCardinality::UnitMandatory,
                kind: FieldKind::Scalar(id_operator_id),
            },
        );

        domain_data
            .queries
            .insert(namespace.list(type_name), type_info.def_id);

        domain_data.mutations.insert(
            namespace.create(type_name),
            MutationData {
                entity: type_info.def_id,
                entity_operator_id: serde_operator_id,
                kind: MutationKind::Create {
                    input: type_info.def_id,
                },
            },
        );
        domain_data.mutations.insert(
            namespace.update(type_name),
            MutationData {
                entity: type_info.def_id,
                entity_operator_id: serde_operator_id,
                kind: MutationKind::Update {
                    id: id_operator_id,
                    input: type_info.def_id,
                },
            },
        );
        domain_data.mutations.insert(
            namespace.delete(type_name),
            MutationData {
                entity: type_info.def_id,
                entity_operator_id: serde_operator_id,
                kind: MutationKind::Delete { id: id_operator_id },
            },
        );
        domain_data.entities.insert(
            type_info.def_id,
            EntityData {
                def_id: type_info.def_id,
                id_operator_id,
            },
        );
        domain_data.edges.insert(
            (None, type_info.def_id),
            adapt_edge(
                env,
                namespace,
                EdgeInfo {
                    type_name,
                    property_name: None,
                    rel_params_operator_id: None,
                },
            ),
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
                            subject_id: type_info.def_id,
                            node_id: map_type.def_variant.def_id,
                            rel_id: property.rel_params_operator_id,
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
                    TypeClassification::Type(node_classification, node_id, _) => {
                        domain_data.edges.insert(
                            (Some(type_info.def_id), node_id),
                            adapt_edge(
                                env,
                                namespace,
                                EdgeInfo {
                                    type_name,
                                    property_name: Some(property_name),
                                    rel_params_operator_id: property.rel_params_operator_id,
                                },
                            ),
                        );
                        fields.insert(
                            property_name.clone(),
                            Field {
                                cardinality,
                                kind: match node_classification {
                                    NodeClassification::Node => FieldKind::Edge {
                                        subject_id: type_info.def_id,
                                        node_id,
                                        rel_id: property.rel_params_operator_id,
                                    },
                                    NodeClassification::Entity => FieldKind::EntityRelationship {
                                        subject_id: Some(type_info.def_id),
                                        node_id,
                                        rel_id: property.rel_params_operator_id,
                                    },
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
            _operator => {
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

    domain_data.types.insert(
        type_info.def_id,
        TypeData {
            type_name: type_name.clone(),
            def_id: type_info.def_id,
            input_type_name: namespace.input(type_name),
            operator_id: serde_operator_id,
            fields,
        },
    );
}

struct EdgeInfo<'a> {
    type_name: &'a str,
    property_name: Option<&'a str>,
    rel_params_operator_id: Option<SerdeOperatorId>,
}

fn adapt_edge(_env: &Env, namespace: &mut Namespace, edge_info: EdgeInfo) -> EdgeData {
    let fields: IndexMap<String, Field> = Default::default();

    if let Some(_rel_params_operator_id) = edge_info.rel_params_operator_id {}

    EdgeData {
        names: namespace.edge_names(edge_info.type_name, edge_info.property_name),
        fields,
    }
}

pub enum TypeClassification {
    Type(NodeClassification, DefId, SerdeOperatorId),
    Id,
    Scalar,
}

pub enum NodeClassification {
    Node,
    Entity,
}

pub fn classify_type(env: &Env, operator_id: SerdeOperatorId) -> TypeClassification {
    let operator = env.get_serde_operator(operator_id);
    // debug!("    classify operator: {operator:?}");

    match operator {
        SerdeOperator::MapType(map_type) => {
            let type_info = env.get_type_info(map_type.def_variant.def_id);
            let node_classification = if type_info.entity_id.is_some() {
                NodeClassification::Entity
            } else {
                NodeClassification::Node
            };
            TypeClassification::Type(
                node_classification,
                map_type.def_variant.def_id,
                operator_id,
            )
        }
        SerdeOperator::ValueUnionType(union_type) => {
            // start with the "highest" classification and downgrade as "lower" variants are found.
            let mut classification = TypeClassification::Type(
                NodeClassification::Entity,
                union_type.union_def_variant.def_id,
                operator_id,
            );

            for variant in &union_type.variants {
                if variant.discriminator.discriminant == Discriminant::MapFallback {
                    panic!("BUG: Don't want to see this in a GraphQL operator");
                }

                let variant_classification = classify_type(env, variant.operator_id);
                match (&classification, variant_classification) {
                    (
                        TypeClassification::Type(..),
                        TypeClassification::Type(NodeClassification::Node, ..),
                    ) => {
                        // downgrade
                        debug!("    Downgrade to Node");
                        classification = TypeClassification::Type(
                            NodeClassification::Node,
                            union_type.union_def_variant.def_id,
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
