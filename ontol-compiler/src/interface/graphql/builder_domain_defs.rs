use indexmap::IndexMap;
use ontol_runtime::{
    interface::serde::{SerdeDef, SerdeKey},
    interface::{
        graphql::{
            argument,
            data::{
                ConnectionData, EdgeData, FieldData, FieldKind, NativeScalarKind, NativeScalarRef,
                NodeData, ObjectData, ObjectKind, Optionality, PropertyData, ScalarData, TypeAddr,
                TypeData, TypeKind, TypeModifier, TypeRef, UnionData, UnitTypeRef,
            },
        },
        serde::{
            operator::{SerdeOperator, SerdeStructFlags},
            SerdeModifier,
        },
    },
    ontology::{PropertyCardinality, ValueCardinality},
    smart_format, DefId, Role,
};
use smartstring::alias::String;
use tracing::trace;

use crate::{
    def::{DefKind, LookupRelationshipMeta, RelParams},
    interface::serde::serde_generator::SerdeGenerator,
    relation::Properties,
    type_check::repr::repr_model::{ReprKind, ReprScalarKind},
};

use super::{builder::*, graphql_namespace::GraphqlNamespace};

impl<'a, 's, 'c, 'm> SchemaBuilder<'a, 's, 'c, 'm> {
    pub fn make_def_type(&mut self, def_id: DefId, level: QLevel) -> NewType {
        match level {
            QLevel::Node => self.make_node_type(def_id),
            QLevel::Edge { rel_params } => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let edge_addr = self.alloc_def_type_addr(def_id, level);
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);
                let node_type_addr = node_ref.unwrap_addr();

                let mut field_namespace = GraphqlNamespace::default();

                // FIXME: what if some of the relation data's fields are called "node"
                let fields: IndexMap<String, FieldData> = [(
                    field_namespace.unique_literal("node"),
                    FieldData {
                        kind: FieldKind::Node,
                        field_type: TypeRef::mandatory(node_ref),
                    },
                )]
                .into();

                let node_operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(type_info.def_id))
                    .unwrap();

                if let Some((rel_def_id, _operator_addr)) = rel_params {
                    let rel_type_info = self.partial_ontology.get_type_info(rel_def_id);
                    let rel_edge_ref = self.get_def_type_ref(rel_def_id, QLevel::Node);

                    let typename = self.type_namespace.edge(Some(rel_type_info), type_info);

                    self.lazy_tasks.push(LazyTask::HarvestFields {
                        type_addr: edge_addr,
                        def_id: rel_def_id,
                        property_field_producer: PropertyFieldProducer::EdgeProperty,
                    });

                    NewType::Addr(
                        edge_addr,
                        TypeData {
                            typename,
                            input_typename: Some(
                                self.type_namespace
                                    .edge_input(Some(rel_type_info), type_info),
                            ),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_type_addr,
                                    node_operator_addr,
                                    rel_edge_ref: Some(rel_edge_ref),
                                }),
                            }),
                        },
                    )
                } else {
                    NewType::Addr(
                        edge_addr,
                        TypeData {
                            typename: self.type_namespace.edge(None, type_info),
                            input_typename: Some(self.type_namespace.edge_input(None, type_info)),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_type_addr,
                                    node_operator_addr,
                                    rel_edge_ref: None,
                                }),
                            }),
                        },
                    )
                }
            }
            QLevel::Connection { rel_params } => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let connection_addr = self.alloc_def_type_addr(def_id, level);
                let edge_ref = self.get_def_type_ref(def_id, QLevel::Edge { rel_params });
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);
                let node_type_addr = node_ref.unwrap_addr();

                NewType::Addr(
                    connection_addr,
                    TypeData {
                        typename: self.type_namespace.connection(type_info),
                        input_typename: None,
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields: [
                                (
                                    smart_format!("nodes"),
                                    FieldData {
                                        kind: FieldKind::Nodes,
                                        field_type: TypeRef::mandatory(node_ref)
                                            .to_array(Optionality::Optional),
                                    },
                                ),
                                (
                                    smart_format!("edges"),
                                    FieldData {
                                        kind: FieldKind::Edges,
                                        field_type: TypeRef::mandatory(edge_ref)
                                            .to_array(Optionality::Optional),
                                    },
                                ),
                                (
                                    smart_format!("pageInfo"),
                                    FieldData {
                                        kind: FieldKind::PageInfo,
                                        field_type: TypeRef::mandatory(UnitTypeRef::Addr(
                                            self.schema.page_info,
                                        )),
                                    },
                                ),
                                (
                                    smart_format!("totalCount"),
                                    FieldData {
                                        kind: FieldKind::TotalCount,
                                        field_type: TypeRef {
                                            modifier: TypeModifier::Unit(Optionality::Mandatory),
                                            unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                                                operator_addr: self
                                                    .serde_generator
                                                    .gen_addr(SerdeKey::Def(SerdeDef::new(
                                                        self.primitives.i64,
                                                        SerdeModifier::NONE,
                                                    )))
                                                    .unwrap(),
                                                kind: NativeScalarKind::Int(self.primitives.i64),
                                            }),
                                        },
                                    },
                                ),
                            ]
                            .into(),
                            kind: ObjectKind::Connection(ConnectionData { node_type_addr }),
                        }),
                    },
                )
            }
            QLevel::MutationResult => {
                let addr = self.alloc_def_type_addr(def_id, level);
                let type_info = self.partial_ontology.get_type_info(def_id);
                let node_ref = self.get_def_type_ref(def_id, QLevel::Node);

                NewType::Addr(
                    addr,
                    TypeData {
                        typename: self.type_namespace.mutation_result(type_info),
                        input_typename: None,
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields: [
                                (
                                    smart_format!("node"),
                                    FieldData {
                                        kind: FieldKind::Node,
                                        field_type: TypeRef::_optional(node_ref),
                                    },
                                ),
                                (
                                    smart_format!("deleted"),
                                    FieldData {
                                        kind: FieldKind::Node,
                                        field_type: TypeRef {
                                            unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                                                operator_addr: self
                                                    .serde_generator
                                                    .gen_addr(gql_serde_key(self.primitives.bool))
                                                    .unwrap(),
                                                kind: NativeScalarKind::Boolean,
                                            }),
                                            modifier: TypeModifier::Unit(Optionality::Mandatory),
                                        },
                                    },
                                ),
                            ]
                            .into(),
                            kind: ObjectKind::MutationResult,
                        }),
                    },
                )
            }
        }
    }

    fn make_node_type(&mut self, def_id: DefId) -> NewType {
        if self.relations.properties_by_def_id(def_id).is_none() {
            return NewType::NativeScalar(NativeScalarRef {
                operator_addr: self
                    .serde_generator
                    .gen_addr(gql_serde_key(DefId::unit()))
                    .unwrap(),
                kind: NativeScalarKind::Unit,
            });
        }

        let repr_kind = self.seal_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");

        match repr_kind {
            ReprKind::Unit | ReprKind::Struct | ReprKind::StructIntersection(_) => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let type_addr = self.alloc_def_type_addr(def_id, QLevel::Node);

                let operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(def_id))
                    .unwrap();

                self.lazy_tasks.push(LazyTask::HarvestFields {
                    type_addr,
                    def_id: type_info.def_id,
                    property_field_producer: PropertyFieldProducer::Property,
                });

                let type_kind = TypeKind::Object(ObjectData {
                    fields: Default::default(),
                    kind: ObjectKind::Node(NodeData {
                        def_id: type_info.def_id,
                        entity_id: type_info
                            .entity_info
                            .as_ref()
                            .map(|entity_info| entity_info.id_value_def_id),
                        operator_addr,
                    }),
                });

                NewType::Addr(
                    type_addr,
                    TypeData {
                        typename: self.type_namespace.typename(type_info),
                        input_typename: Some(self.type_namespace.input(type_info)),
                        partial_input_typename: Some(self.type_namespace.partial_input(type_info)),
                        kind: type_kind,
                    },
                )
            }
            ReprKind::Union(variants) | ReprKind::StructUnion(variants) => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let node_index = self.alloc_def_type_addr(def_id, QLevel::Node);

                let mut needs_scalar = false;
                let mut type_variants = vec![];

                for (variant_def_id, _) in variants {
                    match self.seal_ctx.get_repr_kind(variant_def_id) {
                        Some(ReprKind::Scalar(..)) => {
                            needs_scalar = true;
                            break;
                        }
                        _ => match self.get_def_type_ref(*variant_def_id, QLevel::Node) {
                            UnitTypeRef::Addr(type_addr) => {
                                type_variants.push(type_addr);
                            }
                            UnitTypeRef::NativeScalar(_) => {
                                needs_scalar = true;
                                break;
                            }
                        },
                    }
                }

                let operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(def_id))
                    .unwrap();

                NewType::Addr(
                    node_index,
                    TypeData {
                        typename: self.type_namespace.typename(type_info),
                        input_typename: Some(self.type_namespace.union_input(type_info)),
                        partial_input_typename: Some(
                            self.type_namespace.union_partial_input(type_info),
                        ),
                        kind: if needs_scalar {
                            TypeKind::CustomScalar(ScalarData { operator_addr })
                        } else {
                            TypeKind::Union(UnionData {
                                union_def_id: type_info.def_id,
                                variants: type_variants,
                                operator_addr,
                            })
                        },
                    },
                )
            }
            ReprKind::Seq | ReprKind::Intersection(_) => {
                let type_info = self.partial_ontology.get_type_info(def_id);
                let operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(def_id))
                    .unwrap();
                let type_addr = self.alloc_def_type_addr(def_id, QLevel::Node);

                NewType::Addr(
                    type_addr,
                    TypeData {
                        typename: self.type_namespace.typename(type_info),
                        input_typename: Some(self.type_namespace.input(type_info)),
                        partial_input_typename: Some(self.type_namespace.partial_input(type_info)),
                        kind: TypeKind::CustomScalar(ScalarData { operator_addr }),
                    },
                )
            }
            ReprKind::Scalar(scalar_def_id, ReprScalarKind::I64(range), _) => {
                if range.start() >= &i32::MIN.into() && range.end() <= &i32::MAX.into() {
                    NewType::NativeScalar(NativeScalarRef {
                        operator_addr: self
                            .serde_generator
                            .gen_addr(gql_serde_key(def_id))
                            .unwrap(),
                        kind: NativeScalarKind::Int(*scalar_def_id),
                    })
                } else {
                    let type_info = self.partial_ontology.get_type_info(def_id);
                    let type_addr = self.alloc_def_type_addr(type_info.def_id, QLevel::Node);
                    self.schema.i64_custom_scalar = Some(type_addr);
                    NewType::Addr(
                        type_addr,
                        TypeData {
                            // FIXME: Must make sure that domain typenames take precedence over generated ones
                            typename: smart_format!("_ontol_i64"),
                            input_typename: None,
                            partial_input_typename: None,
                            kind: TypeKind::CustomScalar(ScalarData {
                                operator_addr: self
                                    .serde_generator
                                    .gen_addr(gql_serde_key(def_id))
                                    .unwrap(),
                            }),
                        },
                    )
                }
            }
            ReprKind::Scalar(def_id, ReprScalarKind::F64(_), _) => {
                NewType::NativeScalar(NativeScalarRef {
                    operator_addr: self
                        .serde_generator
                        .gen_addr(gql_serde_key(*def_id))
                        .unwrap(),
                    kind: NativeScalarKind::Number(*def_id),
                })
            }
            ReprKind::Scalar(..) => {
                let operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(def_id))
                    .unwrap();

                NewType::NativeScalar(NativeScalarRef {
                    operator_addr,
                    kind: get_native_scalar_kind(
                        self.serde_generator,
                        self.serde_generator.get_operator(operator_addr),
                    ),
                })
            }
        }
    }

    pub fn harvest_fields(
        &mut self,
        type_addr: TypeAddr,
        def_id: DefId,
        property_field_producer: PropertyFieldProducer,
    ) {
        trace!("Harvest fields for {def_id:?} / {type_addr:?}");

        let mut struct_flags = SerdeStructFlags::empty();
        let mut field_namespace = GraphqlNamespace::default();
        let mut fields = Default::default();

        let repr_kind = self.seal_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");
        if let ReprKind::StructIntersection(members) = repr_kind {
            for (member_def_id, _) in members {
                let Some(properties) = self.relations.properties_by_def_id(*member_def_id) else {
                    continue;
                };

                self.harvest_struct_fields(
                    def_id,
                    properties,
                    property_field_producer,
                    &mut struct_flags,
                    &mut field_namespace,
                    &mut fields,
                );
            }
        } else {
            let Some(properties) = self.relations.properties_by_def_id(def_id) else {
                return;
            };

            self.harvest_struct_fields(
                def_id,
                properties,
                property_field_producer,
                &mut struct_flags,
                &mut field_namespace,
                &mut fields,
            );
        }

        if struct_flags.contains(SerdeStructFlags::OPEN_DATA) {
            fields.insert(
                "_open_data".into(),
                FieldData {
                    kind: FieldKind::OpenData,
                    field_type: TypeRef {
                        modifier: TypeModifier::Unit(Optionality::Optional),
                        unit: UnitTypeRef::Addr(self.schema.json_scalar),
                    },
                },
            );
        }

        match &mut self.schema.types[type_addr.0 as usize].kind {
            TypeKind::Object(object_data) => {
                object_data.fields.extend(fields);
            }
            _ => panic!(),
        }
    }

    fn harvest_struct_fields(
        &mut self,
        def_id: DefId,
        properties: &Properties,
        property_field_producer: PropertyFieldProducer,
        struct_flags: &mut SerdeStructFlags,
        field_namespace: &mut GraphqlNamespace,
        output: &mut IndexMap<String, FieldData>,
    ) {
        let Some(table) = &properties.table else {
            return;
        };

        if let DefKind::Type(type_def) = self.defs.def_kind(def_id) {
            if type_def.open {
                *struct_flags |= SerdeStructFlags::OPEN_DATA;
            }
        }

        for (property_id, property) in table {
            let property_id = *property_id;
            let meta = self.defs.relationship_meta(property_id.relationship_id);
            let (_, (prop_cardinality, value_cardinality), _) =
                meta.relationship.by(property_id.role);
            let (value_def_id, ..) = meta.relationship.by(property_id.role.opposite());
            let prop_key = match property_id.role {
                Role::Subject => {
                    let DefKind::TextLiteral(prop_key) = meta.relation_def_kind.value else {
                        panic!("Subject property is not a string literal");
                    };
                    *prop_key
                }
                Role::Object => meta
                    .relationship
                    .object_prop
                    .expect("Object property has no name"),
            };

            trace!("    register struct field `{prop_key}`: {property_id}");

            let value_properties = self.relations.properties_by_def_id(value_def_id);

            let is_entity_value = {
                let repr_kind = self.seal_ctx.get_repr_kind(&value_def_id);
                match repr_kind {
                    Some(ReprKind::StructUnion(variants)) => {
                        variants.iter().all(|(variant_def_id, _)| {
                            let variant_properties =
                                self.relations.properties_by_def_id(*variant_def_id);
                            variant_properties
                                .map(|properties| properties.identified_by.is_some())
                                .unwrap_or(false)
                        })
                    }
                    _ => value_properties
                        .map(|properties| properties.identified_by.is_some())
                        .unwrap_or(false),
                }
            };

            let field_data = if matches!(value_cardinality, ValueCardinality::One) {
                let modifier = TypeModifier::new_unit(Optionality::from_optional(matches!(
                    prop_cardinality,
                    PropertyCardinality::Optional
                )));

                let value_operator_addr = self
                    .serde_generator
                    .gen_addr(gql_serde_key(value_def_id))
                    .unwrap();

                let field_type = if property.is_entity_id {
                    TypeRef {
                        modifier,
                        unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                            operator_addr: value_operator_addr,
                            kind: NativeScalarKind::ID,
                        }),
                    }
                } else {
                    let qlevel = match meta.relationship.rel_params {
                        RelParams::Unit => QLevel::Node,
                        RelParams::Type(rel_def_id) => {
                            let operator_addr = self
                                .serde_generator
                                .gen_addr(gql_serde_key(rel_def_id))
                                .unwrap();

                            QLevel::Edge {
                                rel_params: Some((rel_def_id, operator_addr)),
                            }
                        }
                        RelParams::IndexRange(_) => todo!(),
                    };
                    TypeRef {
                        modifier,
                        unit: self.get_def_type_ref(value_def_id, qlevel),
                    }
                };

                FieldData {
                    kind: property_field_producer.make_property(PropertyData {
                        property_id,
                        value_operator_addr,
                    }),
                    field_type,
                }
            } else if is_entity_value {
                let connection_ref = {
                    let rel_params = match meta.relationship.rel_params {
                        RelParams::Unit => None,
                        RelParams::Type(rel_def_id) => Some((
                            rel_def_id,
                            self.serde_generator
                                .gen_addr(gql_serde_key(rel_def_id))
                                .unwrap(),
                        )),
                        RelParams::IndexRange(_) => todo!(),
                    };

                    trace!("    connection/edge rel params {rel_params:?}");

                    self.get_def_type_ref(value_def_id, QLevel::Connection { rel_params })
                };

                trace!("Connection `{prop_key}` of prop {property_id:?}");

                FieldData::mandatory(
                    FieldKind::ConnectionProperty {
                        property_id,
                        first_arg: argument::FirstArg,
                        after_arg: argument::AfterArg,
                    },
                    connection_ref,
                )
            } else if let RelParams::Type(_) = meta.relationship.rel_params {
                todo!("Edge list with rel params");
            } else {
                let mut unit = self.get_def_type_ref(value_def_id, QLevel::Node);

                let value_operator_addr = self
                    .serde_generator
                    .gen_addr(gql_array_serde_key(value_def_id))
                    .unwrap();

                trace!("Array value operator addr: {value_operator_addr:?}");

                if let UnitTypeRef::NativeScalar(native) = &mut unit {
                    native.operator_addr = value_operator_addr;
                }

                FieldData {
                    kind: property_field_producer.make_property(PropertyData {
                        property_id,
                        value_operator_addr,
                    }),
                    field_type: TypeRef::mandatory(unit).to_array(Optionality::from_optional(
                        matches!(prop_cardinality, PropertyCardinality::Optional),
                    )),
                }
            };

            output.insert(field_namespace.unique_literal(prop_key), field_data);
        }
    }
}

pub(super) fn get_native_scalar_kind(
    serde_generator: &SerdeGenerator,
    serde_operator: &SerdeOperator,
) -> NativeScalarKind {
    match serde_operator {
        SerdeOperator::Unit => NativeScalarKind::Unit,
        SerdeOperator::False(_) | SerdeOperator::True(_) | SerdeOperator::Boolean(_) => {
            NativeScalarKind::Boolean
        }
        SerdeOperator::I64(..) => panic!("Must be a custom scalar"),
        SerdeOperator::I32(def_id, _) => NativeScalarKind::Int(*def_id),
        SerdeOperator::F64(def_id, _) => NativeScalarKind::Number(*def_id),
        SerdeOperator::String(_)
        | SerdeOperator::StringConstant(..)
        | SerdeOperator::TextPattern(_)
        | SerdeOperator::CapturingTextPattern(_) => NativeScalarKind::String,
        SerdeOperator::IdSingletonStruct(..) => panic!("Id should not appear in GraphQL"),
        SerdeOperator::Alias(alias_op) => get_native_scalar_kind(
            serde_generator,
            serde_generator.get_operator(alias_op.inner_addr),
        ),
        op @ (SerdeOperator::Union(_)
        | SerdeOperator::Struct(_)
        | SerdeOperator::DynamicSequence
        | SerdeOperator::RelationSequence(_)
        | SerdeOperator::ConstructorSequence(_)) => panic!("not a native scalar: {op:?}"),
    }
}
