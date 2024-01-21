use indexmap::IndexMap;
use ontol_runtime::{
    interface::serde::SerdeDef,
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
    smart_format,
    value::PropertyId,
    DefId, Role,
};
use smartstring::alias::String;
use tracing::{trace, trace_span};

use crate::{
    def::{DefKind, LookupRelationshipMeta, RelParams},
    interface::serde::{serde_generator::SerdeGenerator, SerdeKey},
    relation::Property,
    repr::repr_model::{ReprKind, ReprScalarKind},
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
                    .gen_addr_lazy(gql_serde_key(type_info.def_id))
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

                let rel_type_info = rel_params.map(|(rel_params_def_id, _)| {
                    self.partial_ontology.get_type_info(rel_params_def_id)
                });

                NewType::Addr(
                    connection_addr,
                    TypeData {
                        typename: self.type_namespace.connection(rel_type_info, type_info),
                        input_typename: Some(
                            self.type_namespace
                                .patch_edges_input(rel_type_info, type_info),
                        ),
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
                                                    .gen_addr_lazy(SerdeKey::Def(SerdeDef::new(
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
                                        field_type: TypeRef::optional(node_ref),
                                    },
                                ),
                                (
                                    smart_format!("deleted"),
                                    FieldData {
                                        kind: FieldKind::Deleted,
                                        field_type: TypeRef {
                                            unit: UnitTypeRef::NativeScalar(NativeScalarRef {
                                                operator_addr: self
                                                    .serde_generator
                                                    .gen_addr_lazy(gql_serde_key(
                                                        self.primitives.bool,
                                                    ))
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
                    .gen_addr_lazy(gql_serde_key(DefId::unit()))
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
                    .gen_addr_lazy(gql_serde_key(def_id))
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
                    .gen_addr_lazy(gql_serde_key(def_id))
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
                    .gen_addr_lazy(gql_serde_key(def_id))
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
                            .gen_addr_lazy(gql_serde_key(def_id))
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
                                    .gen_addr_lazy(gql_serde_key(def_id))
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
                        .gen_addr_lazy(gql_serde_key(*def_id))
                        .unwrap(),
                    kind: NativeScalarKind::Number(*def_id),
                })
            }
            ReprKind::Scalar(..) => {
                let operator_addr = self
                    .serde_generator
                    .gen_addr_lazy(gql_serde_key(def_id))
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
        let _entered = trace_span!("harvest", id = ?def_id).entered();
        // trace!("Harvest fields for {def_id:?} / {type_addr:?}");

        let mut struct_flags = SerdeStructFlags::empty();
        let mut field_namespace = GraphqlNamespace::default();
        let mut fields = vec![];

        if let DefKind::Type(type_def) = self.defs.def_kind(def_id) {
            if type_def.open {
                struct_flags |= SerdeStructFlags::OPEN_DATA;
            }
        }

        let repr_kind = self.seal_ctx.get_repr_kind(&def_id).expect("NO REPR KIND");

        if let Some(properties) = self.relations.properties_by_def_id(def_id) {
            if let Some(table) = &properties.table {
                for (property_id, property) in table {
                    self.harvest_struct_field(
                        *property_id,
                        property,
                        property_field_producer,
                        &mut field_namespace,
                        &mut fields,
                    );
                }
            }
        };

        if let ReprKind::StructIntersection(members) = repr_kind {
            for (member_def_id, _) in members {
                let Some(properties) = self.relations.properties_by_def_id(*member_def_id) else {
                    continue;
                };

                if let Some(table) = &properties.table {
                    for (property_id, property) in table {
                        self.harvest_struct_field(
                            *property_id,
                            property,
                            property_field_producer,
                            &mut field_namespace,
                            &mut fields,
                        );
                    }
                }
            }
        }

        if let Some(union_memberships) = self.union_member_cache.cache.get(&def_id) {
            for union_def_id in union_memberships {
                let Some(properties) = self.relations.properties_by_def_id(*union_def_id) else {
                    continue;
                };
                let Some(table) = &properties.table else {
                    continue;
                };

                for (property_id, property) in table {
                    let meta = self.defs.relationship_meta(property_id.relationship_id);

                    if meta.relationship.object.0 == *union_def_id {
                        self.harvest_struct_field(
                            *property_id,
                            property,
                            property_field_producer,
                            &mut field_namespace,
                            &mut fields,
                        );
                    }
                }
            }
        }

        if struct_flags.contains(SerdeStructFlags::OPEN_DATA) {
            fields.push((
                "_open_data".into(),
                FieldData {
                    kind: FieldKind::OpenData,
                    field_type: TypeRef {
                        modifier: TypeModifier::Unit(Optionality::Optional),
                        unit: UnitTypeRef::Addr(self.schema.json_scalar),
                    },
                },
            ));
        }

        if fields.is_empty() {
            return;
        }

        // Get the general category of each field, for deciding the overall order.
        fn field_order_category(field_kind: &FieldKind) -> u8 {
            match field_kind {
                FieldKind::Property(data) | FieldKind::EdgeProperty(data) => {
                    match data.property_id.role {
                        Role::Subject => 0,
                        Role::Object => 1,
                    }
                }
                // Connections are placed after "plain" fields
                FieldKind::ConnectionProperty { property_id, .. } => match property_id.role {
                    Role::Subject => 2,
                    Role::Object => 3,
                },
                _ => 4,
            }
        }

        // note: The sort is stable.
        fields.sort_by(|(_, a), (_, b)| {
            field_order_category(&a.kind).cmp(&field_order_category(&b.kind))
        });

        match &mut self.schema.types[type_addr.0 as usize].kind {
            TypeKind::Object(object_data) => {
                object_data.fields.extend(fields);
            }
            _ => panic!(),
        }
    }

    fn harvest_struct_field(
        &mut self,
        property_id: PropertyId,
        property: &Property,
        property_field_producer: PropertyFieldProducer,
        field_namespace: &mut GraphqlNamespace,
        output: &mut Vec<(String, FieldData)>,
    ) {
        let meta = self.defs.relationship_meta(property_id.relationship_id);
        let (_, (prop_cardinality, value_cardinality), _) = meta.relationship.by(property_id.role);
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
                .gen_addr_lazy(gql_serde_key(value_def_id))
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
                            .gen_addr_lazy(gql_serde_key(rel_def_id))
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
                            .gen_addr_lazy(gql_serde_key(rel_def_id))
                            .unwrap(),
                    )),
                    RelParams::IndexRange(_) => todo!(),
                };

                self.get_def_type_ref(value_def_id, QLevel::Connection { rel_params })
            };

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
                .gen_addr_lazy(gql_array_serde_key(value_def_id))
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

        output.push((field_namespace.unique_literal(prop_key), field_data));
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
