use indexmap::IndexMap;
use ontol_runtime::{
    env::{Env, TypeInfo},
    serde::{
        operator::{
            FilteredVariants, MapOperator, SerdeOperator, SerdeOperatorId, SerdeProperty,
            ValueOperator,
        },
        processor::{ProcessorLevel, ProcessorMode},
    },
    smart_format, DefId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::virtual_schema::schema::{classify_type, TypeClassification};

use super::{
    argument,
    data::{
        EdgeData, EntityData, FieldData, FieldKind, IdPropertyData, NativeScalarKind,
        NativeScalarRef, NodeData, ObjectData, ObjectKind, Optionality, PropertyData, ScalarData,
        TypeData, TypeIndex, TypeKind, TypeModifier, TypeRef, UnionData, UnitTypeRef,
    },
    namespace::Namespace,
    schema::NodeClassification,
    QueryLevel, TypingPurpose, VirtualSchema,
};

enum NewType {
    Indexed(TypeIndex, TypeData),
    NativeScalar(NativeScalarRef),
}

pub(super) struct VirtualSchemaBuilder<'a> {
    pub env: &'a Env,
    pub schema: &'a mut VirtualSchema,
    pub namespace: &'a mut Namespace,
}

impl<'a> VirtualSchemaBuilder<'a> {
    pub fn register_query(&mut self) {
        let index = self.next_type_index();
        self.schema.types.push(TypeData {
            typename: "Query".into(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Query,
            }),
        });
        self.schema.query = index;
    }

    pub fn register_mutation(&mut self) {
        let index = self.next_type_index();
        self.schema.types.push(TypeData {
            typename: "Mutation".into(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Mutation,
            }),
        });
        self.schema.mutation = index;
    }

    pub fn get_def_type_ref(&mut self, def_id: DefId, level: QueryLevel) -> UnitTypeRef {
        if let Some(type_index) = self.schema.type_index_by_def.get(&(def_id, level)) {
            return UnitTypeRef::Indexed(*type_index);
        }

        match self.make_def_type(def_id, level) {
            NewType::Indexed(type_index, type_data) => {
                self.schema.types[type_index.0 as usize] = type_data;
                UnitTypeRef::Indexed(type_index)
            }
            NewType::NativeScalar(scalar_ref) => UnitTypeRef::NativeScalar(scalar_ref),
        }
    }

    fn next_type_index(&self) -> TypeIndex {
        TypeIndex(self.schema.types.len() as u32)
    }

    fn alloc_def_type_index(&mut self, def_id: DefId, level: QueryLevel) -> TypeIndex {
        let index = self.next_type_index();
        // note: this will be overwritten later
        self.schema.types.push(TypeData {
            typename: String::new(),
            input_typename: None,
            partial_input_typename: None,
            kind: TypeKind::CustomScalar(ScalarData {
                serde_operator_id: SerdeOperatorId(0),
            }),
        });
        self.schema.type_index_by_def.insert((def_id, level), index);
        index
    }

    fn make_def_type(&mut self, def_id: DefId, level: QueryLevel) -> NewType {
        let type_info = self.env.get_type_info(def_id);

        match level {
            QueryLevel::Node => self.make_node_type(type_info),
            QueryLevel::Edge { rel_params } => {
                let edge_index = self.alloc_def_type_index(def_id, level);
                let node_ref = self.get_def_type_ref(def_id, QueryLevel::Node);

                let mut field_namespace = Namespace::new();

                // FIXME: what if some of the relation data's fields are called "node"
                let mut fields: IndexMap<String, FieldData> = [(
                    field_namespace.unique_literal("node"),
                    FieldData {
                        kind: FieldKind::Node,
                        field_type: TypeRef::mandatory(node_ref),
                    },
                )]
                .into();

                if let Some(rel_params) = rel_params {
                    let rel_def_id = match self.env.get_serde_operator(rel_params) {
                        SerdeOperator::Map(map_op) => {
                            self.register_map_op_fields(
                                map_op,
                                &mut fields,
                                &FieldKind::EdgeProperty,
                                &mut field_namespace,
                            );

                            map_op.def_variant.def_id
                        }
                        SerdeOperator::ValueType(value_op) => {
                            self.register_value_op_fields(
                                value_op,
                                &mut fields,
                                &FieldKind::EdgeProperty,
                                &mut field_namespace,
                            );
                            value_op.def_variant.def_id
                        }
                        other => {
                            panic!("Tried to register edge rel_params for {other:?}");
                        }
                    };

                    let rel_edge_ref = self.get_def_type_ref(rel_def_id, QueryLevel::Node);
                    let rel_type_info = self.env.get_type_info(rel_def_id);

                    NewType::Indexed(
                        edge_index,
                        TypeData {
                            typename: self.namespace.edge(Some(rel_type_info), type_info),
                            input_typename: Some(
                                self.namespace.edge_input(Some(rel_type_info), type_info),
                            ),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_operator_id: type_info.operator_id.unwrap(),
                                    rel_edge_ref: Some(rel_edge_ref),
                                }),
                            }),
                        },
                    )
                } else {
                    NewType::Indexed(
                        edge_index,
                        TypeData {
                            typename: self.namespace.edge(None, type_info),
                            input_typename: Some(self.namespace.edge_input(None, type_info)),
                            partial_input_typename: None,
                            kind: TypeKind::Object(ObjectData {
                                fields,
                                kind: ObjectKind::Edge(EdgeData {
                                    node_operator_id: type_info.operator_id.unwrap(),
                                    rel_edge_ref: None,
                                }),
                            }),
                        },
                    )
                }
            }
            QueryLevel::Connection { rel_params } => {
                let connection_index = self.alloc_def_type_index(def_id, level);
                let edge_ref = self.get_def_type_ref(def_id, QueryLevel::Edge { rel_params });

                NewType::Indexed(
                    connection_index,
                    TypeData {
                        typename: self.namespace.connection(type_info),
                        input_typename: None,
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields: [(
                                smart_format!("edges"),
                                FieldData {
                                    kind: FieldKind::Edges,
                                    field_type: TypeRef::mandatory(edge_ref)
                                        .to_array(Optionality::Optional),
                                },
                            )]
                            .into(),
                            kind: ObjectKind::Connection,
                        }),
                    },
                )
            }
        }
    }

    fn make_node_type(&mut self, type_info: &TypeInfo) -> NewType {
        let operator_id = type_info
            .operator_id
            .unwrap_or_else(|| panic!("No selection operator id for `{:?}`", type_info.name));

        self.make_node_type_inner(type_info, operator_id)
    }

    fn make_node_type_inner(
        &mut self,
        type_info: &TypeInfo,
        operator_id: SerdeOperatorId,
    ) -> NewType {
        let typename = type_info.name.as_ref();

        match self.env.get_serde_operator(operator_id) {
            SerdeOperator::RelationSequence(_) => panic!("not handled here"),
            SerdeOperator::ConstructorSequence(_) => NewType::Indexed(
                self.alloc_def_type_index(type_info.def_id, QueryLevel::Node),
                TypeData {
                    typename: self.namespace.typename(type_info),
                    input_typename: Some(self.namespace.input(type_info)),
                    partial_input_typename: Some(self.namespace.partial_input(type_info)),
                    kind: TypeKind::CustomScalar(ScalarData {
                        serde_operator_id: operator_id,
                    }),
                },
            ),
            SerdeOperator::Union(union_op) => {
                match union_op.variants(ProcessorMode::Select, ProcessorLevel::Root) {
                    FilteredVariants::Single(operator_id) => {
                        self.make_node_type_inner(type_info, operator_id)
                    }
                    FilteredVariants::Multi(variants) => {
                        let node_index =
                            self.alloc_def_type_index(type_info.def_id, QueryLevel::Node);
                        let mut type_variants = vec![];

                        for variant in variants {
                            match classify_type(self.env, variant.operator_id) {
                                TypeClassification::Type(_, def_id, _operator_id) => {
                                    match self.get_def_type_ref(def_id, QueryLevel::Node) {
                                        UnitTypeRef::Indexed(type_index) => {
                                            type_variants.push(type_index);
                                        }
                                        _ => {
                                            panic!("Unexpected type in union");
                                        }
                                    }
                                }
                                TypeClassification::Id => {}
                                TypeClassification::NativeScalar => {
                                    panic!("BUG: Scalar in union");
                                }
                            }
                        }

                        debug!(
                            "created a union for `{typename:?}`: {operator_id:?} variants={variants:?}",
                        );

                        NewType::Indexed(
                            node_index,
                            TypeData {
                                typename: self.namespace.typename(type_info),
                                input_typename: Some(self.namespace.union_input(type_info)),
                                partial_input_typename: Some(
                                    self.namespace.union_partial_input(type_info),
                                ),
                                kind: TypeKind::Union(UnionData {
                                    union_def_id: type_info.def_id,
                                    variants: type_variants,
                                }),
                            },
                        )
                    }
                }
            }
            SerdeOperator::ValueType(value_op) => {
                let type_index = self.alloc_def_type_index(type_info.def_id, QueryLevel::Node);
                NewType::Indexed(
                    type_index,
                    TypeData {
                        typename: self.namespace.typename(type_info),
                        input_typename: Some(self.namespace.input(type_info)),
                        partial_input_typename: Some(self.namespace.partial_input(type_info)),
                        kind: self.make_value_op_type_kind(type_info, value_op),
                    },
                )
            }
            SerdeOperator::Map(map_op) => {
                let type_index = self.alloc_def_type_index(type_info.def_id, QueryLevel::Node);
                NewType::Indexed(
                    type_index,
                    TypeData {
                        typename: self.namespace.typename(type_info),
                        input_typename: Some(self.namespace.input(type_info)),
                        partial_input_typename: Some(self.namespace.partial_input(type_info)),
                        kind: self.make_map_op_type_kind(type_info, map_op),
                    },
                )
            }
            operator => NewType::NativeScalar(self.make_new_native_scalar(operator_id, operator)),
        }
    }

    fn make_new_native_scalar(
        &mut self,
        operator_id: SerdeOperatorId,
        serde_operator: &SerdeOperator,
    ) -> NativeScalarRef {
        let kind = match serde_operator {
            SerdeOperator::Unit => NativeScalarKind::Unit,
            SerdeOperator::False(_) | SerdeOperator::True(_) | SerdeOperator::Bool(_) => {
                NativeScalarKind::Bool
            }
            SerdeOperator::Int(def_id) => NativeScalarKind::Int(*def_id),
            SerdeOperator::Number(def_id) => NativeScalarKind::Number(*def_id),
            SerdeOperator::String(_)
            | SerdeOperator::StringConstant(..)
            | SerdeOperator::StringPattern(_)
            | SerdeOperator::CapturingStringPattern(_) => NativeScalarKind::String,
            SerdeOperator::PrimaryId(..) => panic!("Id should not appear in GraphQL"),
            SerdeOperator::ValueType(_)
            | SerdeOperator::Union(_)
            | SerdeOperator::Map(_)
            | SerdeOperator::RelationSequence(_)
            | SerdeOperator::ConstructorSequence(_) => panic!("not a native scalar"),
        };

        NativeScalarRef { operator_id, kind }
    }

    fn make_value_op_type_kind(
        &mut self,
        type_info: &TypeInfo,
        value_op: &ValueOperator,
    ) -> TypeKind {
        match self.env.get_serde_operator(value_op.inner_operator_id) {
            SerdeOperator::ValueType(inner_value_op) => {
                self.make_value_op_type_kind(type_info, inner_value_op)
            }
            SerdeOperator::Map(map_op) => self.make_map_op_type_kind(type_info, map_op),
            other => panic!("Unsupported: {other:?}"),
        }
    }

    fn make_map_op_type_kind(&mut self, type_info: &TypeInfo, map_op: &MapOperator) -> TypeKind {
        let operator_id = type_info.operator_id.unwrap();
        let mut fields = IndexMap::default();

        if let Some(entity_info) = &type_info.entity_info {
            if let Some(id_field_name) = &entity_info.id_inherent_property_name {
                fields.insert(
                    id_field_name.clone(),
                    FieldData {
                        kind: FieldKind::Id(IdPropertyData {
                            relation_id: entity_info.id_relation_id,
                            operator_id: entity_info.id_operator_id,
                        }),
                        field_type: TypeRef::mandatory(UnitTypeRef::NativeScalar(
                            NativeScalarRef {
                                operator_id: entity_info.id_operator_id,
                                kind: NativeScalarKind::ID,
                            },
                        )),
                    },
                );
            }
        }

        self.register_map_op_fields(
            map_op,
            &mut fields,
            &FieldKind::Property,
            &mut Namespace::new(),
        );

        TypeKind::Object(ObjectData {
            fields,
            kind: ObjectKind::Node(NodeData {
                def_id: type_info.def_id,
                entity_id: type_info
                    .entity_info
                    .as_ref()
                    .map(|entity_info| entity_info.id_value_def_id),
                operator_id,
            }),
        })
    }

    fn register_value_op_fields(
        &mut self,
        value_op: &ValueOperator,
        fields: &mut IndexMap<String, FieldData>,
        make_property_field_kind: &dyn Fn(PropertyData) -> FieldKind,
        field_namespace: &mut Namespace,
    ) {
        match self.env.get_serde_operator(value_op.inner_operator_id) {
            SerdeOperator::Map(map_op) => {
                self.register_map_op_fields(
                    map_op,
                    fields,
                    make_property_field_kind,
                    field_namespace,
                );
            }
            SerdeOperator::ValueType(inner) => self.register_value_op_fields(
                inner,
                fields,
                make_property_field_kind,
                field_namespace,
            ),
            other => panic!("other operator: {other:?}"),
        }
    }

    fn register_map_op_fields(
        &mut self,
        map_op: &MapOperator,
        fields: &mut IndexMap<String, FieldData>,
        make_property_field_kind: &dyn Fn(PropertyData) -> FieldKind,
        field_namespace: &mut Namespace,
    ) {
        for (property_name, property) in &map_op.properties {
            match self.env.get_serde_operator(property.value_operator_id) {
                SerdeOperator::Map(property_map_op) => {
                    fields.insert(
                        field_namespace.unique_literal(property_name),
                        self.unit_def_field_data(property, property_map_op.def_variant.def_id),
                    );
                }
                SerdeOperator::Union(union_op) => {
                    fields.insert(
                        field_namespace.unique_literal(property_name),
                        self.unit_def_field_data(property, union_op.union_def_variant().def_id),
                    );
                }
                SerdeOperator::RelationSequence(seq_op) => {
                    match classify_type(self.env, seq_op.ranges[0].operator_id) {
                        TypeClassification::Type(NodeClassification::Entity, node_id, _) => {
                            let connection_ref = self.get_def_type_ref(
                                node_id,
                                QueryLevel::Connection {
                                    rel_params: property.rel_params_operator_id,
                                },
                            );

                            fields.insert(
                                field_namespace.unique_literal(property_name),
                                FieldData::mandatory(
                                    FieldKind::Connection {
                                        property_id: Some(property.property_id),
                                        first: argument::First,
                                        after: argument::After,
                                    },
                                    connection_ref,
                                ),
                            );
                        }
                        TypeClassification::Type(NodeClassification::Node, node_id, _) => {
                            // Only introduce an Edge level if there are rel_params
                            let query_level = match property.rel_params_operator_id {
                                Some(rel_params) => QueryLevel::Edge {
                                    rel_params: Some(rel_params),
                                },
                                None => QueryLevel::Node,
                            };
                            let edge_ref = self.get_def_type_ref(node_id, query_level);

                            fields.insert(
                                field_namespace.unique_literal(property_name),
                                FieldData {
                                    kind: make_property_field_kind(PropertyData {
                                        property_id: property.property_id,
                                        value_operator_id: property.value_operator_id,
                                    }),
                                    field_type: TypeRef::mandatory(edge_ref)
                                        .to_array(Optionality::from_optional(property.optional)),
                                },
                            );
                        }
                        TypeClassification::Id => {
                            debug!("Id not handled here");
                        }
                        TypeClassification::NativeScalar => {
                            panic!(
                                "Unhandled scalar: {:?}",
                                self.env.get_serde_operator(seq_op.ranges[0].operator_id)
                            )
                        }
                    }
                }
                SerdeOperator::ConstructorSequence(seq_op) => {
                    fields.insert(
                        field_namespace.unique_literal(property_name),
                        self.unit_def_field_data(property, seq_op.def_variant.def_id),
                    );
                }
                operator => {
                    let scalar_ref = UnitTypeRef::NativeScalar(
                        self.make_new_native_scalar(property.value_operator_id, operator),
                    );
                    fields.insert(
                        field_namespace.unique_literal(property_name),
                        FieldData {
                            kind: make_property_field_kind(PropertyData {
                                property_id: property.property_id,
                                value_operator_id: property.value_operator_id,
                            }),
                            field_type: TypeRef {
                                modifier: TypeModifier::new_unit(Optionality::from_optional(
                                    property.optional,
                                )),
                                unit: scalar_ref,
                            },
                        },
                    );
                }
            }
        }
    }

    fn unit_def_field_data(&mut self, property: &SerdeProperty, node_def_id: DefId) -> FieldData {
        let unit_ref = self.get_def_type_ref(
            node_def_id,
            if let Some(rel_params) = property.rel_params_operator_id {
                QueryLevel::Edge {
                    rel_params: Some(rel_params),
                }
            } else {
                QueryLevel::Node
            },
        );
        FieldData {
            kind: FieldKind::Property(PropertyData {
                property_id: property.property_id,
                value_operator_id: property.value_operator_id,
            }),
            field_type: TypeRef {
                modifier: TypeModifier::new_unit(Optionality::from_optional(property.optional)),
                unit: unit_ref,
            },
        }
    }

    pub fn add_entity_queries_and_mutations(&mut self, entity_data: EntityData) {
        let type_info = self.env.get_type_info(entity_data.node_def_id);

        let node_ref = UnitTypeRef::Indexed(entity_data.type_index);
        let connection_ref = self.get_def_type_ref(
            entity_data.node_def_id,
            QueryLevel::Connection { rel_params: None },
        );

        let id_type_info = self.env.get_type_info(entity_data.id_def_id);
        let id_operator_id = id_type_info.operator_id.expect("No id_operator_id");

        {
            let query = self.schema.object_data_mut(self.schema.query);
            query.fields.insert(
                self.namespace.list(type_info),
                FieldData::mandatory(
                    FieldKind::Connection {
                        property_id: None,
                        first: argument::First,
                        after: argument::After,
                    },
                    connection_ref,
                ),
            );
        }

        {
            let mutation = self.schema.object_data_mut(self.schema.mutation);
            mutation.fields.insert(
                self.namespace.create(type_info),
                FieldData {
                    kind: FieldKind::CreateMutation {
                        input: argument::Input(
                            entity_data.type_index,
                            entity_data.node_def_id,
                            TypingPurpose::Input,
                        ),
                    },
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.update(type_info),
                FieldData {
                    kind: FieldKind::UpdateMutation {
                        input: argument::Input(
                            entity_data.type_index,
                            entity_data.node_def_id,
                            TypingPurpose::PartialInput,
                        ),
                        id: argument::Id(id_operator_id),
                    },
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.delete(type_info),
                FieldData {
                    kind: FieldKind::DeleteMutation {
                        id: argument::Id(id_operator_id),
                    },
                    field_type: TypeRef::mandatory(UnitTypeRef::NativeScalar(NativeScalarRef {
                        operator_id: SerdeOperatorId(42),
                        kind: NativeScalarKind::Bool,
                    })),
                },
            );
        }
    }
}
