use indexmap::IndexMap;
use ontol_runtime::{
    env::{Env, TypeInfo},
    serde::{
        operator::{FilteredVariants, MapOperator, SerdeOperator, SerdeOperatorId, SerdeProperty},
        processor::{ProcessorLevel, ProcessorMode},
    },
    smart_format, DefId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::virtual_schema::schema::{classify_type, TypeClassification};

use super::{
    data::{
        ConnectionData, EdgeData, FieldArgument, FieldArguments, FieldData, NativeScalarRef,
        NodeData, ObjectData, ObjectKind, Optionality, ScalarData, TypeData, TypeIndex, TypeKind,
        TypeModifier, TypeRef, UnionData, UnitTypeRef,
    },
    namespace::Namespace,
    schema::NodeClassification,
    EntityInfo, QueryLevel, TypingPurpose, VirtualSchema,
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

    pub fn get_operator_scalar_type_ref(
        &mut self,
        operator_id: SerdeOperatorId,
        serde_operator: &SerdeOperator,
    ) -> UnitTypeRef {
        match self.make_new_scalar(operator_id, serde_operator) {
            NewType::Indexed(..) => panic!("BUG: Scalars should not be indexed"),
            NewType::NativeScalar(native_scalar) => UnitTypeRef::NativeScalar(native_scalar),
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
        let typename = type_info.name.as_str();

        match level {
            QueryLevel::Node => self.make_node_type(type_info),
            QueryLevel::Edge { rel_params } => {
                let edge_index = self.alloc_def_type_index(def_id, level);
                let node_ref = self.get_def_type_ref(def_id, QueryLevel::Node);

                // FIXME: what if some of the relation data's fields are called "node"
                let mut fields: IndexMap<String, FieldData> = [(
                    smart_format!("node"),
                    FieldData::no_args(TypeRef::mandatory(node_ref)),
                )]
                .into();

                if let Some(rel_params) = rel_params {
                    match self.env.get_serde_operator(rel_params) {
                        SerdeOperator::Map(map_op) => {
                            self.register_fields(map_op, &mut fields);
                        }
                        other => {
                            panic!("Tried to register edge rel_params for {other:?}");
                        }
                    }
                }

                NewType::Indexed(
                    edge_index,
                    TypeData {
                        typename: smart_format!("{typename}Edge"),
                        input_typename: Some(smart_format!("{typename}EdgeInput")),
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields,
                            kind: ObjectKind::Edge(EdgeData {}),
                        }),
                    },
                )
            }
            QueryLevel::Connection { rel_params } => {
                let connection_index = self.alloc_def_type_index(def_id, level);
                let edge_ref = self.get_def_type_ref(def_id, QueryLevel::Edge { rel_params });

                NewType::Indexed(
                    connection_index,
                    TypeData {
                        typename: smart_format!("{typename}Connection"),
                        input_typename: None,
                        partial_input_typename: None,
                        kind: TypeKind::Object(ObjectData {
                            fields: [(
                                smart_format!("edges"),
                                FieldData::no_args(
                                    TypeRef::mandatory(edge_ref).to_array(Optionality::Optional),
                                ),
                            )]
                            .into(),
                            kind: ObjectKind::Connection(ConnectionData {}),
                        }),
                    },
                )
            }
        }
    }

    fn make_node_type(&mut self, type_info: &TypeInfo) -> NewType {
        let operator_id = type_info.operator_id.expect("No selection operator id");

        self.make_node_type_inner(type_info, operator_id)
    }

    fn make_node_type_inner(
        &mut self,
        type_info: &TypeInfo,
        operator_id: SerdeOperatorId,
    ) -> NewType {
        match self.env.get_serde_operator(operator_id) {
            SerdeOperator::RelationSequence(_) => panic!("not handled here"),
            SerdeOperator::ConstructorSequence(_) => todo!("custom scalar"),
            SerdeOperator::ValueType(_) => todo!("value type"),
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
                                TypeClassification::Scalar => {
                                    panic!("BUG: Scalar in union");
                                }
                            }
                        }

                        let typename = type_info.name.as_str();

                        debug!(
                            "created a union for `{typename}`: {operator_id:?} variants={variants:?}",
                        );

                        NewType::Indexed(
                            node_index,
                            TypeData {
                                typename: type_info.name.clone(),
                                input_typename: Some(smart_format!("{typename}UnionInput")),
                                partial_input_typename: Some(smart_format!(
                                    "{typename}UnionPartialInput"
                                )),
                                kind: TypeKind::Union(UnionData {
                                    union_def_id: type_info.def_id,
                                    variants: type_variants,
                                }),
                            },
                        )
                    }
                }
            }
            SerdeOperator::Map(map_op) => self.make_map_type(type_info, map_op),
            operator => self.make_new_scalar(operator_id, operator),
        }
    }

    fn make_new_scalar(
        &mut self,
        operator_id: SerdeOperatorId,
        serde_operator: &SerdeOperator,
    ) -> NewType {
        match serde_operator {
            SerdeOperator::Unit => NewType::NativeScalar(NativeScalarRef::Unit),
            SerdeOperator::Int(def_id) => NewType::NativeScalar(NativeScalarRef::Int(*def_id)),
            SerdeOperator::Number(def_id) => {
                NewType::NativeScalar(NativeScalarRef::Number(*def_id))
            }
            SerdeOperator::String(_)
            | SerdeOperator::StringConstant(..)
            | SerdeOperator::StringPattern(_)
            | SerdeOperator::CapturingStringPattern(_) => {
                NewType::NativeScalar(NativeScalarRef::String(operator_id))
            }
            SerdeOperator::ConstructorSequence(_) => todo!("custom scalar"),
            SerdeOperator::Id(_) => panic!("Id should not appear in GraphQL"),
            SerdeOperator::ValueType(_)
            | SerdeOperator::Union(_)
            | SerdeOperator::Map(_)
            | SerdeOperator::RelationSequence(_) => panic!("not a scalar"),
        }
    }

    fn make_map_type(&mut self, type_info: &TypeInfo, map_type: &MapOperator) -> NewType {
        let type_index = self.alloc_def_type_index(type_info.def_id, QueryLevel::Node);
        let typename = type_info.name.as_str();
        let operator_id = type_info.operator_id.unwrap();

        let mut fields = IndexMap::default();

        if let Some(entity_id) = type_info.entity_id {
            let id_type_info = self.env.get_type_info(entity_id);
            let id_operator_id = id_type_info.operator_id.expect("No id_operator_id");

            fields.insert(
                "_id".into(),
                FieldData {
                    arguments: FieldArguments::Empty,
                    field_type: TypeRef::mandatory(UnitTypeRef::ID(id_operator_id)),
                },
            );
        }

        self.register_fields(map_type, &mut fields);

        NewType::Indexed(
            type_index,
            TypeData {
                typename: typename.into(),
                input_typename: Some(self.namespace.input(typename)),
                partial_input_typename: Some(self.namespace.partial_input(typename)),
                kind: TypeKind::Object(ObjectData {
                    fields,
                    kind: ObjectKind::Node(NodeData {
                        def_id: type_info.def_id,
                        entity_id: type_info.entity_id,
                        operator_id,
                    }),
                }),
            },
        )
    }

    fn register_fields(&mut self, map_op: &MapOperator, fields: &mut IndexMap<String, FieldData>) {
        for (property_name, property) in &map_op.properties {
            match self.env.get_serde_operator(property.value_operator_id) {
                SerdeOperator::Map(property_map_op) => {
                    fields.insert(
                        property_name.clone(),
                        self.unit_def_field_data(property, property_map_op.def_variant.def_id),
                    );
                }
                SerdeOperator::Union(union_op) => {
                    fields.insert(
                        property_name.clone(),
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
                                property_name.clone(),
                                FieldData::connection(connection_ref),
                            );
                        }
                        TypeClassification::Type(NodeClassification::Node, node_id, _) => {
                            let edge_ref = self.get_def_type_ref(
                                node_id,
                                QueryLevel::Edge {
                                    rel_params: property.rel_params_operator_id,
                                },
                            );
                            fields.insert(
                                property_name.clone(),
                                FieldData::no_args(
                                    TypeRef::mandatory(edge_ref)
                                        .to_array(Optionality::from_optional(property.optional)),
                                ),
                            );
                        }
                        TypeClassification::Id => {
                            debug!("Id not handled here");
                        }
                        TypeClassification::Scalar => {
                            panic!(
                                "Unhandled scalar: {:?}",
                                self.env.get_serde_operator(seq_op.ranges[0].operator_id)
                            )
                        }
                    }
                }
                operator => {
                    let scalar_ref =
                        self.get_operator_scalar_type_ref(property.value_operator_id, operator);
                    fields.insert(
                        property_name.clone(),
                        FieldData::no_args(TypeRef {
                            modifier: TypeModifier::new_unit(Optionality::from_optional(
                                property.optional,
                            )),
                            unit: scalar_ref,
                        }),
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

        FieldData::no_args(TypeRef {
            modifier: TypeModifier::new_unit(Optionality::from_optional(property.optional)),
            unit: unit_ref,
        })
    }

    pub fn add_entity_queries_and_mutations(&mut self, entity_info: EntityInfo) {
        let type_data = self.schema.type_data(entity_info.type_index);
        let typename = type_data.typename.clone();

        let node_ref = UnitTypeRef::Indexed(entity_info.type_index);
        let connection_ref = self.get_def_type_ref(
            entity_info.node_def_id,
            QueryLevel::Connection { rel_params: None },
        );

        let id_type_info = self.env.get_type_info(entity_info.id_def_id);
        let id_operator_id = id_type_info.operator_id.expect("No id_operator_id");

        {
            let query = self.schema.object_data_mut(self.schema.query);
            query.fields.insert(
                self.namespace.list(&typename),
                FieldData::connection(connection_ref),
            );
        }

        {
            let mutation = self.schema.object_data_mut(self.schema.mutation);
            mutation.fields.insert(
                self.namespace.create(&typename),
                FieldData {
                    arguments: FieldArguments::CreateMutation {
                        input: FieldArgument::Input(
                            entity_info.type_index,
                            entity_info.node_def_id,
                            TypingPurpose::Input,
                        ),
                    },
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.update(&typename),
                FieldData {
                    arguments: FieldArguments::UpdateMutation {
                        input: FieldArgument::Input(
                            entity_info.type_index,
                            entity_info.node_def_id,
                            TypingPurpose::PartialInput,
                        ),
                        id: FieldArgument::Id(id_operator_id, TypingPurpose::Input),
                    },
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.delete(&typename),
                FieldData {
                    arguments: FieldArguments::DeleteMutation {
                        id: FieldArgument::Id(id_operator_id, TypingPurpose::Input),
                    },
                    field_type: TypeRef::mandatory(UnitTypeRef::NativeScalar(
                        NativeScalarRef::Bool,
                    )),
                },
            );
        }
    }
}
