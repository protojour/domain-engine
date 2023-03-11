use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    env::{Env, TypeInfo},
    serde::{MapType, SerdeOperator, SerdeOperatorId, SerdeProperty},
    smart_format, DefId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::adapter::{
    adapt::{classify_type, NodeClassification, TypeClassification},
    namespace::Namespace,
};

use super::{
    data::{
        ArgumentsKind, ConnectionData, EdgeData, FieldData, NativeScalarRef, NodeData, ObjectData,
        ObjectKind, Optionality, ScalarData, TypeData, TypeIndex, TypeKind, TypeModifier, TypeRef,
        UnionData, UnitTypeRef,
    },
    VirtualSchema,
};

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub(super) enum QueryLevel {
    Node,
    Edge { rel_params: Option<SerdeOperatorId> },
    Connection { rel_params: Option<SerdeOperatorId> },
}

enum NewType {
    Indexed(TypeIndex, TypeData),
    NativeScalar(NativeScalarRef),
}

pub(super) struct VirtualSchemaBuilder<'a> {
    pub env: &'a Env,
    pub schema: &'a mut VirtualSchema,
    pub namespace: &'a mut Namespace,
    pub type_index_by_def: FnvHashMap<(DefId, QueryLevel), TypeIndex>,
}

impl<'a> VirtualSchemaBuilder<'a> {
    pub fn register_query(&mut self) {
        let index = self.next_type_index();
        self.schema.types.push(TypeData {
            typename: "Query".into(),
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
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Mutation,
            }),
        });
        self.schema.mutation = index;
    }

    pub fn get_def_type_ref(&mut self, def_id: DefId, level: QueryLevel) -> UnitTypeRef {
        if let Some(type_index) = self.type_index_by_def.get(&(def_id, level)) {
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
            kind: TypeKind::Scalar(ScalarData {
                serde_operator_id: SerdeOperatorId(0),
            }),
        });
        self.type_index_by_def.insert((def_id, level), index);
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
                        SerdeOperator::MapType(map_type) => {
                            self.register_fields(map_type, &mut fields);
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
                        kind: TypeKind::Object(ObjectData {
                            fields: [(
                                smart_format!("edges"),
                                FieldData::no_args(
                                    TypeRef::mandatory(edge_ref).to_mandatory_array(),
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
        let operator_id = type_info
            .graphql_operator_id
            .expect("No GraphQL operator id");

        match self.env.get_serde_operator(operator_id) {
            SerdeOperator::RelationSequence(_) => panic!("not handled here"),
            SerdeOperator::ConstructorSequence(_) => todo!("custom scalar"),
            SerdeOperator::ValueType(_) => todo!("value type"),
            SerdeOperator::ValueUnionType(value_union_type) => {
                let node_index = self.alloc_def_type_index(type_info.def_id, QueryLevel::Node);

                let mut variants = vec![];

                for variant in &value_union_type.variants {
                    match classify_type(self.env, variant.operator_id) {
                        TypeClassification::Type(_, def_id, _operator_id) => {
                            match self.get_def_type_ref(def_id, QueryLevel::Node) {
                                UnitTypeRef::Indexed(type_index) => {
                                    variants.push(type_index);
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

                debug!(
                    "created a union for `{type_name}`: {operator_id:?} variants={variants:?}",
                    type_name = type_info.name
                );

                NewType::Indexed(
                    node_index,
                    TypeData {
                        typename: type_info.name.clone(),
                        kind: TypeKind::Union(UnionData {
                            union_def_id: type_info.def_id,
                            variants,
                        }),
                    },
                )
            }
            SerdeOperator::MapType(map_type) => self.make_map_type(type_info, map_type),
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
            | SerdeOperator::ValueUnionType(_)
            | SerdeOperator::MapType(_)
            | SerdeOperator::RelationSequence(_) => panic!("not a scalar"),
        }
    }

    fn make_map_type(&mut self, type_info: &TypeInfo, map_type: &MapType) -> NewType {
        let type_index = self.alloc_def_type_index(type_info.def_id, QueryLevel::Node);
        let typename = type_info.name.as_str();

        let mut fields = IndexMap::default();

        if let Some(entity_id) = type_info.entity_id {
            let id_type_info = self.env.get_type_info(entity_id);
            let id_operator_id = id_type_info.rest_operator_id.expect("No id_operator_id");

            fields.insert(
                "_id".into(),
                FieldData {
                    arguments: ArgumentsKind::Empty,
                    field_type: TypeRef::mandatory(UnitTypeRef::ID(id_operator_id)),
                },
            );
        }

        self.register_fields(map_type, &mut fields);

        NewType::Indexed(
            type_index,
            TypeData {
                typename: typename.into(),
                kind: TypeKind::Object(ObjectData {
                    fields,
                    kind: ObjectKind::Node(NodeData {
                        def_id: type_info.def_id,
                        entity_id: type_info.entity_id,
                    }),
                }),
            },
        )
    }

    fn register_fields(&mut self, map_type: &MapType, fields: &mut IndexMap<String, FieldData>) {
        for (property_name, property) in &map_type.properties {
            match self.env.get_serde_operator(property.value_operator_id) {
                SerdeOperator::MapType(property_map_type) => {
                    fields.insert(
                        property_name.clone(),
                        self.unit_def_field_data(property, property_map_type.def_variant.def_id),
                    );
                }
                SerdeOperator::ValueUnionType(value_union_type) => {
                    fields.insert(
                        property_name.clone(),
                        self.unit_def_field_data(
                            property,
                            value_union_type.union_def_variant.def_id,
                        ),
                    );
                }
                SerdeOperator::RelationSequence(sequence_type) => {
                    match classify_type(self.env, sequence_type.ranges[0].operator_id) {
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
                                self.env
                                    .get_serde_operator(sequence_type.ranges[0].operator_id)
                            )
                        }
                    }
                }
                operator => {
                    let scalar_ref =
                        self.get_operator_scalar_type_ref(property.value_operator_id, operator);
                    fields.insert(
                        property_name.clone(),
                        FieldData::no_args(
                            TypeRef::mandatory(scalar_ref)
                                .to_array(Optionality::from_optional(property.optional)),
                        ),
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

    pub fn add_entity_queries_and_mutations(&mut self, type_index: TypeIndex, def_id: DefId) {
        let type_data = self.schema.type_data(type_index);
        let typename = type_data.typename.clone();

        let node_ref = UnitTypeRef::Indexed(type_index);
        let connection_ref =
            self.get_def_type_ref(def_id, QueryLevel::Connection { rel_params: None });

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
                    arguments: ArgumentsKind::CreateMutation,
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.update(&typename),
                FieldData {
                    arguments: ArgumentsKind::UpdateMutation,
                    field_type: TypeRef::mandatory(node_ref),
                },
            );

            mutation.fields.insert(
                self.namespace.delete(&typename),
                FieldData {
                    arguments: ArgumentsKind::DeleteMutation,
                    field_type: TypeRef::mandatory(UnitTypeRef::NativeScalar(
                        NativeScalarRef::Bool,
                    )),
                },
            );
        }
    }
}
