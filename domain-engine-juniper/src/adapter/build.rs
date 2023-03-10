use std::sync::Arc;

use fnv::FnvHashMap;
use indexmap::IndexMap;
use ontol_runtime::{
    env::{Env, TypeInfo},
    serde::{MapType, SerdeOperator, SerdeOperatorId},
    smart_format, DefId, PackageId,
};
use smartstring::alias::String;
use tracing::debug;

use crate::{
    adapter::{
        adapt::{classify_type, TypeClassification},
        data2::UnionData,
    },
    SchemaBuildError,
};

use super::{
    data2::{
        ArgumentsKind, DomainData, EdgeData, FieldData, NativeScalarRef, NodeData, ObjectData,
        ObjectKind, ScalarData, TypeData, TypeIndex, TypeKind, TypeRef, UnitTypeRef,
    },
    namespace::Namespace,
};

/// Builds a "shadow schema" before handing that over to juniper
pub fn build_domain_data(
    env: Arc<Env>,
    package_id: PackageId,
) -> Result<DomainData, SchemaBuildError> {
    let domain = env
        .find_domain(&package_id)
        .ok_or(SchemaBuildError::UnknownPackage)?;

    let mut domain_data = DomainData {
        env: env.clone(),
        package_id,
        query: TypeIndex(0),
        mutation: TypeIndex(0),
        types: vec![],
    };
    let mut namespace = Namespace::new();
    let mut builder = Builder {
        env: env.as_ref(),
        domain_data: &mut domain_data,
        namespace: &mut namespace,
        type_index_by_def: FnvHashMap::default(),
    };

    builder.register_query();
    builder.register_mutation();

    for (_, def_id) in &domain.type_names {
        let type_info = domain.type_info(*def_id);

        if let Some(operator_id) = type_info.graphql_operator_id {
            debug!("adapt type `{}` {:?}", type_info.name, operator_id);

            let type_ref = builder.get_def_type_ref(type_info.def_id, QueryLevel::Node);

            if let Some((type_index, def_id)) = builder.domain_data.entity_check(type_ref) {
                builder.add_entity_queries_and_mutations(type_index, def_id);
            }
        }
    }

    Ok(domain_data)
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
enum QueryLevel {
    Node,
    Edge,
    Connection,
}

enum NewType {
    Indexed(TypeIndex, TypeData),
    NativeScalar(NativeScalarRef),
}

pub struct Builder<'a> {
    env: &'a Env,
    domain_data: &'a mut DomainData,
    namespace: &'a mut Namespace,
    type_index_by_def: FnvHashMap<(DefId, QueryLevel), TypeIndex>,
}

impl<'a> Builder<'a> {
    fn register_query(&mut self) {
        let index = self.next_type_index();
        self.domain_data.types.push(TypeData {
            typename: "Query".into(),
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Query,
            }),
        });
        self.domain_data.query = index;
    }

    fn register_mutation(&mut self) {
        let index = self.next_type_index();
        self.domain_data.types.push(TypeData {
            typename: "Mutation".into(),
            kind: TypeKind::Object(ObjectData {
                fields: Default::default(),
                kind: ObjectKind::Mutation,
            }),
        });
        self.domain_data.mutation = index;
    }

    fn get_def_type_ref(&mut self, def_id: DefId, level: QueryLevel) -> UnitTypeRef {
        if let Some(type_index) = self.type_index_by_def.get(&(def_id, level)) {
            return UnitTypeRef::Indexed(*type_index);
        }

        match self.make_def_type(def_id, level) {
            NewType::Indexed(type_index, type_data) => {
                self.domain_data.types[type_index.0 as usize] = type_data;
                UnitTypeRef::Indexed(type_index)
            }
            NewType::NativeScalar(scalar_ref) => UnitTypeRef::NativeScalar(scalar_ref),
        }
    }

    fn next_type_index(&self) -> TypeIndex {
        TypeIndex(self.domain_data.types.len() as u32)
    }

    fn alloc_def_type_index(&mut self, def_id: DefId, level: QueryLevel) -> TypeIndex {
        let index = self.next_type_index();
        // note: this will be overwritten later
        self.domain_data.types.push(TypeData {
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
            QueryLevel::Edge => {
                let edge_index = self.alloc_def_type_index(def_id, level);
                let node_ref = self.get_def_type_ref(def_id, QueryLevel::Node);

                NewType::Indexed(
                    edge_index,
                    TypeData {
                        typename: smart_format!("{typename}Edge"),
                        kind: TypeKind::Object(ObjectData {
                            fields: [(
                                smart_format!("node"),
                                FieldData {
                                    arguments: ArgumentsKind::Empty,
                                    field_type: TypeRef::mandatory(node_ref),
                                },
                            )]
                            .into(),
                            kind: ObjectKind::Edge(EdgeData {}),
                        }),
                    },
                )
            }
            QueryLevel::Connection => {
                let connection_index = self.alloc_def_type_index(def_id, level);
                let edge_ref = self.get_def_type_ref(def_id, QueryLevel::Edge);

                NewType::Indexed(
                    connection_index,
                    TypeData {
                        typename: smart_format!("{typename}Connection"),
                        kind: TypeKind::Object(ObjectData {
                            fields: [(
                                smart_format!("edges"),
                                FieldData {
                                    arguments: ArgumentsKind::Empty,
                                    field_type: TypeRef::mandatory(edge_ref).to_mandatory_array(),
                                },
                            )]
                            .into(),
                            kind: ObjectKind::Edge(EdgeData {}),
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
            SerdeOperator::Id(_) => panic!("Id should not appear in GraphQL"),
            SerdeOperator::MapType(map_type) => self.register_map_type(type_info, map_type),
        }
    }

    fn register_map_type(&mut self, type_info: &TypeInfo, _map_type: &MapType) -> NewType {
        let node_index = self.alloc_def_type_index(type_info.def_id, QueryLevel::Node);
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

        NewType::Indexed(
            node_index,
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

    fn add_entity_queries_and_mutations(&mut self, type_index: TypeIndex, def_id: DefId) {
        let type_data = self.domain_data.type_data(type_index);
        let typename = type_data.typename.clone();

        let node_ref = UnitTypeRef::Indexed(type_index);
        let connection_type = self.get_def_type_ref(def_id, QueryLevel::Connection);

        {
            let query = self.domain_data.object_data_mut(self.domain_data.query);
            query.fields.insert(
                self.namespace.list(&typename),
                FieldData {
                    arguments: ArgumentsKind::ConnectionQuery,
                    field_type: TypeRef::mandatory(connection_type),
                },
            );
        }

        {
            let mutation = self.domain_data.object_data_mut(self.domain_data.mutation);
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
                    arguments: ArgumentsKind::UpdateMutation,
                    field_type: TypeRef::mandatory(UnitTypeRef::NativeScalar(
                        NativeScalarRef::Bool,
                    )),
                },
            );
        }
    }
}
