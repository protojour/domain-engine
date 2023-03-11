use std::sync::Arc;

use fnv::FnvHashMap;
use ontol_runtime::{env::Env, DefId, PackageId};
use tracing::debug;

use crate::{
    adapter::namespace::Namespace, type_info::GraphqlTypeName, virtual_schema::builder::QueryLevel,
    SchemaBuildError,
};

use self::{
    builder::VirtualSchemaBuilder,
    data::{NodeData, ObjectData, ObjectKind, TypeData, TypeIndex, TypeKind, UnitTypeRef},
};

pub mod data;

mod builder;

#[derive(Clone)]
pub struct VirtualIndexedTypeInfo {
    pub virtual_schema: Arc<VirtualSchema>,
    pub type_index: TypeIndex,
}

impl VirtualIndexedTypeInfo {
    pub fn env(&self) -> &Env {
        &self.virtual_schema.env
    }

    pub fn type_data(&self) -> &TypeData {
        self.virtual_schema.type_data(self.type_index)
    }
}

impl GraphqlTypeName for VirtualIndexedTypeInfo {
    fn graphql_type_name(&self) -> &str {
        &self.type_data().typename
    }
}

/// The virtual schema is a schema representation
/// held as a "shadow" schema behind juniper's user-facing schema.
///
/// This comes in handy when performing lookahead on selection sets,
/// parsing field arguments, etc.
pub struct VirtualSchema {
    env: Arc<Env>,
    _package_id: PackageId,
    query: TypeIndex,
    mutation: TypeIndex,
    types: Vec<TypeData>,
}

impl VirtualSchema {
    /// Builds a "shadow schema" before handing that over to juniper
    pub fn build(env: Arc<Env>, package_id: PackageId) -> Result<Self, SchemaBuildError> {
        let domain = env
            .find_domain(&package_id)
            .ok_or(SchemaBuildError::UnknownPackage)?;

        let mut schema = Self {
            env: env.clone(),
            _package_id: package_id,
            query: TypeIndex(0),
            mutation: TypeIndex(0),
            types: Vec::with_capacity(domain.type_names.len()),
        };
        let mut namespace = Namespace::default();
        let mut builder = VirtualSchemaBuilder {
            env: env.as_ref(),
            schema: &mut schema,
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

                if let Some(entity_info) = builder.schema.entity_check(type_ref) {
                    builder.add_entity_queries_and_mutations(entity_info);
                }
            }
        }

        Ok(schema)
    }

    pub fn env(&self) -> &Env {
        &self.env
    }

    pub fn indexed_type_info(self: &Arc<Self>, type_index: TypeIndex) -> VirtualIndexedTypeInfo {
        VirtualIndexedTypeInfo {
            virtual_schema: self.clone(),
            type_index,
        }
    }

    pub fn query_type_info(self: &Arc<Self>) -> VirtualIndexedTypeInfo {
        self.indexed_type_info(self.query)
    }

    pub fn mutation_type_info(self: &Arc<Self>) -> VirtualIndexedTypeInfo {
        self.indexed_type_info(self.mutation)
    }

    pub fn type_data(&self, index: TypeIndex) -> &TypeData {
        &self.types[index.0 as usize]
    }

    fn object_data_mut(&mut self, index: TypeIndex) -> &mut ObjectData {
        let type_data = self.types.get_mut(index.0 as usize).unwrap();
        match &mut type_data.kind {
            TypeKind::Object(object_data) => object_data,
            _ => panic!("{index:?} is not an object"),
        }
    }

    fn entity_check(&self, type_ref: UnitTypeRef) -> Option<EntityInfo> {
        if let UnitTypeRef::Indexed(type_index) = type_ref {
            let type_data = &self.type_data(type_index);

            if let TypeData {
                kind:
                    TypeKind::Object(ObjectData {
                        kind:
                            ObjectKind::Node(NodeData {
                                def_id: node_def_id,
                                entity_id: Some(id_def_id),
                                ..
                            }),
                        ..
                    }),
                ..
            } = type_data
            {
                Some(EntityInfo {
                    type_index,
                    node_def_id: *node_def_id,
                    id_def_id: *id_def_id,
                })
            } else {
                None
            }
        } else {
            None
        }
    }
}

struct EntityInfo {
    type_index: TypeIndex,
    node_def_id: DefId,
    id_def_id: DefId,
}
