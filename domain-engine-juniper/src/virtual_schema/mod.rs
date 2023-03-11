use std::sync::Arc;

use fnv::FnvHashMap;
use ontol_runtime::{env::Env, DefId, PackageId};
use tracing::debug;

use crate::{adapter::namespace::Namespace, virtual_schema::builder::QueryLevel, SchemaBuildError};

use self::{
    builder::VirtualSchemaBuilder,
    data::{ObjectData, ObjectKind, TypeData, TypeIndex, TypeKind, UnitTypeRef},
};

pub mod data;

mod builder;

/// The virtual schema is a schema representation
/// held as a "shadow" schema behind juniper's user-facing schema.
///
/// This comes in handy when performing lookahead on selection sets,
/// parsing field arguments, etc.
pub struct VirtualSchema {
    pub env: Arc<Env>,
    pub package_id: PackageId,
    pub query: TypeIndex,
    pub mutation: TypeIndex,
    pub types: Vec<TypeData>,
}

impl VirtualSchema {
    /// Builds a "shadow schema" before handing that over to juniper
    pub fn build(env: Arc<Env>, package_id: PackageId) -> Result<Self, SchemaBuildError> {
        let domain = env
            .find_domain(&package_id)
            .ok_or(SchemaBuildError::UnknownPackage)?;

        let mut schema = Self {
            env: env.clone(),
            package_id,
            query: TypeIndex(0),
            mutation: TypeIndex(0),
            types: vec![],
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

                if let Some((type_index, def_id)) = builder.schema.entity_check(type_ref) {
                    builder.add_entity_queries_and_mutations(type_index, def_id);
                }
            }
        }

        Ok(schema)
    }

    pub fn type_data(&self, index: TypeIndex) -> &TypeData {
        &self.types[index.0 as usize]
    }

    pub fn object_data_mut(&mut self, index: TypeIndex) -> &mut ObjectData {
        let type_data = self.types.get_mut(index.0 as usize).unwrap();
        match &mut type_data.kind {
            TypeKind::Object(object_data) => object_data,
            _ => panic!("{index:?} is not an object"),
        }
    }

    pub fn entity_check(&self, type_ref: UnitTypeRef) -> Option<(TypeIndex, DefId)> {
        if let UnitTypeRef::Indexed(type_index) = type_ref {
            if let TypeKind::Object(obj) = &self.type_data(type_index).kind {
                if let ObjectKind::Node(node) = &obj.kind {
                    if node.entity_id.is_some() {
                        Some((type_index, node.def_id))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}
