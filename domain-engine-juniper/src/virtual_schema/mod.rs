use std::sync::Arc;

use ontol_runtime::{
    env::Env,
    serde::processor::{ProcessorLevel, ProcessorMode},
    DefId,
};

use crate::type_info::GraphqlTypeName;

use self::data::{ObjectData, ObjectKind, TypeData, TypeIndex};

pub mod data;

mod builder;
mod namespace;
mod schema;

pub use schema::VirtualSchema;

#[derive(Clone)]
pub struct VirtualIndexedTypeInfo {
    pub virtual_schema: Arc<VirtualSchema>,
    pub type_index: TypeIndex,
    pub mode: ProcessorMode,
    pub level: ProcessorLevel,
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
        let type_data = &self.type_data();
        match self.mode {
            ProcessorMode::Select => &type_data.typename,
            _ => match &type_data.kind {
                data::TypeKind::Object(ObjectData {
                    kind: ObjectKind::Node(node),
                    ..
                }) => &node.input_type_name,
                _ => panic!(
                    "No {mode:?} typename available for this type_data",
                    mode = self.mode
                ),
            },
        }
    }
}

struct EntityInfo {
    type_index: TypeIndex,
    node_def_id: DefId,
    id_def_id: DefId,
}
