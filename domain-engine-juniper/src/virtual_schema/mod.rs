use std::sync::Arc;

use ontol_runtime::{
    env::Env,
    serde::processor::{ProcessorLevel, ProcessorMode},
    DefId,
};

use crate::type_info::GraphqlTypeName;

use self::data::{TypeData, TypeIndex};

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
        &self.type_data().typename
    }
}

struct EntityInfo {
    type_index: TypeIndex,
    node_def_id: DefId,
    id_def_id: DefId,
}
