use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};

use crate::{DefId, PackageId};

use super::{
    data::{TypeData, TypeIndex},
    QueryLevel,
};

#[derive(Serialize, Deserialize)]
pub struct GraphqlSchema {
    pub package_id: PackageId,
    pub query: TypeIndex,
    pub mutation: TypeIndex,
    pub types: Vec<TypeData>,
    pub type_index_by_def: FnvHashMap<(DefId, QueryLevel), TypeIndex>,
}

impl GraphqlSchema {
    pub fn type_data(&self, type_index: TypeIndex) -> &TypeData {
        &self.types[type_index.0 as usize]
    }
}
