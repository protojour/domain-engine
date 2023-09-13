use fnv::FnvHashMap;

use crate::{DefId, PackageId};

use super::{
    data::{TypeData, TypeIndex},
    QueryLevel,
};

pub struct GraphqlSchema {
    pub(super) package_id: PackageId,
    pub(super) query: TypeIndex,
    pub(super) mutation: TypeIndex,
    pub(super) types: Vec<TypeData>,
    pub(super) type_index_by_def: FnvHashMap<(DefId, QueryLevel), TypeIndex>,
}
