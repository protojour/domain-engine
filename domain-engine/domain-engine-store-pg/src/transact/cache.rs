use fnv::FnvHashMap;
use ontol_runtime::{DefId, PackageId};

use super::insert::PreparedInsert;

#[derive(Default)]
pub struct StatementCache {
    pub insert: FnvHashMap<(PackageId, DefId), PreparedInsert>,
}

impl StatementCache {
    pub fn clear(&mut self) {
        self.insert.clear();
    }
}
