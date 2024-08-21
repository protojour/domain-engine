use std::{collections::HashMap, sync::Arc};

use fnv::FnvHashMap;
use ontol_runtime::{DefId, PackageId, PropId};

use crate::statement::PreparedStatement;

use super::insert::PreparedInsert;

#[derive(Default)]
pub struct PgCache {
    pub insert: FnvHashMap<(PackageId, DefId), PreparedInsert>,
    pub key_by_id: FnvHashMap<(DefId, PropId), PreparedStatement>,
    pub upsert_self_identifying: FnvHashMap<DefId, PreparedStatement>,
    pub edge_update: HashMap<Arc<String>, PreparedStatement>,
}

impl PgCache {
    /// clear statements that are dependent on specific field selection
    pub fn clear_select_dependent(&mut self) {
        self.insert.clear();
    }
}
