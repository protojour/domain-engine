use std::{collections::BTreeMap, sync::Arc};

use fnv::FnvHashMap;
use ontol_runtime::{DefId, PackageId, PropId};

use crate::{
    pg_model::{PgDataKey, PgRegKey},
    sql_value::SqlScalar,
    statement::PreparedStatement,
};

use super::insert::PreparedInsert;

#[derive(Default)]
pub struct PgCache {
    pub insert: FnvHashMap<(PackageId, DefId), PreparedInsert>,
    pub key_by_id: FnvHashMap<(DefId, PropId), PreparedStatement>,
    pub insert_tmp_id: FnvHashMap<(DefId, PropId), PreparedStatement>,
    pub upsert_self_identifying: FnvHashMap<DefId, PreparedStatement>,
    pub edge_update: FnvHashMap<Arc<String>, PreparedStatement>,

    pub tentative_foreign_keys:
        BTreeMap<(PropId, DefId), BTreeMap<SqlScalar, (PgRegKey, PgDataKey)>>,
}

impl PgCache {
    /// clear statements that are dependent on specific field selection
    pub fn clear_select_dependent(&mut self) {
        self.insert.clear();
    }
}
