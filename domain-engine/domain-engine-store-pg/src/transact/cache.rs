use std::collections::BTreeMap;

use arcstr::ArcStr;
use fnv::FnvHashMap;
use ontol_runtime::{DefId, PackageId, PropId};

use crate::{
    pg_model::{PgDataKey, PgRegKey},
    sql_value::SqlScalar,
    statement::PreparedStatement,
};

use super::{insert::PreparedInsert, InsertMode};

#[derive(Default)]
pub struct PgCache {
    pub insert: FnvHashMap<(InsertMode, PackageId, DefId), PreparedInsert>,
    pub update_tentative: FnvHashMap<(PackageId, DefId), PreparedStatement>,
    pub key_by_id: FnvHashMap<(DefId, PropId), PreparedStatement>,
    pub insert_tmp_id: FnvHashMap<(DefId, PropId), PreparedStatement>,
    pub upsert_self_identifying: FnvHashMap<DefId, PreparedStatement>,

    pub edge_patch: FnvHashMap<ArcStr, PreparedStatement>,

    pub tentative_foreign_keys:
        BTreeMap<(PropId, DefId), BTreeMap<SqlScalar, (PgRegKey, PgDataKey)>>,
}

impl PgCache {
    /// clear statements that are dependent on specific field selection
    pub fn clear_select_dependent(&mut self) {
        self.insert.clear();
    }
}
