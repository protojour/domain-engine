use std::collections::BTreeMap;

use arcstr::ArcStr;
use domain_engine_core::{
    system::SystemAPI,
    transact::{TransactionMode, WriteStats, WriteStatsBuilder},
};
use fnv::FnvHashMap;
use ontol_runtime::{DefId, DomainIndex, PropId};

use crate::{
    pg_model::{PgDataKey, PgRegKey},
    sql_value::SqlScalar,
    statement::PreparedStatement,
};

use super::{InsertMode, insert::PreparedInsert};

/// A context type with mutability across one entire transaction
pub struct PgMutCtx {
    pub cache: PgCache,

    pub write_stats: WriteStatsBuilder,

    pub tentative_foreign_keys:
        BTreeMap<(PropId, DefId), BTreeMap<SqlScalar, (PgRegKey, PgDataKey)>>,
}

impl PgMutCtx {
    pub fn new(mode: TransactionMode, system: &dyn SystemAPI) -> Self {
        Self {
            cache: PgCache::default(),
            write_stats: WriteStats::builder(mode, system),
            tentative_foreign_keys: Default::default(),
        }
    }
}

/// Statement cache
#[derive(Default)]
pub struct PgCache {
    pub insert: FnvHashMap<(InsertMode, DomainIndex, DefId), PreparedInsert>,
    pub update_tentative: FnvHashMap<(DomainIndex, DefId), PreparedStatement>,
    pub key_by_id: FnvHashMap<(DefId, PropId), PreparedStatement>,
    pub insert_tmp_id: FnvHashMap<(DefId, PropId), PreparedStatement>,
    pub upsert_self_identifying: FnvHashMap<DefId, PreparedStatement>,

    pub edge_patch: FnvHashMap<ArcStr, PreparedStatement>,
}

impl PgCache {
    /// clear statements that are dependent on specific field selection
    pub fn clear_select_dependent(&mut self) {
        self.insert.clear();
    }
}
