use std::sync::Arc;

use automerge::Automerge;
use domain_engine_core::{transact::TransactionMode, DomainResult};
use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::DomainIndex;
use tracing::{debug, error, info};

use crate::{
    pg_error::PgError,
    pg_model::{PgDataKey, PgRegKey},
    sql,
    transact::{ConnectionState, TransactCtx},
    PostgresDataStore,
};

const DEFAULT_CRDT_COMPACTION_THRESHOLD: u32 = 64;

pub struct CompactionCtx {
    state: std::sync::Mutex<State>,
    pub(crate) crdt_compaction_threshold: u32,
    trigger: tokio::sync::watch::Sender<()>,
    watcher: tokio::sync::watch::Receiver<()>,
}

#[derive(Default)]
struct State {
    crdt: FnvHashMap<(DomainIndex, PgRegKey), CrdtCompactionState>,
}

/// Compaction tracking state for a CRDT table.
/// It is used to keep track of when to _trigger_ compaction.
struct CrdtCompactionState {
    /// The "watermark", i.e. largest chunk id that the compaction ctx has seen
    observed_watermark: i64,
    /// The level of the watermark last time compaction was triggered
    triggered_watermark: i64,
    /// The watermark of the most recent compacted snapshot-chunk that has been written
    compacted_watermark: i64,
    data_keys: FnvHashSet<PgDataKey>,
}

impl Default for CompactionCtx {
    fn default() -> Self {
        let (trigger, watcher) = tokio::sync::watch::channel(());
        Self {
            state: Default::default(),
            crdt_compaction_threshold: DEFAULT_CRDT_COMPACTION_THRESHOLD,
            trigger,
            watcher,
        }
    }
}

impl CompactionCtx {
    pub fn signal_crdt_saved(
        &self,
        domain_index: DomainIndex,
        prop_key: PgRegKey,
        data_key: PgDataKey,
        chunk_id: i64,
    ) {
        let do_trigger_compaction: bool = {
            let mut state = self.state.lock().unwrap();
            let crdt_state = state
                .crdt
                .entry((domain_index, prop_key))
                .or_insert_with(|| CrdtCompactionState {
                    compacted_watermark: chunk_id,
                    triggered_watermark: chunk_id,
                    observed_watermark: chunk_id,
                    data_keys: Default::default(),
                });

            crdt_state.observed_watermark = i64::max(chunk_id, crdt_state.observed_watermark);
            crdt_state.data_keys.insert(data_key);
            let diff = crdt_state.observed_watermark - crdt_state.triggered_watermark;

            if diff >= self.crdt_compaction_threshold as i64 {
                // prevent this triggering again for a while
                crdt_state.triggered_watermark = crdt_state.observed_watermark;
                true
            } else {
                false
            }
        };

        if do_trigger_compaction {
            self.trigger.send(()).unwrap();
        }
    }
}

pub async fn compaction_task(store: Arc<PostgresDataStore>) {
    let mut watcher = store.compaction_ctx.watcher.clone();

    watcher.mark_unchanged();
    loop {
        watcher.changed().await.unwrap();

        let mut compaction_tasks: Vec<(DomainIndex, PgRegKey, FnvHashSet<PgDataKey>)> = vec![];

        // lock and collect compactions to perform
        {
            let mut state = store.compaction_ctx.state.lock().unwrap();
            for ((domain_index, prop_key), crdt_state) in &mut state.crdt {
                let diff = crdt_state.triggered_watermark - crdt_state.compacted_watermark;

                if diff > store.compaction_ctx.crdt_compaction_threshold as i64 {
                    compaction_tasks.push((
                        *domain_index,
                        *prop_key,
                        std::mem::take(&mut crdt_state.data_keys),
                    ));
                }
            }
        }

        let mut compacted_watermarks: Vec<(DomainIndex, PgRegKey, i64)> = vec![];

        // run compaction
        for (domain_index, prop_key, data_keys) in compaction_tasks {
            let result = compact_crdts(domain_index, prop_key, data_keys, store.clone()).await;

            match result {
                Err(err) => {
                    error!("unable to compact: {err:?}");
                }
                Ok(Some(new_snapshot)) => {
                    compacted_watermarks.push((domain_index, prop_key, new_snapshot));
                }
                Ok(None) => {}
            }
        }

        // lock and write back compacted watermarks
        {
            let mut state = store.compaction_ctx.state.lock().unwrap();
            for (domain_index, prop_key, compacted_watermark) in compacted_watermarks {
                if let Some(crdt_state) = state.crdt.get_mut(&(domain_index, prop_key)) {
                    crdt_state.compacted_watermark = compacted_watermark;
                }
            }
        }
    }
}

async fn compact_crdts(
    domain_index: DomainIndex,
    prop_key: PgRegKey,
    data_keys: FnvHashSet<PgDataKey>,
    store: Arc<PostgresDataStore>,
) -> DomainResult<Option<i64>> {
    let mut new_watermark: Option<i64> = None;

    let pg_domain = store.pg_model.pg_domain(domain_index)?;

    for data_key in data_keys {
        let connection = store
            .pool
            .get()
            .await
            .map_err(|e| PgError::DbConnectionAcquire(e.into()))?;

        let ctx = TransactCtx::new(
            TransactionMode::ReadWrite,
            ConnectionState::NonAtomic(connection),
            &store,
        );

        let automerge = {
            let Some(input) = ctx.crdt_get(pg_domain, prop_key, data_key).await? else {
                continue;
            };
            let Ok(automerge) = Automerge::load(&input) else {
                error!("cannot load CRDT for compaction");
                continue;
            };
            automerge
        };

        let compacted = automerge.save();

        let new_snapshot = ctx
            .save_crdt_raw(pg_domain, prop_key, data_key, "snapshot", &compacted)
            .await?;

        debug!("new snapshot: {new_snapshot}");

        let count = ctx
            .garbage_collect_crdts(pg_domain, prop_key, data_key, new_snapshot)
            .await?;

        new_watermark = Some(new_snapshot);

        info!("garbage collected {count} CRDT rows");
    }

    // vacuum
    let connection = store
        .pool
        .get()
        .await
        .map_err(|e| PgError::DbConnectionAcquire(e.into()))?;

    let vacuum = sql::Vacuum {
        tables: vec![sql::TableName(&pg_domain.schema_name, "crdt")],
    };
    let sql = vacuum.to_string();
    debug!("{sql}");

    connection.query(&sql, &[]).await.map_err(PgError::Vacuum)?;

    Ok(new_watermark)
}
