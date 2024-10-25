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

pub enum CompactionMessage {
    CrdtSaved {
        domain_index: DomainIndex,
        prop_key: PgRegKey,
        data_key: PgDataKey,
        chunk_id: i64,
    },
}

#[derive(Default)]
struct State {
    crdt: FnvHashMap<(DomainIndex, PgRegKey), CrdtCompactionState>,
}

struct CrdtCompactionState {
    compacted_watermark: i64,
    current_watermark: i64,
    data_keys: FnvHashSet<PgDataKey>,
}

pub async fn compaction_task(
    _compaction_tx: tokio::sync::mpsc::Sender<CompactionMessage>,
    mut compaction_rx: tokio::sync::mpsc::Receiver<CompactionMessage>,
    store: Arc<PostgresDataStore>,
) {
    let mut state = State::default();

    loop {
        let Some(msg) = compaction_rx.recv().await else {
            return;
        };

        match msg {
            CompactionMessage::CrdtSaved {
                domain_index,
                prop_key,
                data_key,
                chunk_id,
            } => {
                let crdt_state = state
                    .crdt
                    .entry((domain_index, prop_key))
                    .or_insert_with(|| CrdtCompactionState {
                        compacted_watermark: chunk_id,
                        current_watermark: chunk_id,
                        data_keys: Default::default(),
                    });

                crdt_state.current_watermark = i64::max(chunk_id, crdt_state.current_watermark);
                crdt_state.data_keys.insert(data_key);
                let diff = crdt_state.current_watermark - crdt_state.compacted_watermark;

                if diff >= store.crdt_compaction_threshold as i64 {
                    // currently do this from within the task,
                    // even if that may result in filling up the message channel
                    // and result in CRDT saves _waiting_ for compaction.
                    let data_keys = std::mem::take(&mut crdt_state.data_keys);

                    let result =
                        compact_crdts(domain_index, prop_key, data_keys, store.clone()).await;

                    match result {
                        Err(err) => {
                            error!("unable to compact: {err:?}");
                        }
                        Ok(Some(new_snapshot)) => {
                            crdt_state.compacted_watermark = new_snapshot;
                        }
                        Ok(None) => {}
                    }
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
            TransactionMode::ReadWriteAtomic,
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
