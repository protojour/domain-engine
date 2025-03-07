use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, anyhow};
use deadpool_postgres::Object;
use fnv::FnvHashMap;
use ontol_runtime::{
    DefId, DefPropTag, DomainIndex, OntolDefTag,
    ontology::{aspects::DefsAspect, domain::DefKind},
    tuple::CardinalIdx,
};
use read_registry::read_registry;
use tokio_postgres::Transaction;
use tracing::{Instrument, debug_span, info};
use txn_wrapper::TxnWrapper;

use crate::{
    PgModel,
    pg_model::{
        DomainUid, PgDomain, PgEdgeCardinalKind, PgIndexType, PgPropertyData, PgTableIdUnion,
        PgType, RegVersion,
    },
};

mod registry {
    refinery::embed_migrations!("./m6mreg_migrations");
}

pub mod txn_wrapper;

mod domain_schemas;
mod execute;
mod read_registry;
mod steps;

/// NB: Changing this is likely a bad idea.
const MIGRATIONS_TABLE_NAME: &str = "public.m6mreg_schema_history";

#[derive(Clone, Copy)]
struct PgDomainIds {
    index: DomainIndex,
    uid: DomainUid,
}

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
enum Stage {
    Domain,
    Vertex,
    Edge,
}

struct MigrationCtx {
    /// The version of the registry
    current_version: RegVersion,
    /// The version of the domain schema model, stored in m6mreg.domain_migration.
    /// All changes to domain schemas have to be done through a migration in [domain_schemas].
    deployed_domain_schema_version: RegVersion,
    domains: FnvHashMap<DomainIndex, PgDomain>,
    stats: Stats,
    steps: Steps,
    abstract_scalars: FnvHashMap<DomainIndex, BTreeMap<OntolDefTag, PgType>>,
    next_schema_disambiguator: i32,
}

#[derive(Default)]
struct Stats {
    new_domains_deployed: usize,
    domains_already_deployed: usize,
    orphan_domains: usize,
}

#[derive(Default)]
struct Steps {
    steps: BTreeMap<Stage, Vec<(PgDomainIds, MigrationStep)>>,
}

impl Steps {
    pub fn extend(
        &mut self,
        stage: Stage,
        domain_ids: PgDomainIds,
        steps: impl IntoIterator<Item = MigrationStep>,
    ) {
        self.steps
            .entry(stage)
            .or_default()
            .extend(steps.into_iter().map(|step| (domain_ids, step)));
    }

    pub fn into_inner(self) -> BTreeMap<Stage, Vec<(PgDomainIds, MigrationStep)>> {
        self.steps
    }
}

/// The descructive steps that may be performed by the domain migration
#[derive(Debug)]
enum MigrationStep {
    DeployDomain {
        name: Box<str>,
        schema_name: Box<str>,
    },
    DeployVertex {
        vertex_def_id: DefId,
        table_name: Box<str>,
    },
    DeployVertexFKey {
        vertex_def_id: DefId,
    },
    DeployProperty {
        table_id: PgTableIdUnion,
        prop_tag: DefPropTag,
        data: PgPropertyData,
    },
    DeployPropertyIndex {
        table_id: PgTableIdUnion,
        index_def_id: DefId,
        index_type: PgIndexType,
        field_tuple: Vec<DefPropTag>,
    },
    DeployEdge {
        edge_tag: u16,
        table_name: Box<str>,
    },
    DeployEdgeCardinal {
        edge_tag: u16,
        index: CardinalIdx,
        ident: Box<str>,
        kind: PgEdgeCardinalKind,
        index_type: Option<PgIndexType>,
    },
    DeployCrdt {
        domain_index: DomainIndex,
    },
    RenameDomainSchema {
        old: Box<str>,
        new: Box<str>,
    },
    RenameDataTable {
        def_id: DefId,
        old: Box<str>,
        new: Box<str>,
    },
}

pub async fn migrate_registry(
    txn_wrapper: &mut TxnWrapper,
    db_name: &str,
) -> anyhow::Result<RegVersion> {
    info!(%db_name, "migrating registry");

    let mut runner = registry::migrations::runner();
    runner.set_migration_table_name(MIGRATIONS_TABLE_NAME);

    let mut migrations = runner
        .get_migrations()
        .iter()
        .filter(|mig| format!("{}", mig.prefix()) == "V")
        .collect::<Vec<_>>();
    migrations.sort();

    runner.run_async(txn_wrapper).await?;

    let reg_version = RegVersion::try_from(migrations.last().unwrap().version())
        .map_err(|ver| anyhow!("applied version not representable: {ver}"))?;

    assert_eq!(RegVersion::current(), reg_version);

    Ok(reg_version)
}

/// precondition: registry must be migrated first
pub async fn migrate_ontology(
    persistent_domains: &BTreeSet<DomainIndex>,
    ontology_defs: &DefsAspect,
    current_version: RegVersion,
    mut conn: Object,
) -> anyhow::Result<PgModel> {
    // initialize domain migration context
    let mut ctx = MigrationCtx {
        current_version,
        deployed_domain_schema_version: RegVersion::Init,
        domains: Default::default(),
        stats: Default::default(),
        steps: Default::default(),
        abstract_scalars: Default::default(),
        next_schema_disambiguator: 0,
    };

    // Migrate all the domains in a single transaction
    let txn = conn
        .build_transaction()
        .deferrable(false)
        .isolation_level(tokio_postgres::IsolationLevel::Serializable)
        .start()
        .await?;

    ctx.deployed_domain_schema_version = query_domain_migration_version(&txn).await?;

    let mut entity_id_to_entity = FnvHashMap::<DefId, DefId>::default();

    // read current state from registry (registry is already migrated)
    read_registry(ontology_defs, &mut ctx, &txn).await?;

    // apply PG changes to the domain schemas if needed
    domain_schemas::migrate_domain_schemas(&txn, &mut ctx).await?;

    // collect migration steps for persistent domains
    // this improves separation of concerns while also enabling dry run simulations
    for domain_index in [DomainIndex::ontol()].iter().chain(persistent_domains) {
        let domain = ontology_defs
            .domain_by_index(*domain_index)
            .ok_or_else(|| anyhow!("domain does not exist"))?;

        steps::migrate_domain_steps(*domain_index, domain, ontology_defs, &mut ctx)
            .instrument(debug_span!("migrate", uid = %domain.domain_id().ulid))
            .await
            .context("domain migration steps")?;

        for def in domain.defs() {
            if let DefKind::Entity(entity) = &def.kind {
                entity_id_to_entity.insert(entity.id_value_def_id, def.id);
            }
        }
    }

    // do actual migration based on changes to ontology
    execute::execute_domain_migration(&txn, &mut ctx)
        .await
        .context("perform migration")?;

    // sanity checks
    {
        assert_eq!(ctx.current_version, ctx.deployed_domain_schema_version);
        assert_eq!(
            ctx.current_version,
            query_domain_migration_version(&txn).await?
        );

        // check edge cardinals for multiple unique indexes (not handled)
        for (domain_index, pg_domain) in &ctx.domains {
            for (edge_tag, edgetable) in &pg_domain.edgetables {
                let unique_count = edgetable
                    .edge_cardinals
                    .values()
                    .filter(|c| matches!(c.index_type, Some(PgIndexType::Unique)))
                    .count();

                if unique_count > 1 {
                    let edge_id = DefId(*domain_index, *edge_tag);
                    return Err(anyhow!(
                        "edge {edge_id:?} has more than one unique cardinal, which doesn't work yet"
                    ));
                }
            }
        }
    }

    txn.commit().await?;

    info!(
        "{new} new domain(s) deployed, {existing} domain(s) were already deployed, {orphans} orphans",
        new = ctx.stats.new_domains_deployed,
        existing = ctx.stats.domains_already_deployed,
        orphans = ctx.stats.orphan_domains
    );

    Ok(PgModel::new(ctx.domains, entity_id_to_entity))
}

async fn query_domain_migration_version(txn: &Transaction<'_>) -> anyhow::Result<RegVersion> {
    let version = txn
        .query_one("SELECT version FROM m6mreg.domain_migration", &[])
        .await?
        .get::<_, i32>(0);

    RegVersion::try_from(u32::try_from(version)?)
        .map_err(|_| anyhow!("deployed version not representable"))
}
