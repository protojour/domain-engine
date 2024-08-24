use std::collections::BTreeSet;

use anyhow::{anyhow, Context};
use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::{domain::DefKind, Ontology},
    tuple::CardinalIdx,
    DefId, DefPropTag, EdgeId, OntolDefTag, PackageId,
};
use tokio_postgres::{Client, Transaction};
use tracing::{debug_span, info, Instrument};

use crate::{
    pg_model::{
        DomainUid, PgDomain, PgEdgeCardinalKind, PgIndexType, PgPropertyData, PgTableIdUnion,
        RegVersion,
    },
    PgModel,
};

mod registry {
    refinery::embed_migrations!("./m6mreg_migrations");
}

mod execute;
mod steps;

/// NB: Changing this is likely a bad idea.
const MIGRATIONS_TABLE_NAME: &str = "public.m6mreg_schema_history";

#[derive(Clone, Copy)]
struct PgDomainIds {
    pkg_id: PackageId,
    uid: DomainUid,
}

struct MigrationCtx {
    current_version: RegVersion,
    deployed_version: RegVersion,
    domains: FnvHashMap<PackageId, PgDomain>,
    steps: Vec<(PgDomainIds, MigrationStep)>,
    abstract_scalars: FnvHashMap<PackageId, BTreeSet<OntolDefTag>>,
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

pub async fn migrate(
    persistent_domains: &BTreeSet<PackageId>,
    ontology: &Ontology,
    db_name: &str,
    pg_client: &mut Client,
) -> anyhow::Result<PgModel> {
    info!("migrating database `{db_name}`");

    // Migrate the registry
    let mut ctx = {
        let mut runner = registry::migrations::runner();
        runner.set_migration_table_name(MIGRATIONS_TABLE_NAME);
        let current_version =
            RegVersion::try_from(runner.get_migrations().last().unwrap().version() as i32)
                .map_err(|_| anyhow!("applied version not representable"))?;

        runner.run_async(pg_client).await?;

        let ctx = MigrationCtx {
            current_version,
            deployed_version: current_version,
            domains: Default::default(),
            steps: Default::default(),
            abstract_scalars: Default::default(),
        };
        assert_eq!(RegVersion::current(), ctx.current_version);
        ctx
    };

    // Migrate all the domains in a single transaction
    let txn = pg_client
        .build_transaction()
        .deferrable(false)
        .isolation_level(tokio_postgres::IsolationLevel::Serializable)
        .start()
        .await?;

    ctx.deployed_version = query_domain_migration_version(&txn).await?;

    let mut entity_id_to_entity = FnvHashMap::<DefId, DefId>::default();

    // collect migration steps
    // this improves separation of concerns while also enabling dry run simulations
    for package_id in persistent_domains {
        let domain = ontology
            .find_domain(*package_id)
            .ok_or_else(|| anyhow!("domain does not exist"))?;

        steps::migrate_domain_steps(*package_id, domain, ontology, &mut ctx, &txn)
            .instrument(debug_span!("migrate", id = %domain.domain_id().ulid))
            .await
            .context("domain migration steps")?;

        for def in domain.defs() {
            if let DefKind::Entity(entity) = &def.kind {
                entity_id_to_entity.insert(entity.id_value_def_id, def.id);
            }
        }
    }

    execute::execute_domain_migration(&txn, &mut ctx)
        .await
        .context("perform migration")?;

    // sanity checks
    {
        assert_eq!(ctx.current_version, ctx.deployed_version);
        assert_eq!(
            ctx.current_version,
            query_domain_migration_version(&txn).await?
        );

        // check edge cardinals for multiple unique indexes (not handled)
        for (pkg_id, pg_domain) in &ctx.domains {
            for (edge_tag, edgetable) in &pg_domain.edgetables {
                let unique_count = edgetable
                    .edge_cardinals
                    .values()
                    .filter(|c| matches!(c.index_type, Some(PgIndexType::Unique)))
                    .count();

                if unique_count > 1 {
                    let edge_id = EdgeId(*pkg_id, *edge_tag);
                    return Err(anyhow!(
                        "edge {edge_id:?} has more than one unique cardinal, which doesn't work yet"
                    ));
                }
            }
        }
    }

    txn.commit().await?;

    Ok(PgModel::new(ctx.domains, entity_id_to_entity))
}

async fn query_domain_migration_version<'t>(txn: &Transaction<'t>) -> anyhow::Result<RegVersion> {
    RegVersion::try_from(
        txn.query_one("SELECT version FROM m6mreg.domain_migration", &[])
            .await?
            .get::<_, i32>(0),
    )
    .map_err(|_| anyhow!("deployed version not representable"))
}
