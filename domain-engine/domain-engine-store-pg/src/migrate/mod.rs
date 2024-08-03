use std::collections::BTreeSet;

use anyhow::{anyhow, Context};
use fnv::FnvHashMap;
use ontol_runtime::{ontology::Ontology, DefId, DefRelTag, PackageId};
use tokio_postgres::{Client, NoTls, Transaction};
use tracing::{debug_span, info, Instrument};

use crate::pg_model::{DefUid, DomainUid, PgDomain, PgSerial, RegVersion};

mod registry {
    refinery::embed_migrations!("./registry_migrations");
}

mod execute;
mod steps;

/// NB: Changing this is likely a bad idea.
const MIGRATIONS_TABLE_NAME: &str = "public.m6m_registry_schema_history";

struct MigrationCtx {
    current_version: RegVersion,
    deployed_version: RegVersion,
    domains: FnvHashMap<DomainUid, PgDomain>,
    steps: Vec<(DomainUid, MigrationStep)>,
}

impl MigrationCtx {
    pub fn domain_key(&self, uid: &DomainUid) -> Option<PgSerial> {
        self.domains.get(uid).and_then(|domain| domain.key)
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
    DeployDataField {
        datatable_def_id: DefId,
        rel_tag: DefRelTag,
        column_name: Box<str>,
    },
    RenameDomainSchema {
        old: Box<str>,
        new: Box<str>,
    },
    RenameDataTable {
        def_uid: DefUid,
        old: Box<str>,
        new: Box<str>,
    },
}

pub async fn connect_and_migrate(
    persistent_domains: &BTreeSet<PackageId>,
    ontology: &Ontology,
    pg_config: &tokio_postgres::Config,
) -> anyhow::Result<()> {
    let db_name = pg_config.get_dbname().unwrap();
    let (mut client, connection) = pg_config.connect(NoTls).await.unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    let join_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    migrate(persistent_domains, ontology, db_name, &mut client).await?;
    drop(client);

    join_handle.await.unwrap();

    Ok(())
}

async fn migrate(
    persistent_domains: &BTreeSet<PackageId>,
    ontology: &Ontology,
    db_name: &str,
    pg_client: &mut Client,
) -> anyhow::Result<()> {
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

    // collect migration steps
    // this improves separation of concerns while also enabling dry run simulations
    for package_id in persistent_domains {
        let domain = ontology
            .find_domain(*package_id)
            .ok_or_else(|| anyhow!("domain does not exist"))?;

        steps::migrate_domain_steps(domain, ontology, &mut ctx, &txn)
            .instrument(debug_span!("migrate", id = %domain.domain_id().ulid))
            .await
            .context("domain migration steps")?;
    }

    execute::execute_domain_migration(&txn, &mut ctx)
        .await
        .context("perform migration")?;

    // sanity check
    {
        assert_eq!(ctx.current_version, ctx.deployed_version);
        assert_eq!(
            ctx.current_version,
            query_domain_migration_version(&txn).await?
        );
    }

    txn.commit().await?;

    Ok(())
}

async fn query_domain_migration_version<'t>(txn: &Transaction<'t>) -> anyhow::Result<RegVersion> {
    RegVersion::try_from(
        txn.query_one("SELECT version FROM m6m_reg.domain_migration", &[])
            .await?
            .get::<_, i32>(0),
    )
    .map_err(|_| anyhow!("deployed version not representable"))
}
