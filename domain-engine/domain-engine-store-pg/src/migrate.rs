use std::collections::BTreeSet;

use anyhow::{anyhow, Context};
use fnv::FnvHashMap;
use indoc::indoc;
use ontol_runtime::{
    ontology::{
        domain::{Domain, Entity},
        Ontology,
    },
    DefId, PackageId,
};
use tokio_postgres::{Client, NoTls, Transaction};
use tracing::{debug_span, info, info_span, Instrument};

use crate::sql::EscapeIdent;

mod registry {
    refinery::embed_migrations!("./registry_migrations");
}

/// NB: Changing this is likely a bad idea.
const MIGRATIONS_TABLE_NAME: &str = "public.m6m_registry_schema_history";

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(i32)]
enum RegVersion {
    Init = 1,
}

impl RegVersion {
    const fn current() -> Self {
        Self::Init
    }
}

impl TryFrom<i32> for RegVersion {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Init),
            _ => Err(()),
        }
    }
}

type DomainId = ulid::Ulid;

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
            vertices: Default::default(),
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

        migrate_domain_steps(domain, ontology, &mut ctx, &txn)
            .instrument(debug_span!("migrate", id = %domain.domain_id().ulid))
            .await
            .context("domain migration steps")?;
    }

    execute_domain_migration(&txn, &mut ctx)
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

struct MigrationCtx {
    current_version: RegVersion,
    deployed_version: RegVersion,
    domains: FnvHashMap<DomainId, PgDomain>,
    vertices: FnvHashMap<(DomainId, DefId), PgVertex>,
    steps: Vec<(DomainId, MigrationStep)>,
}

/// The descructive steps that may be performed by the domain migration
#[derive(Debug)]
enum MigrationStep {
    DeployDomain {
        name: Box<str>,
        schema: Box<str>,
    },
    DeployVertex {
        vertex_def_id: DefId,
        table: Box<str>,
        key_def_id: DefId,
        key_column: Box<str>,
    },
    RenameDomainSchema {
        old: Box<str>,
        new: Box<str>,
    },
    RenameVertexTable {
        def_id: DefId,
        old: Box<str>,
        new: Box<str>,
    },
}

#[derive(Clone)]
struct PgDomain {
    schema: Box<str>,
}

#[derive(Clone)]
#[allow(unused)]
struct PgVertex {
    table: Box<str>,
}

async fn migrate_domain_steps<'t>(
    domain: &Domain,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
    txn: &Transaction<'t>,
) -> anyhow::Result<()> {
    let unique_name = &ontology[domain.unique_name()];
    let schema = format!("m6m_d_{unique_name}").into_boxed_str();

    let row = txn
        .query_opt(
            "SELECT domain_id, schema FROM m6m_reg.domain WHERE name = $1",
            &[&unique_name],
        )
        .await?;

    let domain_id = if let Some(row) = row {
        info!("domain already deployed");

        let domain_id: DomainId = row.get(0);

        let pg_domain = PgDomain { schema: row.get(1) };
        ctx.domains.insert(domain_id, pg_domain.clone());

        if pg_domain.schema != schema {
            ctx.steps.push((
                domain_id,
                MigrationStep::RenameDomainSchema {
                    old: pg_domain.schema.clone(),
                    new: schema,
                },
            ));
        }

        domain_id
    } else {
        let domain_id = domain.domain_id().ulid;
        ctx.domains.insert(
            domain_id,
            PgDomain {
                schema: schema.clone(),
            },
        );

        ctx.steps.push((
            domain_id,
            MigrationStep::DeployDomain {
                name: ontology[domain.unique_name()].into(),
                schema,
            },
        ));

        domain_id
    };

    for def in domain.defs() {
        let Some(entity) = def.entity() else {
            continue;
        };

        migrate_vertex_steps(domain_id, def.id, entity, ontology, txn, ctx).await?;
    }

    Ok(())
}

async fn migrate_vertex_steps<'t>(
    domain_id: DomainId,
    vertex_def_id: DefId,
    entity: &Entity,
    ontology: &Ontology,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let name = &ontology[entity.name];
    let vertex_def_tag = vertex_def_id.1 as i32;
    let table = format!("v_{}", name).into_boxed_str();
    let key_column = "key".to_string().into_boxed_str();

    let row = txn
        .query_opt(
            indoc! { "
                SELECT \"table\" FROM m6m_reg.vertex
                WHERE domain_id = $1 AND def_tag = $2
            "},
            &[&domain_id, &vertex_def_tag],
        )
        .await?;

    if let Some(row) = row {
        let pg_vertex = PgVertex { table: row.get(0) };

        if pg_vertex.table != table {
            ctx.steps.push((
                domain_id,
                MigrationStep::RenameVertexTable {
                    def_id: vertex_def_id,
                    old: pg_vertex.table.clone(),
                    new: table.clone(),
                },
            ));
        }

        ctx.vertices.insert((domain_id, vertex_def_id), pg_vertex);
    } else {
        ctx.steps.push((
            domain_id,
            MigrationStep::DeployVertex {
                vertex_def_id,
                table: table.clone(),
                key_def_id: entity.id_value_def_id,
                key_column: key_column.clone(),
            },
        ));
        ctx.vertices
            .insert((domain_id, vertex_def_id), PgVertex { table });
    }

    {
        let key_def_tag = entity.id_value_def_id.1 as i32;

        let row = txn
            .query_opt(
                indoc! { "
                    SELECT \"column\", key_def_tag FROM m6m_reg.vertex_key
                    WHERE domain_id = $1 AND vertex_def_tag = $2 AND key_def_tag = $3
                "},
                &[&domain_id, &vertex_def_tag, &key_def_tag],
            )
            .await?;

        if let Some(row) = row {
            let column: Box<str> = row.get(0);
            let key_def_tag: u16 = row.get::<_, i32>(1).try_into()?;

            if column != key_column {
                return Err(anyhow!("migrate key column change"));
            }

            if key_def_tag != entity.id_value_def_id.1 {
                return Err(anyhow!("key def tag has changed"));
            }
        }
    }

    Ok(())
}

async fn execute_domain_migration<'t>(
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    for (domain_id, step) in std::mem::take(&mut ctx.steps) {
        execute_migration_step(domain_id, step, txn, ctx)
            .instrument(info_span!("migrate", id = %domain_id))
            .await?;
    }

    Ok(())
}

async fn execute_migration_step<'t>(
    domain_id: DomainId,
    step: MigrationStep,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    info!("{step:?}");

    match step {
        MigrationStep::DeployDomain { name, schema } => {
            // All the things owned by the domain will be isolated inside this schema.
            txn.query(&format!("CREATE SCHEMA {}", EscapeIdent(&schema)), &[])
                .await
                .context("create schema")?;

            txn.query(
                indoc! { "
                    INSERT INTO m6m_reg.domain (
                        domain_id,
                        name,
                        schema
                    ) VALUES($1, $2, $3)
                "},
                &[&domain_id, &name, &schema],
            )
            .await?;
        }
        MigrationStep::DeployVertex {
            vertex_def_id,
            table,
            key_def_id,
            key_column,
        } => {
            let vertex_def_tag = vertex_def_id.1 as i32;
            let key_def_tag = key_def_id.1 as i32;
            let pg_domain = ctx.domains.get(&domain_id).unwrap();

            txn.query(
                &format!(
                    "CREATE TABLE {schema}.{table} (
                        {key_column} bytea NOT NULL UNIQUE,
                        data jsonb NOT NULL
                    )",
                    schema = EscapeIdent(&pg_domain.schema),
                    table = EscapeIdent(&table),
                    key_column = EscapeIdent(&key_column),
                ),
                &[],
            )
            .await
            .context("create vertex table")?;

            txn.query(
                indoc! { "
                    INSERT INTO m6m_reg.vertex (
                        domain_id,
                        def_tag,
                        \"table\"
                    ) VALUES($1, $2, $3)
                "},
                &[&domain_id, &vertex_def_tag, &table],
            )
            .await
            .context("insert vertex")?;

            txn.query(
                indoc! { "
                    INSERT INTO m6m_reg.vertex_key (
                        domain_id,
                        vertex_def_tag,
                        key_def_tag,
                        \"column\"
                    ) VALUES($1, $2, $3, $4)
                "},
                &[&domain_id, &vertex_def_tag, &key_def_tag, &key_column],
            )
            .await
            .context("insert vertex key")?;
        }
        MigrationStep::RenameDomainSchema { old, new } => {
            txn.query(
                &format!(
                    "ALTER SCHEMA {old} RENAME TO {new}",
                    old = EscapeIdent(&old),
                    new = EscapeIdent(&new),
                ),
                &[],
            )
            .await?;

            txn.query(
                "UPDATE m6m_reg.domain SET(schema = $1) WHERE (domain_id = $2)",
                &[&new, &domain_id],
            )
            .await?;

            ctx.domains.get_mut(&domain_id).unwrap().schema = new;
        }
        MigrationStep::RenameVertexTable {
            def_id,
            old: old_table,
            new: new_table,
        } => {
            let def_tag = def_id.1 as i32;
            let pg_domain = ctx.domains.get_mut(&domain_id).unwrap();
            let pg_vertex = ctx.vertices.get_mut(&(domain_id, def_id)).unwrap();

            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{old} RENAME TO {schema}.{new}",
                    schema = EscapeIdent(&pg_domain.schema),
                    old = EscapeIdent(&old_table),
                    new = EscapeIdent(&new_table),
                ),
                &[],
            )
            .await?;

            txn.query(
                "UPDATE m6m_reg.vertex SET(table = $1) WHERE domain_id = $2 AND def_tag = $3",
                &[&new_table, &domain_id, &def_tag],
            )
            .await?;

            pg_vertex.table = new_table;
        }
    }

    Ok(())
}
