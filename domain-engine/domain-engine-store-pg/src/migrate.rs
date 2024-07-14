use anyhow::{anyhow, Context};
use fnv::FnvHashMap;
use indoc::indoc;
use ontol_runtime::{
    ontology::{domain::Entity, Ontology},
    DefId, PackageId,
};
use tokio_postgres::{Client, NoTls, Transaction};
use tracing::{debug_span, info, info_span, Instrument};

use crate::sql::EscapeIdent;

mod registry {
    use refinery::embed_migrations;
    embed_migrations!("./registry_migrations");
}

/// NB: Changing this is likely a bad idea.
const MIGRATIONS_TABLE_NAME: &str = "public.m6m_registry_schema_history";

type DomainId = ulid::Ulid;

pub async fn connect_and_migrate(
    persistent_domains: &[PackageId],
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
    persistent_domains: &[PackageId],
    ontology: &Ontology,
    db_name: &str,
    pg_client: &mut Client,
) -> anyhow::Result<()> {
    info!("migrating database `{db_name}`");

    registry::migrations::runner()
        .set_migration_table_name(MIGRATIONS_TABLE_NAME)
        .run_async(pg_client)
        .await?;

    // Migrate all the domains in a single transaction
    let txn = pg_client
        .build_transaction()
        .deferrable(false)
        .isolation_level(tokio_postgres::IsolationLevel::Serializable)
        .start()
        .await?;

    let mut ctx = MigrationCtx::default();

    // collect migration steps
    // this improves separation of concerns while also enabling dry run simulations
    for package_id in persistent_domains {
        migrate_domain_steps(*package_id, ontology, &mut ctx, &txn)
            .instrument(debug_span!("migrate", pkg = package_id.id()))
            .await
            .context("domain migration steps")?;
    }

    execute_domain_migration(&txn, &mut ctx)
        .await
        .context("perform migration")?;

    txn.commit().await?;

    Ok(())
}

#[derive(Default)]
struct MigrationCtx {
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
        def_id: DefId,
        table: Box<str>,
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
    package_id: PackageId,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
    txn: &Transaction<'t>,
) -> anyhow::Result<()> {
    let domain = ontology
        .find_domain(package_id)
        .ok_or_else(|| anyhow!("domain does not exist"))?;

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
        let domain_id = DomainId::new();
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
    def_id: DefId,
    entity: &Entity,
    ontology: &Ontology,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let name = &ontology[entity.name];
    let def_tag = def_id.1 as i32;
    let table = format!("v_{}", name).into_boxed_str();

    let row = txn
        .query_opt(
            indoc! { "
                SELECT \"table\" FROM m6m_reg.vertex
                WHERE
                    domain_id = $1
                AND
                    def_tag = $2
            "},
            &[&domain_id, &def_tag],
        )
        .await?;

    if let Some(row) = row {
        let pg_vertex = PgVertex { table: row.get(0) };

        if pg_vertex.table != table {
            ctx.steps.push((
                domain_id,
                MigrationStep::RenameVertexTable {
                    def_id,
                    old: pg_vertex.table.clone(),
                    new: table.clone(),
                },
            ));
        }

        ctx.vertices.insert((domain_id, def_id), pg_vertex);
    } else {
        ctx.steps.push((
            domain_id,
            MigrationStep::DeployVertex {
                def_id,
                table: table.clone(),
            },
        ));
        ctx.vertices.insert((domain_id, def_id), PgVertex { table });
    }

    Ok(())
}

async fn execute_domain_migration<'t>(
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    for (domain_id, step) in std::mem::take(&mut ctx.steps) {
        execute_migration_step(domain_id, step, txn, ctx)
            .instrument(info_span!("migrate", domain_id = %domain_id))
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
        MigrationStep::DeployVertex { def_id, table } => {
            let def_tag = def_id.1 as i32;
            let pg_domain = ctx.domains.get(&domain_id).unwrap();

            txn.query(
                &format!(
                    "CREATE TABLE {}.{} ()",
                    EscapeIdent(&pg_domain.schema),
                    EscapeIdent(&table)
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
                &[&domain_id, &def_tag, &table],
            )
            .await
            .context("insert vertex")?;
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
                "UPDATE m6m_reg.vertice SET(table = $1) WHERE domain_id = $2 AND def_tag = $3",
                &[&new_table, &domain_id, &def_tag],
            )
            .await?;

            pg_vertex.table = new_table;
        }
    }

    Ok(())
}
