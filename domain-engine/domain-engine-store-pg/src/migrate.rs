use std::collections::BTreeSet;

use anyhow::{anyhow, Context};
use fnv::FnvHashMap;
use indoc::indoc;
use ontol_runtime::{
    ontology::{
        domain::{Domain, Entity},
        Ontology,
    },
    DefId, PackageId, RelId,
};
use tokio_postgres::{Client, NoTls, Transaction};
use tracing::{debug_span, info, info_span, Instrument};

use crate::{sql::EscapeIdent, Unpack};

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

type PgSerial = i64;
type DomainUid = ulid::Ulid;

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
    domains: FnvHashMap<DomainUid, PgDomain>,
    vertices: FnvHashMap<(DomainUid, DefId), PgVertex>,
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
        id_rel_id: RelId,
        key_column_name: Box<str>,
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

#[derive(Clone)]
struct PgDomain {
    key: Option<PgSerial>,
    schema_name: Box<str>,
}

#[derive(Clone)]
#[allow(unused)]
struct PgVertex {
    table_name: Box<str>,
}

async fn migrate_domain_steps<'t>(
    domain: &Domain,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
    txn: &Transaction<'t>,
) -> anyhow::Result<()> {
    let domain_uid = domain.domain_id().ulid;
    let unique_name = &ontology[domain.unique_name()];
    let schema = format!("m6m_d_{unique_name}").into_boxed_str();

    let row = txn
        .query_opt(
            "SELECT key, schema_name FROM m6m_reg.domain WHERE uid = $1",
            &[&domain_uid],
        )
        .await?;

    if let Some(row) = row {
        info!("domain already deployed");

        let (key, schema_name) = row.unpack();

        let pg_domain = PgDomain {
            key: Some(key),
            schema_name,
        };
        ctx.domains.insert(domain_uid, pg_domain.clone());

        if pg_domain.schema_name != schema {
            ctx.steps.push((
                domain_uid,
                MigrationStep::RenameDomainSchema {
                    old: pg_domain.schema_name.clone(),
                    new: schema,
                },
            ));
        }
    } else {
        ctx.domains.insert(
            domain_uid,
            PgDomain {
                key: None,
                schema_name: schema.clone(),
            },
        );

        ctx.steps.push((
            domain_uid,
            MigrationStep::DeployDomain {
                name: ontology[domain.unique_name()].into(),
                schema_name: schema,
            },
        ));
    };

    for def in domain.defs() {
        let Some(entity) = def.entity() else {
            continue;
        };

        migrate_vertex_steps(domain_uid, def.id, entity, ontology, txn, ctx).await?;
    }

    Ok(())
}

async fn migrate_vertex_steps<'t>(
    domain_uid: DomainUid,
    vertex_def_id: DefId,
    entity: &Entity,
    ontology: &Ontology,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let name = &ontology[entity.name];
    let vertex_def_tag = vertex_def_id.1 as i32;
    let table_name = format!("v_{}", name).into_boxed_str();
    let key_column_name = "key".to_string().into_boxed_str();
    let domain_key = ctx.domain_key(&domain_uid);

    let row = if let Some(domain_key) = domain_key {
        txn.query_opt(
            indoc! { "
                SELECT key, table_name FROM m6m_reg.datatable
                WHERE domain_key = $1 AND def_tag = $2
            "},
            &[&domain_key, &vertex_def_tag],
        )
        .await?
    } else {
        None
    };

    let datatable_key: Option<PgSerial> = if let Some(row) = row {
        let (datatable_key, db_table_name) = row.unpack();
        let pg_vertex = PgVertex {
            table_name: db_table_name,
        };

        if pg_vertex.table_name != table_name {
            ctx.steps.push((
                domain_uid,
                MigrationStep::RenameDataTable {
                    def_id: vertex_def_id,
                    old: pg_vertex.table_name.clone(),
                    new: table_name.clone(),
                },
            ));
        }

        ctx.vertices.insert((domain_uid, vertex_def_id), pg_vertex);
        Some(datatable_key)
    } else {
        ctx.steps.push((
            domain_uid,
            MigrationStep::DeployVertex {
                vertex_def_id,
                table_name: table_name.clone(),
                id_rel_id: entity.id_relationship_id,
                key_column_name: key_column_name.clone(),
            },
        ));
        ctx.vertices
            .insert((domain_uid, vertex_def_id), PgVertex { table_name });
        None
    };

    if let Some(datatable_key) = datatable_key {
        // let id_rel_tag = entity.id_relationship_id.tag().0 as i32;

        let row = txn
            .query_opt(
                indoc! {"
                    SELECT key, column_name, rel_tag FROM m6m_reg.datafield
                    WHERE datatable_key = $1
                "},
                &[&datatable_key],
            )
            .await?;

        if let Some(row) = row {
            let _datafield_key: PgSerial = row.get(0);
            let column: Box<str> = row.get(1);
            let key_rel_tag: u16 = row.get::<_, i32>(2).try_into()?;

            if column != key_column_name {
                return Err(anyhow!("migrate key column change"));
            }

            if key_rel_tag != entity.id_relationship_id.tag().0 {
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
    for (domain_uid, step) in std::mem::take(&mut ctx.steps) {
        execute_migration_step(domain_uid, step, txn, ctx)
            .instrument(info_span!("migrate", uid = %domain_uid))
            .await?;
    }

    Ok(())
}

async fn execute_migration_step<'t>(
    domain_uid: DomainUid,
    step: MigrationStep,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    info!("{step:?}");

    match step {
        MigrationStep::DeployDomain { name, schema_name } => {
            // All the things owned by the domain will be isolated inside this schema.
            txn.query(&format!("CREATE SCHEMA {}", EscapeIdent(&schema_name)), &[])
                .await
                .context("create schema")?;

            let row = txn
                .query_one(
                    indoc! {"
                        INSERT INTO m6m_reg.domain (
                            uid,
                            name,
                            schema_name
                        ) VALUES($1, $2, $3)
                        RETURNING key
                    "},
                    &[&domain_uid, &name, &schema_name],
                )
                .await?;

            ctx.domains.get_mut(&domain_uid).unwrap().key = Some(row.get(0));
        }
        MigrationStep::DeployVertex {
            vertex_def_id,
            table_name,
            id_rel_id,
            key_column_name: key_column,
        } => {
            let vertex_def_tag = vertex_def_id.1 as i32;
            let pg_domain = ctx.domains.get(&domain_uid).unwrap();

            txn.query(
                &format!(
                    "CREATE TABLE {schema}.{table} (
                        {key_column} bytea NOT NULL UNIQUE,
                        data jsonb NOT NULL
                    )",
                    schema = EscapeIdent(&pg_domain.schema_name),
                    table = EscapeIdent(&table_name),
                    key_column = EscapeIdent(&key_column),
                ),
                &[],
            )
            .await
            .context("create vertex table")?;

            let row = txn
                .query_one(
                    indoc! { "
                        INSERT INTO m6m_reg.datatable (
                            domain_key,
                            def_domain_key,
                            def_tag,
                            table_name,
                            key_column
                        ) VALUES($1, $2, $3, $4, $5)
                        RETURNING key
                    "},
                    &[
                        &pg_domain.key,
                        &pg_domain.key,
                        &vertex_def_tag,
                        &table_name,
                        &"key",
                    ],
                )
                .await
                .context("insert datatable")?;

            let datatable_key: PgSerial = row.get(0);

            txn.query(
                indoc! { "
                    INSERT INTO m6m_reg.datafield (
                        datatable_key,
                        rel_tag,
                        column_name
                    ) VALUES($1, $2, $3)
                    RETURNING key
                "},
                &[&datatable_key, &(id_rel_id.tag().0 as i32), &key_column],
            )
            .await
            .context("insert vertex key")?;
        }
        MigrationStep::RenameDomainSchema { old, new } => {
            let domain_key = ctx.domain_key(&domain_uid).unwrap();
            let pg_domain = ctx.domains.get_mut(&domain_uid).unwrap();

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
                "UPDATE m6m_reg.domain SET(schema_name = $1) WHERE (key = $2)",
                &[&new, &domain_key],
            )
            .await
            .context("update domain schema name")?;

            pg_domain.schema_name = new;
        }
        MigrationStep::RenameDataTable {
            def_id,
            old: old_table,
            new: new_table,
        } => {
            let domain_key = ctx.domain_key(&domain_uid).unwrap();
            let def_tag = def_id.1 as i32;
            let pg_domain = ctx.domains.get_mut(&domain_uid).unwrap();
            let pg_vertex = ctx.vertices.get_mut(&(domain_uid, def_id)).unwrap();

            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{old} RENAME TO {schema}.{new}",
                    schema = EscapeIdent(&pg_domain.schema_name),
                    old = EscapeIdent(&old_table),
                    new = EscapeIdent(&new_table),
                ),
                &[],
            )
            .await?;

            txn.query(
                "UPDATE m6m_reg.datatable SET(table_name = $1) WHERE domain_key = $2 AND def_tag = $3",
                &[&new_table, &domain_key, &def_tag],
            )
            .await?;

            pg_vertex.table_name = new_table;
        }
    }

    Ok(())
}
