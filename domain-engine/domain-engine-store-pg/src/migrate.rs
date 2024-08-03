use std::collections::BTreeSet;

use anyhow::{anyhow, Context};
use fnv::FnvHashMap;
use indoc::indoc;
use itertools::Itertools;
use ontol_runtime::{
    ontology::{
        domain::{DataRelationshipKind, Def, Domain, Entity},
        Ontology,
    },
    DefId, DefRelTag, PackageId,
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

#[derive(Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
struct DefUid(DomainUid, u16);

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

#[derive(Clone)]
struct PgDomain {
    key: Option<PgSerial>,
    schema_name: Box<str>,
    datatables: FnvHashMap<DefUid, PgDataTable>,
}

#[derive(Clone)]
struct PgDataTable {
    key: PgSerial,
    table_name: Box<str>,
    data_fields: FnvHashMap<DefRelTag, PgDataField>,
}

#[derive(Clone)]
struct PgDataField {
    column_name: Box<str>,
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

        let (domain_key, schema_name) = row.unpack();

        let pg_datatables = txn
            .query(
                indoc! {"
                    SELECT key, def_domain_key, def_tag, table_name
                    FROM m6m_reg.datatable
                    WHERE domain_key = $1
                "},
                &[&domain_key],
            )
            .await?
            .into_iter()
            .map(|row| -> anyhow::Result<_> {
                let key: PgSerial = row.get(0);
                let def_domain_key: PgSerial = row.get(1);
                let def_tag: u16 = row.get::<_, i32>(2).try_into()?;
                let table_name: Box<str> = row.get(3);

                // for now
                assert_eq!(def_domain_key, domain_key);

                Ok((
                    DefUid(domain_uid, def_tag),
                    PgDataTable {
                        key,
                        table_name,
                        data_fields: Default::default(),
                    },
                ))
            })
            .try_collect()?;

        let pg_domain = PgDomain {
            key: Some(domain_key),
            schema_name,
            datatables: pg_datatables,
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
                datatables: Default::default(),
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

        migrate_vertex_steps(domain_uid, def.id, def, entity, ontology, txn, ctx).await?;
    }

    Ok(())
}

async fn migrate_vertex_steps<'t>(
    domain_uid: DomainUid,
    vertex_def_id: DefId,
    def: &Def,
    entity: &Entity,
    ontology: &Ontology,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let name = &ontology[entity.name];
    let table_name = format!("v_{}", name).into_boxed_str();
    let pg_domain = ctx.domains.get_mut(&domain_uid).unwrap();
    let def_uid = DefUid(domain_uid, vertex_def_id.1);

    if let Some(datatable) = pg_domain.datatables.get_mut(&def_uid) {
        let pg_vertex = PgVertex {
            table_name: datatable.table_name.clone(),
        };

        let pg_fields = txn
            .query(
                indoc! {"
                        SELECT key, rel_tag, column_name
                        FROM m6m_reg.datafield
                        WHERE datatable_key = $1
                    "},
                &[&datatable.key],
            )
            .await
            .context("read datatables")?
            .into_iter()
            .map(|row| -> anyhow::Result<_> {
                let _key: PgSerial = row.get(0);
                let rel_tag = DefRelTag(row.get::<_, i32>(1).try_into()?);
                let column_name: Box<str> = row.get(2);

                Ok((rel_tag, PgDataField { column_name }))
            })
            .try_collect()?;

        if pg_vertex.table_name != table_name {
            ctx.steps.push((
                domain_uid,
                MigrationStep::RenameDataTable {
                    def_uid,
                    old: pg_vertex.table_name.clone(),
                    new: table_name.clone(),
                },
            ));
        }

        ctx.vertices.insert((domain_uid, vertex_def_id), pg_vertex);
        datatable.data_fields = pg_fields;
    } else {
        ctx.steps.push((
            domain_uid,
            MigrationStep::DeployVertex {
                vertex_def_id,
                table_name: table_name.clone(),
            },
        ));
        ctx.vertices
            .insert((domain_uid, vertex_def_id), PgVertex { table_name });
    }

    let mut tree_relationships: Vec<_> = def
        .data_relationships
        .iter()
        .filter_map(|(rel_id, rel)| match &rel.kind {
            DataRelationshipKind::Id | DataRelationshipKind::Tree => Some((rel_id.tag(), rel)),
            DataRelationshipKind::Edge(_) => None,
        })
        .collect_vec();

    tree_relationships.sort_by_key(|(rel_tag, _)| rel_tag.0);

    for (rel_tag, rel) in tree_relationships {
        let pg_data_field = pg_domain
            .datatables
            .get(&def_uid)
            .and_then(|datatable| datatable.data_fields.get(&rel_tag));

        let column_name = format!("r_{}", &ontology[rel.name]).into_boxed_str();

        if let Some(pg_data_field) = pg_data_field {
            assert_eq!(
                pg_data_field.column_name, column_name,
                "TODO: rename column"
            );
        } else {
            ctx.steps.push((
                domain_uid,
                MigrationStep::DeployDataField {
                    datatable_def_id: vertex_def_id,
                    rel_tag,
                    column_name,
                },
            ));
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
        } => {
            let vertex_def_tag = vertex_def_id.1 as i32;
            let pg_domain = ctx.domains.get_mut(&domain_uid).unwrap();

            txn.query(
                &format!(
                    "CREATE TABLE {schema}.{table} (key bigserial PRIMARY KEY)",
                    schema = EscapeIdent(&pg_domain.schema_name),
                    table = EscapeIdent(&table_name),
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
            pg_domain.datatables.insert(
                DefUid(domain_uid, vertex_def_id.1),
                PgDataTable {
                    key: datatable_key,
                    table_name,
                    data_fields: Default::default(),
                },
            );
        }
        MigrationStep::DeployDataField {
            datatable_def_id,
            rel_tag,
            column_name,
        } => {
            let pg_domain = ctx.domains.get(&domain_uid).unwrap();
            let datatable = pg_domain
                .datatables
                .get(&DefUid(domain_uid, datatable_def_id.1))
                .unwrap();

            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{table} ADD COLUMN {column} text",
                    schema = EscapeIdent(&pg_domain.schema_name),
                    table = EscapeIdent(&datatable.table_name),
                    column = EscapeIdent(&column_name)
                ),
                &[],
            )
            .await
            .context("alter table add column")?;

            txn.query_one(
                indoc! { "
                    INSERT INTO m6m_reg.datafield (
                        datatable_key,
                        rel_tag,
                        column_name
                    ) VALUES($1, $2, $3)
                    RETURNING key
                "},
                &[&datatable.key, &(rel_tag.0 as i32), &column_name],
            )
            .await
            .context("update datafield")?;
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
            def_uid,
            old: old_table,
            new: new_table,
        } => {
            let domain_key = ctx.domain_key(&domain_uid).unwrap();
            let pg_domain = ctx.domains.get_mut(&domain_uid).unwrap();
            let pg_datatable = pg_domain.datatables.get_mut(&def_uid).unwrap();

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
                &[&new_table, &domain_key, &(def_uid.1 as i32)],
            )
            .await?;

            pg_datatable.table_name = new_table;
        }
    }

    Ok(())
}
