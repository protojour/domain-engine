use anyhow::{anyhow, Context};
use indoc::indoc;
use ontol_runtime::{
    ontology::{
        domain::{Domain, Entity},
        Ontology,
    },
    DefId, PackageId,
};
use tokio_postgres::{Client, NoTls, Transaction};
use tracing::{debug, debug_span, info, Instrument};
use ulid::Ulid;

use crate::sql::EscapeIdent;

mod registry {
    use refinery::embed_migrations;
    embed_migrations!("./registry_migrations");
}

const MIGRATIONS_TABLE_NAME: &str = "public.m6m_registry_schema_history";

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

    for package_id in persistent_domains {
        migrate_domain(*package_id, ontology, &txn)
            .instrument(debug_span!("migrate", pkg = package_id.id()))
            .await?;
    }

    txn.commit().await?;

    Ok(())
}

struct PgDomain {
    domain_id: Ulid,
    schema: String,
}

#[allow(unused)]
struct PgVertice {
    domain_id: Ulid,
    def_id: DefId,
    table: String,
}

async fn migrate_domain<'t>(
    package_id: PackageId,
    ontology: &Ontology,
    txn: &Transaction<'t>,
) -> anyhow::Result<()> {
    let domain = ontology
        .find_domain(package_id)
        .ok_or_else(|| anyhow!("domain does not exist"))?;

    let pg_domain = migrate_domain_meta(domain, ontology, txn).await?;

    for def in domain.defs() {
        let Some(entity) = def.entity() else {
            continue;
        };

        migrate_vertex_meta(def.id, entity, &pg_domain, ontology, txn).await?;
    }

    Ok(())
}

async fn migrate_domain_meta<'t>(
    domain: &Domain,
    ontology: &Ontology,
    txn: &Transaction<'t>,
) -> anyhow::Result<PgDomain> {
    let unique_name = &ontology[domain.unique_name()];
    let schema = format!("m6m_d_{unique_name}");

    let row = txn
        .query_opt(
            "SELECT domain_id, schema FROM m6m_reg.domain WHERE name = $1",
            &[&unique_name],
        )
        .await?;

    if let Some(row) = row {
        debug!("domain already installed");

        let mut pg_domain = PgDomain {
            domain_id: row.get(0),
            schema: row.get(1),
        };

        if pg_domain.schema != schema {
            debug!("rename schema");
            txn.query(
                &format!(
                    "ALTER SCHEMA {old} RENAME TO {new}",
                    old = EscapeIdent(&pg_domain.schema),
                    new = EscapeIdent(&schema),
                ),
                &[],
            )
            .await?;

            txn.query(
                "UPDATE m6m_reg.domain SET(schema = $1) WHERE (domain_id = $2)",
                &[&pg_domain.domain_id],
            )
            .await?;
            pg_domain.schema = schema;
        }

        Ok(pg_domain)
    } else {
        let domain_id = Ulid::new();

        debug!("install domain `{schema}`");

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
            &[&domain_id, &unique_name, &schema],
        )
        .await?;

        Ok(PgDomain { domain_id, schema })
    }
}

async fn migrate_vertex_meta<'t>(
    def_id: DefId,
    entity: &Entity,
    pg_domain: &PgDomain,
    ontology: &Ontology,
    txn: &Transaction<'t>,
) -> anyhow::Result<PgVertice> {
    let name = &ontology[entity.name];
    let def_tag = def_id.1 as i32;
    let table = format!("v_{}", name);

    let row = txn
        .query_opt(
            indoc! { "
                SELECT \"table\" FROM m6m_reg.vertice
                WHERE
                    domain_id = $1
                AND
                    def_tag = $2
            "},
            &[&pg_domain.domain_id, &(def_id.1 as i32)],
        )
        .await?;

    if let Some(row) = row {
        let mut vertice = PgVertice {
            domain_id: pg_domain.domain_id,
            def_id,
            table: row.get(0),
        };

        if vertice.table != table {
            debug!("rename table");
            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{old} RENAME TO {schema}.{new}",
                    schema = EscapeIdent(&pg_domain.schema),
                    old = EscapeIdent(&vertice.table),
                    new = EscapeIdent(&table),
                ),
                &[],
            )
            .await?;

            txn.query(
                "UPDATE m6m_reg.vertice SET(table = $1) WHERE domain_id = $2 AND def_tag = $3",
                &[&table, &pg_domain.domain_id, &def_tag],
            )
            .await?;

            vertice.table = table;
        }

        Ok(vertice)
    } else {
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
                INSERT INTO m6m_reg.vertice (
                    domain_id,
                    def_tag,
                    \"table\"
                ) VALUES($1, $2, $3)
            "},
            &[&pg_domain.domain_id, &def_tag, &table],
        )
        .await
        .context("create vertice")?;

        Ok(PgVertice {
            domain_id: pg_domain.domain_id,
            def_id,
            table,
        })
    }
}
