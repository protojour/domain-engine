use anyhow::anyhow;
use ontol_runtime::{ontology::Ontology, PackageId};
use tokio_postgres::{Client, NoTls, Transaction};
use tracing::{debug, info};

use crate::sql::EscapeIdentifier;

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
        migrate_domain(*package_id, ontology, &txn).await?;
    }

    txn.commit().await?;

    Ok(())
}

async fn migrate_domain<'a>(
    package_id: PackageId,
    ontology: &Ontology,
    txn: &Transaction<'a>,
) -> anyhow::Result<()> {
    let domain = ontology
        .find_domain(package_id)
        .ok_or_else(|| anyhow!("domain does not exist"))?;
    let unique_name = &ontology[domain.unique_name()];
    let schema_name = format!("m6m_domain_{unique_name}");

    debug!("migrate domain {package_id:?}: `{unique_name}`");

    txn.query(
        "
        INSERT INTO m6m_reg.domain VALUES($1, $2)
        ON CONFLICT DO NOTHING
        ",
        &[&unique_name, &schema_name],
    )
    .await?;

    // All the things owned by the domain will be isolated inside a schema.
    // Ensure the schema exists.
    txn.query(
        &format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            EscapeIdentifier(&schema_name)
        ),
        &[],
    )
    .await?;

    Ok(())
}
