use ontol_runtime::{ontology::Ontology, PackageId};
use tokio_postgres::{Client, NoTls};
use tracing::info;

mod registry {
    use refinery::embed_migrations;
    embed_migrations!("./registry_migrations");
}

const MIGRATIONS_TABLE_NAME: &str = "memoriam_registry_schema_history";

pub async fn connect_and_migrate(
    persistent_domains: &[PackageId],
    ontology: &Ontology,
    pg_config: &tokio_postgres::Config,
) -> anyhow::Result<()> {
    let (mut client, connection) = pg_config.connect(NoTls).await.unwrap();

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    let join_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    migrate(persistent_domains, ontology, &mut client).await?;
    drop(client);

    join_handle.await.unwrap();

    Ok(())
}

async fn migrate(
    persistent_domains: &[PackageId],
    ontology: &Ontology,
    pg_client: &mut Client,
) -> anyhow::Result<()> {
    info!("migrating database");

    migrate_registry(pg_client).await?;

    for package_id in persistent_domains {
        migrate_domain(*package_id, ontology, pg_client).await?;
    }

    Ok(())
}

async fn migrate_registry(pg_client: &mut Client) -> anyhow::Result<()> {
    // ensure the migrations table is stored in the default "public" schema
    pg_client.query("SET search_path TO public", &[]).await?;

    registry::migrations::runner()
        .set_migration_table_name(MIGRATIONS_TABLE_NAME)
        .run_async(pg_client)
        .await?;

    Ok(())
}

async fn migrate_domain(
    _package_id: PackageId,
    _ontology: &Ontology,
    _pg_client: &mut Client,
) -> anyhow::Result<()> {
    Ok(())
}
