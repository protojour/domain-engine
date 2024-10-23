//! migration of existing domain schemas due to the PostgreSQL model changing (disregarding the ontology)

use anyhow::Context;
use tokio_postgres::Transaction;
use tracing::info;

use crate::pg_model::RegVersion;

use super::MigrationCtx;

pub async fn migrate_domain_schemas<'t>(
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    while ctx.deployed_domain_schema_version != ctx.current_version {
        let next_int = ctx.deployed_domain_schema_version as u32 + 1;
        let next = RegVersion::try_from(next_int).unwrap();

        info!("migrating domain schemas to V{next_int}: {next:?}");

        migrate(ctx, next).await?;

        txn.query_one(
            "UPDATE m6mreg.domain_migration SET version = $1 RETURNING 0",
            &[&(next as i32)],
        )
        .await
        .context("write next version")?;
        ctx.deployed_domain_schema_version = next;
    }

    Ok(())
}

async fn migrate(_ctx: &mut MigrationCtx, version: RegVersion) -> anyhow::Result<()> {
    match version {
        RegVersion::Init => {
            // the initial migration, this is trivial
        }
        RegVersion::Crdt => {
            // nothing to do in domain schemas, the "crdt" table is created on demand.
        }
    }

    Ok(())
}
