use anyhow::Context;
use indoc::indoc;
use tokio_postgres::Transaction;
use tracing::{info, info_span, Instrument};

use crate::{
    pg_model::{DefUid, PgDataTable, PgSerial, PgType},
    sql::EscapeIdent,
};

use super::{DomainUid, MigrationCtx, MigrationStep};

pub async fn execute_domain_migration<'t>(
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
            pg_type,
        } => {
            let pg_domain = ctx.domains.get(&domain_uid).unwrap();
            let datatable = pg_domain
                .datatables
                .get(&DefUid(domain_uid, datatable_def_id.1))
                .unwrap();

            let type_ident = match pg_type {
                PgType::Boolean => "boolean",
                PgType::Text => "text",
                PgType::Bytea => "bytea",
                PgType::Timestamp => "timestamp",
                PgType::Bigserial => "bigserial",
            };

            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{table} ADD COLUMN {column} {type}",
                    schema = EscapeIdent(&pg_domain.schema_name),
                    table = EscapeIdent(&datatable.table_name),
                    column = EscapeIdent(&column_name),
                    type = EscapeIdent(&type_ident)
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
                        pg_type,
                        column_name
                    ) VALUES($1, $2, $3, $4)
                    RETURNING key
                "},
                &[
                    &datatable.key,
                    &(rel_tag.0 as i32),
                    &pg_type.to_string()?,
                    &column_name,
                ],
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
