use anyhow::Context;
use indoc::indoc;
use tokio_postgres::Transaction;
use tracing::{info, info_span, Instrument};

use crate::{
    pg_model::{PgDataField, PgDataTable, PgEdge, PgEdgeCardinal, PgRegKey, PgType},
    sql,
};

use super::{MigrationCtx, MigrationStep, PgDomainIds};

pub async fn execute_domain_migration<'t>(
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    for (ids, step) in std::mem::take(&mut ctx.steps) {
        execute_migration_step(ids, step, txn, ctx)
            .instrument(info_span!("migrate", uid = %ids.uid))
            .await?;
    }

    Ok(())
}

async fn execute_migration_step<'t>(
    domain_ids: PgDomainIds,
    step: MigrationStep,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    info!("{step:?}");

    let pkg_id = domain_ids.pkg_id;
    let domain_uid = domain_ids.uid;

    match step {
        MigrationStep::DeployDomain { name, schema_name } => {
            // All the things owned by the domain will be isolated inside this schema.
            txn.query(&format!("CREATE SCHEMA {}", sql::Ident(&schema_name)), &[])
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

            ctx.domains.get_mut(&pkg_id).unwrap().key = Some(row.get(0));
        }
        MigrationStep::DeployVertex {
            vertex_def_id,
            table_name,
        } => {
            let vertex_def_tag = vertex_def_id.1 as i32;
            let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();

            txn.query(
                &format!(
                    "CREATE TABLE {schema}.{table} (_key bigserial PRIMARY KEY)",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&table_name),
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

            let datatable_key: PgRegKey = row.get(0);
            pg_domain.datatables.insert(
                vertex_def_id,
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
            let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
            let datatable = pg_domain.datatables.get_mut(&datatable_def_id).unwrap();

            let type_ident = match pg_type {
                PgType::Boolean => "boolean",
                PgType::BigInt => "bigint",
                PgType::DoublePrecision => "double precision",
                PgType::Text => "text",
                PgType::Bytea => "bytea",
                PgType::Timestamp => "timestamp",
                PgType::Bigserial => "bigserial",
            };

            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{table} ADD COLUMN {column} {type}",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&datatable.table_name),
                    column = sql::Ident(&column_name),
                    type = sql::Ident(&type_ident)
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

            datatable.data_fields.insert(
                rel_tag,
                PgDataField {
                    col_name: column_name,
                    pg_type,
                },
            );
        }
        MigrationStep::DeployEdge {
            edge_tag,
            table_name,
        } => {
            let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
            txn.query(
                &format!(
                    "CREATE TABLE {schema}.{table} ()",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&table_name),
                ),
                &[],
            )
            .await
            .context("create edge table")?;

            let row = txn
                .query_one(
                    indoc! { "
                        INSERT INTO m6m_reg.edgetable (
                            domain_key,
                            edge_tag,
                            table_name
                        ) VALUES($1, $2, $3)
                        RETURNING key
                    "},
                    &[&pg_domain.key, &(edge_tag as i32), &table_name],
                )
                .await
                .context("insert edgetable")?;

            let key: PgRegKey = row.get(0);

            pg_domain.edges.insert(
                edge_tag,
                PgEdge {
                    key,
                    table_name,
                    cardinals: Default::default(),
                },
            );
        }
        MigrationStep::DeployEdgeCardinal {
            edge_tag,
            ordinal,
            ident,
            def_col_name,
            key_col_name,
        } => {
            let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
            let pg_edge = pg_domain.edges.get_mut(&edge_tag).unwrap();

            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{table} ADD COLUMN {column} integer",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&pg_edge.table_name),
                    column = sql::Ident(&def_col_name)
                ),
                &[],
            )
            .await
            .context("alter table add type column")?;

            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{table} ADD COLUMN {column} bigint",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&pg_edge.table_name),
                    column = sql::Ident(&key_col_name)
                ),
                &[],
            )
            .await
            .context("alter table add key column")?;

            let row = txn
                .query_one(
                    indoc! { "
                        INSERT INTO m6m_reg.edgecardinal (
                            edge_key,
                            ordinal,
                            ident,
                            def_column_name,
                            key_column_name
                        ) VALUES($1, $2, $3, $4, $5)
                        RETURNING key
                    "},
                    &[
                        &pg_edge.key,
                        &(ordinal as i32),
                        &ident,
                        &def_col_name,
                        &key_col_name,
                    ],
                )
                .await
                .context("insert edgetable")?;

            pg_edge.cardinals.insert(
                ordinal as usize,
                PgEdgeCardinal {
                    key: row.get(0),
                    ident,
                    def_col_name,
                    key_col_name,
                },
            );
        }
        MigrationStep::RenameDomainSchema { old, new } => {
            let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
            let domain_key = pg_domain.key.unwrap();

            txn.query(
                &format!(
                    "ALTER SCHEMA {old} RENAME TO {new}",
                    old = sql::Ident(&old),
                    new = sql::Ident(&new),
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
            let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
            let domain_key = pg_domain.key.unwrap();
            let pg_datatable = pg_domain.datatables.get_mut(&def_id).unwrap();

            txn.query(
                &format!(
                    "ALTER TABLE {schema}.{old} RENAME TO {schema}.{new}",
                    schema = sql::Ident(&pg_domain.schema_name),
                    old = sql::Ident(&old_table),
                    new = sql::Ident(&new_table),
                ),
                &[],
            )
            .await?;

            txn.query(
                "UPDATE m6m_reg.datatable SET(table_name = $1) WHERE domain_key = $2 AND def_tag = $3",
                &[&new_table, &domain_key, &(def_id.1 as i32)],
            )
            .await?;

            pg_datatable.table_name = new_table;
        }
    }

    Ok(())
}
