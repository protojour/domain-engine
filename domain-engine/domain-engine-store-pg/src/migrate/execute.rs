use anyhow::Context;
use indoc::indoc;
use itertools::Itertools;
use tokio_postgres::Transaction;
use tracing::{info, info_span, Instrument};

use crate::{
    pg_model::{
        PgDataField, PgDataTable, PgEdge, PgEdgeCardinal, PgEdgeCardinalKind, PgIndexType,
        PgRegKey, PgType,
    },
    sql::{self, Unpack},
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
                        INSERT INTO m6mreg.domain (
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
                        INSERT INTO m6mreg.datatable (
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
                    datafield_indexes: Default::default(),
                },
            );
        }
        MigrationStep::DeployDataField {
            datatable_def_id,
            rel_tag,
            pg_type,
            column_name,
        } => {
            let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
            let datatable = pg_domain.datatables.get_mut(&datatable_def_id).unwrap();

            let type_ident = match pg_type {
                PgType::Boolean => "boolean",
                PgType::Integer => "integer",
                PgType::BigInt => "bigint",
                PgType::DoublePrecision => "double precision",
                PgType::Text => "text",
                PgType::Bytea => "bytea",
                PgType::TimestampTz => "timestamptz",
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

            let (key,) = txn
                .query_one(
                    indoc! { "
                    INSERT INTO m6mreg.datafield (
                        datatable_key,
                        rel_tag,
                        pg_type,
                        column_name
                    ) VALUES($1, $2, $3, $4)
                    RETURNING key
                "},
                    &[&datatable.key, &(rel_tag.0 as i32), &pg_type, &column_name],
                )
                .await
                .context("create datafield")?
                .unpack();

            datatable.data_fields.insert(
                rel_tag,
                PgDataField {
                    key,
                    col_name: column_name,
                    pg_type,
                },
            );
        }
        MigrationStep::DeployDataIndex {
            datatable_def_id,
            index_def_id,
            index_type,
            field_tuple,
        } => {
            let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
            let pg_datatable = pg_domain.datatables.get(&datatable_def_id).unwrap();

            let datafield_tuple: Vec<_> = field_tuple
                .iter()
                .map(|rel_tag| pg_datatable.data_fields.get(rel_tag).unwrap())
                .collect();

            txn.query(
                &format!(
                    "CREATE {index} ON {schema}.{table} ({columns})",
                    index = match index_type {
                        PgIndexType::Unique => "UNIQUE INDEX",
                        PgIndexType::BTree => "INDEX",
                    },
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&pg_datatable.table_name),
                    columns = datafield_tuple
                        .iter()
                        .map(|pg_datafield| pg_datafield.col_name.as_ref())
                        .map(sql::Ident)
                        .format(","),
                ),
                &[],
            )
            .await
            .context("create index")?;

            txn.query(
                indoc! { "
                    INSERT INTO m6mreg.datatable_index (
                        datatable_key,
                        def_domain_key,
                        def_tag,
                        index_type,
                        datafield_keys
                    ) VALUES($1, $2, $3, $4, $5)
                "},
                &[
                    &pg_datatable.key,
                    &pg_domain.key,
                    &(index_def_id.1 as i32),
                    &index_type,
                    &datafield_tuple
                        .iter()
                        .map(|datafield| datafield.key)
                        .collect_vec(),
                ],
            )
            .await
            .context("update datafield")?;
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
                        INSERT INTO m6mreg.edgetable (
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
            key_col_name,
            kind,
        } => {
            let pg_edge_domain = ctx.domains.get(&pkg_id).unwrap();
            let pg_edge = pg_edge_domain.edges.get(&edge_tag).unwrap();

            match &kind {
                PgEdgeCardinalKind::Dynamic { def_col_name } => {
                    txn.query(
                        &format!(
                            "ALTER TABLE {schema}.{table} ADD COLUMN {column} integer",
                            schema = sql::Ident(&pg_edge_domain.schema_name),
                            table = sql::Ident(&pg_edge.table_name),
                            column = sql::Ident(&def_col_name)
                        ),
                        &[],
                    )
                    .await
                    .context("alter table add def column")?;

                    txn.query(
                        &format!(
                            "ALTER TABLE {schema}.{table} ADD COLUMN {column} bigint",
                            schema = sql::Ident(&pg_edge_domain.schema_name),
                            table = sql::Ident(&pg_edge.table_name),
                            column = sql::Ident(&key_col_name)
                        ),
                        &[],
                    )
                    .await
                    .context("alter table add key column")?;
                }
                PgEdgeCardinalKind::Unique { def_id } => {
                    let pg_target_domain = ctx.domains.get(&def_id.package_id()).unwrap();
                    let pg_target_datatable = pg_target_domain.datatables.get(&def_id).unwrap();

                    txn.query(
                        &format!(
                            indoc! { "
                                ALTER TABLE {schema}.{table}
                                    ADD COLUMN {column} bigint NOT NULL
                                    REFERENCES {refschema}.{refdatatable}(_key)
                                    ON DELETE CASCADE
                            "},
                            schema = sql::Ident(&pg_edge_domain.schema_name),
                            table = sql::Ident(&pg_edge.table_name),
                            column = sql::Ident(&key_col_name),
                            refschema = sql::Ident(&pg_target_domain.schema_name),
                            refdatatable = sql::Ident(&pg_target_datatable.table_name)
                        ),
                        &[],
                    )
                    .await
                    .context("alter table add key column")?;
                }
            };

            let mut def_column_name: Option<&str> = None;
            let mut unique_datatable_key: Option<PgRegKey> = None;

            match &kind {
                PgEdgeCardinalKind::Dynamic { def_col_name } => {
                    def_column_name = Some(def_col_name.as_ref())
                }
                PgEdgeCardinalKind::Unique { def_id } => {
                    unique_datatable_key = Some(pg_edge_domain.datatables.get(def_id).unwrap().key);
                }
            }

            let row = txn
                .query_one(
                    indoc! { "
                        INSERT INTO m6mreg.edgecardinal (
                            edge_key,
                            ordinal,
                            ident,
                            def_column_name,
                            unique_datatable_key,
                            key_column_name
                        ) VALUES($1, $2, $3, $4, $5, $6)
                        RETURNING key
                    "},
                    &[
                        &pg_edge.key,
                        &(ordinal as i32),
                        &ident,
                        &def_column_name,
                        &unique_datatable_key,
                        &key_col_name,
                    ],
                )
                .await
                .context("insert edgetable")?;

            let pg_edge = ctx
                .domains
                .get_mut(&pkg_id)
                .unwrap()
                .edges
                .get_mut(&edge_tag)
                .unwrap();

            pg_edge.cardinals.insert(
                ordinal as usize,
                PgEdgeCardinal {
                    key: row.get(0),
                    ident,
                    key_col_name,
                    kind,
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
                "UPDATE m6mreg.domain SET(schema_name = $1) WHERE (key = $2)",
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
                "UPDATE m6mreg.datatable SET(table_name = $1) WHERE domain_key = $2 AND def_tag = $3",
                &[&new_table, &domain_key, &(def_id.1 as i32)],
            )
            .await?;

            pg_datatable.table_name = new_table;
        }
    }

    Ok(())
}
