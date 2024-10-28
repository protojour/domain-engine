//! ontology migration

use anyhow::{anyhow, Context};
use indoc::{formatdoc, indoc};
use itertools::Itertools;
use tokio_postgres::Transaction;
use tracing::{info, info_span, Instrument};

use crate::{
    pg_model::{
        PgColumn, PgDomainTableType, PgEdgeCardinal, PgEdgeCardinalKind, PgIndexType, PgProperty,
        PgPropertyData, PgPropertyType, PgRegKey, PgTable, PgTableIdUnion, PgType,
    },
    sql::{self},
};

use super::{MigrationCtx, MigrationStep, PgDomainIds};

pub async fn execute_domain_migration<'t>(
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let staged_steps = std::mem::take(&mut ctx.steps).into_inner();

    for (_stage, steps) in staged_steps {
        for (ids, step) in steps {
            execute_migration_step(ids, step, txn, ctx)
                .instrument(info_span!("migrate", uid = %ids.uid))
                .await?;
        }
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

    let domain_index = domain_ids.index;
    let domain_uid = domain_ids.uid;

    match step {
        MigrationStep::DeployDomain { name, schema_name } => {
            // All the things owned by the domain will be isolated inside this schema.
            txn.query(&format!("CREATE SCHEMA {}", sql::Ident(&schema_name)), &[])
                .await
                .context("create schema")?;

            let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();

            let row = txn
                .query_one(
                    indoc! {"
                        INSERT INTO m6mreg.domain (
                            uid,
                            name,
                            schema_name,
                            has_crdt
                        ) VALUES($1, $2, $3, $4)
                        RETURNING key
                    "},
                    &[&domain_uid, &name, &schema_name, &pg_domain.has_crdt],
                )
                .await?;

            pg_domain.key = Some(row.get(0));
        }
        MigrationStep::DeployVertex {
            vertex_def_id,
            table_name,
        } => {
            let vertex_def_tag = vertex_def_id.1 as i32;
            let def_domain_key = ctx
                .domains
                .get(&vertex_def_id.domain_index())
                .ok_or_else(|| anyhow!("could not find deployed domain for {vertex_def_id:?}"))?
                .key
                .ok_or_else(|| anyhow!("deployed domain for {vertex_def_id:?} has no key"))?;

            let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();

            let key_column = "_key";
            let created_at_column = "_created";
            let updated_at_column = "_updated";

            txn.execute(
                &format!(
                    "CREATE TABLE {schema}.{table} (
                        {key_column} bigserial PRIMARY KEY,
                        {created_at_column} timestamptz NOT NULL,
                        {updated_at_column} timestamptz NOT NULL
                    )",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&table_name),
                ),
                &[],
            )
            .await
            .context("create vertex table")?;

            txn.execute(
                &format!(
                    "CREATE INDEX ON {schema}.{table} ({updated_at_column})",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&table_name),
                ),
                &[],
            )
            .await
            .context("create index on _updated")?;

            let row = txn
                .query_one(
                    indoc! { "
                        INSERT INTO m6mreg.domaintable (
                            domain_key,
                            table_type,
                            def_domain_key,
                            def_tag,
                            table_name,
                            key_column,
                            created_at_column,
                            updated_at_column
                        ) VALUES($1, $2, $3, $4, $5, $6, $7, $8)
                        RETURNING key
                    "},
                    &[
                        &pg_domain.key,
                        &PgDomainTableType::Vertex,
                        &def_domain_key,
                        &vertex_def_tag,
                        &table_name,
                        &key_column,
                        &created_at_column,
                        &updated_at_column,
                    ],
                )
                .await
                .context("insert vertex table")?;

            let domaintable_key: PgRegKey = row.get(0);
            pg_domain.datatables.insert(
                vertex_def_id,
                PgTable {
                    key: domaintable_key,
                    table_name,
                    has_fkey: false,
                    properties: Default::default(),
                    property_indexes: Default::default(),
                    edge_cardinals: Default::default(),
                },
            );
        }
        MigrationStep::DeployVertexFKey { vertex_def_id } => {
            let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();
            let pg_table = pg_domain.datatables.get_mut(&vertex_def_id).unwrap();

            let (fprop, fkey) = ("_fprop", "_fkey");

            txn.execute(
                &formatdoc!(
                    "ALTER TABLE {schema}.{table}
                        ADD COLUMN {fprop} int NOT NULL,
                        ADD COLUMN {fkey} bigint NOT NULL",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&pg_table.table_name),
                    fprop = sql::Ident(fprop),
                    fkey = sql::Ident(fkey),
                ),
                &[],
            )
            .await
            .context("alter table add fkey")?;

            txn.execute(
                &format!(
                    "CREATE INDEX ON {schema}.{table} ({fprop}, {fkey})",
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&pg_table.table_name),
                    fprop = sql::Ident(fprop),
                    fkey = sql::Ident(fkey),
                ),
                &[],
            )
            .await
            .context("create fkey index")?;

            txn.query_one(
                "UPDATE m6mreg.domaintable SET fprop_column = $1, fkey_column = $2 WHERE key = $3 RETURNING 0",
                &[&fprop, &fkey, &pg_table.key],
            )
            .await
            .context("update domaintable fkey")?;

            pg_table.has_fkey = true;
        }
        MigrationStep::DeployProperty {
            table_id,
            prop_tag,
            data,
        } => {
            let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();
            let pg_table = match table_id {
                PgTableIdUnion::Def(def_id) => pg_domain.datatables.get_mut(&def_id),
                PgTableIdUnion::Edge(edge_id) => pg_domain.edgetables.get_mut(&edge_id.def_id().1),
            }
            .unwrap();

            if let PgPropertyData::Scalar { col_name, pg_type } = &data {
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

                txn.execute(
                    &format!(
                        "ALTER TABLE {schema}.{table} ADD COLUMN {column} {type}",
                        schema = sql::Ident(&pg_domain.schema_name),
                        table = sql::Ident(&pg_table.table_name),
                        column = sql::Ident(&col_name),
                        type = &type_ident
                    ),
                    &[],
                )
                .await
                .context("alter table add column")?;
            }

            let (pg_property_type, pg_type, column_name) = match &data {
                PgPropertyData::Scalar { col_name, pg_type } => {
                    (PgPropertyType::Column, Some(pg_type), Some(col_name))
                }
                PgPropertyData::AbstractStruct => (PgPropertyType::AbstractStruct, None, None),
                PgPropertyData::AbstractCrdt => (PgPropertyType::AbstractCrdt, None, None),
            };

            let key = txn
                .query_one(
                    indoc! {"
                        INSERT INTO m6mreg.property (
                            domaintable_key,
                            prop_tag,
                            property_type,
                            pg_type,
                            column_name
                        ) VALUES($1, $2, $3, $4, $5)
                        RETURNING key
                    "},
                    &[
                        &pg_table.key,
                        &(prop_tag.0 as i32),
                        &pg_property_type,
                        &pg_type,
                        &column_name,
                    ],
                )
                .await
                .context("create property")?
                .get(0);

            let existing = pg_table.properties.insert(
                prop_tag,
                match data {
                    PgPropertyData::Scalar { col_name, pg_type } => PgProperty::Column(PgColumn {
                        key,
                        col_name,
                        pg_type,
                    }),
                    PgPropertyData::AbstractStruct => PgProperty::AbstractStruct(key),
                    PgPropertyData::AbstractCrdt => PgProperty::AbstractCrdt(key),
                },
            );

            if existing.is_some() {
                return Err(anyhow!("{prop_tag:?} already a field in {table_id:?}"));
            }
        }
        MigrationStep::DeployPropertyIndex {
            table_id,
            index_def_id,
            index_type,
            field_tuple,
        } => {
            let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();
            let pg_table = pg_domain.get_table(&table_id).unwrap();

            let column_tuple: Vec<_> = field_tuple
                .iter()
                .map(|prop_tag| {
                    pg_table
                        .properties
                        .get(prop_tag)
                        .unwrap()
                        .as_ref()
                        .as_column()
                        .unwrap()
                })
                .collect();

            txn.execute(
                &format!(
                    "CREATE {index} ON {schema}.{table} ({columns})",
                    index = match index_type {
                        PgIndexType::Unique => "UNIQUE INDEX",
                        PgIndexType::BTree => "INDEX",
                    },
                    schema = sql::Ident(&pg_domain.schema_name),
                    table = sql::Ident(&pg_table.table_name),
                    columns = column_tuple
                        .iter()
                        .map(|pg_column| pg_column.col_name)
                        .map(sql::Ident)
                        .format(","),
                ),
                &[],
            )
            .await
            .context("create index")?;

            txn.query(
                indoc! { "
                    INSERT INTO m6mreg.domaintable_index (
                        domaintable_key,
                        def_domain_key,
                        def_tag,
                        index_type,
                        property_keys
                    ) VALUES($1, $2, $3, $4, $5)
                "},
                &[
                    &pg_table.key,
                    &pg_domain.key,
                    &(index_def_id.1 as i32),
                    &index_type,
                    &column_tuple
                        .iter()
                        .map(|datafield| datafield.key)
                        .collect_vec(),
                ],
            )
            .await
            .context("update domaintable_index")?;
        }
        MigrationStep::DeployEdge {
            edge_tag,
            table_name,
        } => {
            let created_at_column = "_created";
            let updated_at_column = "_updated";

            let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();
            txn.execute(
                &formatdoc!(
                    "CREATE TABLE {schema}.{table} (
                        {created_at_column} timestamptz NOT NULL,
                        {updated_at_column} timestamptz NOT NULL
                    )",
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
                        INSERT INTO m6mreg.domaintable (
                            domain_key,
                            table_type,
                            def_domain_key,
                            def_tag,
                            table_name,
                            created_at_column,
                            updated_at_column
                        ) VALUES($1, $2, $3, $4, $5, $6, $7)
                        RETURNING key
                    "},
                    &[
                        &pg_domain.key,
                        &PgDomainTableType::Edge,
                        &pg_domain.key,
                        &(edge_tag as i32),
                        &table_name,
                        &created_at_column,
                        &updated_at_column,
                    ],
                )
                .await
                .context("insert edge table")?;

            let key: PgRegKey = row.get(0);

            pg_domain.edgetables.insert(
                edge_tag,
                PgTable {
                    key,
                    table_name,
                    has_fkey: false,
                    properties: Default::default(),
                    edge_cardinals: Default::default(),
                    property_indexes: Default::default(),
                },
            );
        }
        MigrationStep::DeployEdgeCardinal {
            edge_tag,
            index,
            ident,
            kind,
            index_type,
        } => {
            let pg_edge_domain = ctx.domains.get(&domain_index).unwrap();
            let pg_table = pg_edge_domain.edgetables.get(&edge_tag).unwrap();

            match &kind {
                PgEdgeCardinalKind::Dynamic {
                    def_col_name,
                    key_col_name,
                } => {
                    txn.execute(
                        &format!(
                            "ALTER TABLE {schema}.{table} ADD COLUMN {column} integer",
                            schema = sql::Ident(&pg_edge_domain.schema_name),
                            table = sql::Ident(&pg_table.table_name),
                            column = sql::Ident(&def_col_name)
                        ),
                        &[],
                    )
                    .await
                    .context("alter table add def column")?;

                    txn.execute(
                        &format!(
                            "ALTER TABLE {schema}.{table} ADD COLUMN {column} bigint",
                            schema = sql::Ident(&pg_edge_domain.schema_name),
                            table = sql::Ident(&pg_table.table_name),
                            column = sql::Ident(&key_col_name)
                        ),
                        &[],
                    )
                    .await
                    .context("alter table add key column")?;

                    if let Some(index_type) = &index_type {
                        txn.execute(
                            &format!(
                                "CREATE {index} ON {schema}.{table} ({columns})",
                                index = match index_type {
                                    PgIndexType::Unique => "UNIQUE INDEX",
                                    PgIndexType::BTree => "INDEX",
                                },
                                schema = sql::Ident(&pg_edge_domain.schema_name),
                                table = sql::Ident(&pg_table.table_name),
                                columns = [def_col_name, key_col_name]
                                    .iter()
                                    .map(sql::Ident)
                                    .format(","),
                            ),
                            &[],
                        )
                        .await
                        .context("create edge index")?;
                    }
                }
                PgEdgeCardinalKind::PinnedDef {
                    pinned_def_id: def_id,
                    key_col_name,
                } => {
                    let pg_target_domain = ctx.domains.get(&def_id.domain_index()).unwrap();
                    let pg_target_datatable = pg_target_domain.datatables.get(def_id).unwrap();

                    txn.execute(
                        &format!(
                            indoc! { "
                                ALTER TABLE {schema}.{table}
                                    ADD COLUMN {column} bigint NOT NULL
                                    REFERENCES {refschema}.{refdatatable}(_key)
                                    ON DELETE CASCADE
                                    UNIQUE
                            "},
                            schema = sql::Ident(&pg_edge_domain.schema_name),
                            table = sql::Ident(&pg_table.table_name),
                            column = sql::Ident(&key_col_name),
                            refschema = sql::Ident(&pg_target_domain.schema_name),
                            refdatatable = sql::Ident(&pg_target_datatable.table_name)
                        ),
                        &[],
                    )
                    .await
                    .context("alter table add key column")?;
                }
                PgEdgeCardinalKind::Parameters(_param_def_id) => {}
            };

            let mut def_column_name: Option<&str> = None;
            let mut key_column_name: Option<&str> = None;
            let mut pinned_domaintable_key: Option<PgRegKey> = None;

            match &kind {
                PgEdgeCardinalKind::Dynamic {
                    def_col_name,
                    key_col_name,
                } => {
                    def_column_name = Some(def_col_name.as_ref());
                    key_column_name = Some(key_col_name.as_ref());
                }
                PgEdgeCardinalKind::PinnedDef {
                    pinned_def_id,
                    key_col_name,
                } => {
                    let pinned_domain = ctx.domains.get(&pinned_def_id.domain_index()).unwrap();
                    let pinned_table = pinned_domain.datatables.get(pinned_def_id).unwrap();

                    pinned_domaintable_key = Some(pinned_table.key);
                    key_column_name = Some(key_col_name.as_ref());
                }
                PgEdgeCardinalKind::Parameters(_) => {}
            }

            let row = txn
                .query_one(
                    indoc! { "
                        INSERT INTO m6mreg.edgecardinal (
                            domaintable_key,
                            ordinal,
                            ident,
                            def_column_name,
                            pinned_domaintable_key,
                            key_column_name,
                            index_type
                        ) VALUES($1, $2, $3, $4, $5, $6, $7)
                        RETURNING key
                    "},
                    &[
                        &pg_table.key,
                        &(index.0 as i32),
                        &ident,
                        &def_column_name,
                        &pinned_domaintable_key,
                        &key_column_name,
                        &index_type,
                    ],
                )
                .await
                .context("insert edgetable")?;

            let pg_edge = ctx
                .domains
                .get_mut(&domain_index)
                .unwrap()
                .edgetables
                .get_mut(&edge_tag)
                .unwrap();

            pg_edge.edge_cardinals.insert(
                index,
                PgEdgeCardinal {
                    key: row.get(0),
                    ident,
                    kind,
                    index_type,
                },
            );
        }
        MigrationStep::DeployCrdt { domain_index } => {
            let pg_domain = ctx.domains.get(&domain_index).unwrap();
            txn.execute(
                &format!(
                    "CREATE TABLE {schema}.crdt (
                        _fprop int NOT NULL,
                        _fkey bigint NOT NULL,
                        chunk_id bigserial NOT NULL,
                        chunk_type m6m_crdt_chunk_type NOT NULL,
                        chunk bytea NOT NULL
                    )",
                    schema = sql::Ident(&pg_domain.schema_name),
                ),
                &[],
            )
            .await
            .context("create crdt table")?;

            txn.execute(
                &format!(
                    "CREATE INDEX ON {schema}.crdt (_fprop, _fkey, chunk_id)",
                    schema = sql::Ident(&pg_domain.schema_name),
                ),
                &[],
            )
            .await
            .context("create crdt index")?;
        }
        MigrationStep::RenameDomainSchema { old, new } => {
            let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();
            let domain_key = pg_domain.key.unwrap();

            txn.execute(
                &format!(
                    "ALTER SCHEMA {old} RENAME TO {new}",
                    old = sql::Ident(&old),
                    new = sql::Ident(&new),
                ),
                &[],
            )
            .await?;

            txn.execute(
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
            let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();
            let domain_key = pg_domain.key.unwrap();
            let pg_datatable = pg_domain.datatables.get_mut(&def_id).unwrap();

            txn.execute(
                &format!(
                    "ALTER TABLE {schema}.{old} RENAME TO {schema}.{new}",
                    schema = sql::Ident(&pg_domain.schema_name),
                    old = sql::Ident(&old_table),
                    new = sql::Ident(&new_table),
                ),
                &[],
            )
            .await?;

            txn.execute(
                "UPDATE m6mreg.domaintable SET(table_name = $1) WHERE domain_key = $2 AND def_tag = $3",
                &[&new_table, &domain_key, &(def_id.1 as i32)],
            )
            .await?;

            pg_datatable.table_name = new_table;
        }
    }

    Ok(())
}
