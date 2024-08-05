use std::collections::BTreeMap;

use anyhow::{anyhow, Context};
use indoc::indoc;
use itertools::Itertools;
use ontol_runtime::{
    ontology::{
        domain::{
            DataRelationshipKind, DataRelationshipTarget, Def, DefKind, DefRepr, Domain, Entity,
        },
        Ontology,
    },
    DefId, DefRelTag, PackageId,
};
use tokio_postgres::Transaction;
use tracing::info;

use crate::{
    migrate::{MigrationStep, PgDomain},
    pg_model::{PgDataField, PgDataTable, PgEdge, PgEdgeCardinal, PgRegKey, PgType},
    sql::Unpack,
};

use super::{MigrationCtx, PgDomainIds};

pub async fn migrate_domain_steps<'t>(
    pkg_id: PackageId,
    domain: &Domain,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
    txn: &Transaction<'t>,
) -> anyhow::Result<()> {
    let domain_uid = domain.domain_id().ulid;
    let domain_ids = PgDomainIds {
        pkg_id,
        uid: domain_uid,
    };
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
                let key: PgRegKey = row.get(0);
                let def_domain_key: PgRegKey = row.get(1);
                let def_tag: u16 = row.get::<_, i32>(2).try_into()?;
                let table_name: Box<str> = row.get(3);

                // for now
                assert_eq!(def_domain_key, domain_key);

                Ok((
                    DefId(pkg_id, def_tag),
                    PgDataTable {
                        key,
                        table_name,
                        data_fields: Default::default(),
                    },
                ))
            })
            .try_collect()?;

        let pg_edges = txn
            .query(
                indoc! {"
                    SELECT key, edge_tag, table_name
                    FROM m6m_reg.edgetable
                    WHERE domain_key = $1
                "},
                &[&domain_key],
            )
            .await?
            .into_iter()
            .map(|row| -> anyhow::Result<_> {
                let key: PgRegKey = row.get(0);
                let edge_tag: u16 = row.get::<_, i32>(1).try_into()?;
                let table_name: Box<str> = row.get(2);

                Ok((
                    edge_tag,
                    PgEdge {
                        key,
                        table_name,
                        cardinals: Default::default(),
                    },
                ))
            })
            .try_collect()?;

        let pg_domain = PgDomain {
            key: Some(domain_key),
            schema_name,
            datatables: pg_datatables,
            edges: pg_edges,
        };
        ctx.domains.insert(pkg_id, pg_domain.clone());

        if pg_domain.schema_name != schema {
            ctx.steps.push((
                domain_ids,
                MigrationStep::RenameDomainSchema {
                    old: pg_domain.schema_name.clone(),
                    new: schema,
                },
            ));
        }
    } else {
        ctx.domains.insert(
            pkg_id,
            PgDomain {
                key: None,
                schema_name: schema.clone(),
                datatables: Default::default(),
                edges: Default::default(),
            },
        );

        ctx.steps.push((
            domain_ids,
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

        migrate_vertex_steps(domain_ids, def.id, def, entity, ontology, txn, ctx).await?;
    }

    let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();

    for (edge_id, edge_info) in domain.edges() {
        let edge_tag = edge_id.1;
        let table_name = format!("e_{edge_tag}").into_boxed_str();

        if let Some(pg_edge) = pg_domain.edges.get_mut(&edge_tag) {
            let pg_cardinals: BTreeMap<usize, PgEdgeCardinal> = txn
                .query(
                    indoc! {"
                        SELECT key, ordinal, ident, def_column_name, key_column_name
                        FROM m6m_reg.edgecardinal
                        WHERE edge_key = $1
                        ORDER BY ordinal
                    "},
                    &[&pg_edge.key],
                )
                .await?
                .into_iter()
                .map(|row| -> anyhow::Result<_> {
                    let (key, ordinal, ident, def_col_name, key_col_name) = row.unpack();
                    let ordinal: i32 = ordinal;

                    Ok((
                        ordinal as usize,
                        PgEdgeCardinal {
                            key,
                            ident,
                            def_col_name,
                            key_col_name,
                        },
                    ))
                })
                .try_collect()?;

            if edge_info.cardinals.len() != pg_cardinals.len() {
                todo!("adjust edge arity");
            }

            pg_edge.cardinals = pg_cardinals;
        } else {
            ctx.steps.push((
                domain_ids,
                MigrationStep::DeployEdge {
                    edge_tag,
                    table_name,
                },
            ));

            for (index, _cardinal) in edge_info.cardinals.iter().enumerate() {
                let ordinal: u16 = index.try_into()?;

                // FIXME: ontology must provide human readable name for cardinal
                let ident = format!("todo_{ordinal}").into_boxed_str();
                let def_col_name = format!("def_{ordinal}").into_boxed_str();
                let key_col_name = format!("key_{ordinal}").into_boxed_str();

                ctx.steps.push((
                    domain_ids,
                    MigrationStep::DeployEdgeCardinal {
                        edge_tag,
                        ordinal,
                        ident,
                        def_col_name,
                        key_col_name,
                    },
                ))
            }
        }
    }

    Ok(())
}

async fn migrate_vertex_steps<'t>(
    domain_ids: PgDomainIds,
    vertex_def_id: DefId,
    def: &Def,
    entity: &Entity,
    ontology: &Ontology,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let name = &ontology[entity.name];
    let table_name = format!("v_{}", name).into_boxed_str();
    let pg_domain = ctx.domains.get_mut(&domain_ids.pkg_id).unwrap();

    if let Some(datatable) = pg_domain.datatables.get_mut(&vertex_def_id) {
        let pg_fields = txn
            .query(
                indoc! {"
                    SELECT key, rel_tag, pg_type, column_name
                    FROM m6m_reg.datafield
                    WHERE datatable_key = $1
                "},
                &[&datatable.key],
            )
            .await
            .context("read datatables")?
            .into_iter()
            .map(|row| -> anyhow::Result<_> {
                let _key: PgRegKey = row.get(0);
                let rel_tag = DefRelTag(row.get::<_, i32>(1).try_into()?);
                let pg_type = row.get(2);
                let col_name: Box<str> = row.get(3);

                Ok((rel_tag, PgDataField { col_name, pg_type }))
            })
            .try_collect()?;

        if datatable.table_name != table_name {
            ctx.steps.push((
                domain_ids,
                MigrationStep::RenameDataTable {
                    def_id: vertex_def_id,
                    old: datatable.table_name.clone(),
                    new: table_name.clone(),
                },
            ));
        }

        datatable.data_fields = pg_fields;
    } else {
        ctx.steps.push((
            domain_ids,
            MigrationStep::DeployVertex {
                vertex_def_id,
                table_name: table_name.clone(),
            },
        ));
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
            .get(&vertex_def_id)
            .and_then(|datatable| datatable.data_fields.get(&rel_tag));

        // FIXME: should mix columns on the root _and_ child structures,
        // so there needs to be some disambiguation in place
        let column_name = ontology[rel.name].to_string().into_boxed_str();

        let pg_type = match rel.target {
            DataRelationshipTarget::Unambiguous(def_id) => match get_pg_type(def_id, ontology)? {
                Some(pg_type) => pg_type,
                None => {
                    // This is a unit type, does not need to be represented
                    continue;
                }
            },
            DataRelationshipTarget::Union(_) => {
                return Err(anyhow!("FIXME: union target for {rel_tag:?}"));
            }
        };

        if let Some(pg_data_field) = pg_data_field {
            assert_eq!(pg_data_field.col_name, column_name, "TODO: rename column");
            assert_eq!(
                pg_data_field.pg_type, pg_type,
                "TODO: change data field pg_type",
            );
        } else {
            ctx.steps.push((
                domain_ids,
                MigrationStep::DeployDataField {
                    datatable_def_id: vertex_def_id,
                    rel_tag,
                    pg_type,
                    column_name,
                },
            ));
        }
    }

    Ok(())
}

fn get_pg_type(def_id: DefId, ontology: &Ontology) -> anyhow::Result<Option<PgType>> {
    let def = ontology.get_def(def_id).unwrap();
    let def_repr = match &def.kind {
        DefKind::Data(basic_def) => &basic_def.repr,
        _ => &DefRepr::Unknown,
    };

    match def_repr {
        DefRepr::Unit => Err(anyhow!("TODO: ignore unit column")),
        DefRepr::I64 => Ok(Some(PgType::BigInt)),
        DefRepr::F64 => Ok(Some(PgType::DoublePrecision)),
        DefRepr::Serial => Ok(Some(PgType::Bigserial)),
        DefRepr::Boolean => Ok(Some(PgType::Boolean)),
        DefRepr::Text => Ok(Some(PgType::Text)),
        DefRepr::Octets => Ok(Some(PgType::Bytea)),
        DefRepr::DateTime => Ok(Some(PgType::Timestamp)),
        DefRepr::FmtStruct(Some((_rel_id, def_id))) => get_pg_type(*def_id, ontology),
        DefRepr::FmtStruct(None) => Ok(None),
        DefRepr::Seq => todo!("seq"),
        DefRepr::Struct => todo!("struct"),
        DefRepr::Intersection(_) => todo!("intersection"),
        DefRepr::Union(..) => todo!("union"),
        DefRepr::Unknown => Err(anyhow!("unknown repr: {def_id:?}")),
    }
}
