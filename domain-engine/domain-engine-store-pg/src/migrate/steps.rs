use std::collections::BTreeMap;

use anyhow::{anyhow, Context};
use fnv::FnvHashMap;
use indoc::indoc;
use itertools::Itertools;
use ontol_runtime::{
    ontology::{
        domain::{
            DataRelationshipKind, DataRelationshipTarget, Def, Domain, EdgeCardinalFlags, Entity,
        },
        Ontology,
    },
    tuple::CardinalIdx,
    DefId, DefRelTag, EdgeId, PackageId,
};
use tokio_postgres::Transaction;
use tracing::info;

use crate::{
    migrate::{MigrationStep, PgDomain},
    pg_model::{
        PgDataField, PgEdgeCardinal, PgEdgeCardinalKind, PgIndexData, PgIndexType, PgRegKey,
        PgTable, PgTableIdUnion, PgType,
    },
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
    let schema = format!("m6md_{unique_name}").into_boxed_str();

    let row = txn
        .query_opt(
            "SELECT key, schema_name FROM m6mreg.domain WHERE uid = $1",
            &[&domain_uid],
        )
        .await?;

    if let Some(row) = row {
        info!("domain already deployed");

        let domain_key: PgRegKey = row.get(0);
        let schema_name = row.get(1);

        let mut pg_datatables: FnvHashMap<DefId, PgTable> = Default::default();
        let mut pg_edges: FnvHashMap<u16, PgTable> = Default::default();

        for row in txn
            .query(
                indoc! {"
                SELECT key, def_domain_key, def_tag, edge_tag, table_name
                FROM m6mreg.domaintable
                WHERE domain_key = $1
            "},
                &[&domain_key],
            )
            .await?
        {
            let key: PgRegKey = row.get(0);
            let def_domain_key: Option<PgRegKey> = row.get(1);
            let def_tag: Option<u16> = row
                .get::<_, Option<i32>>(2)
                .map(|tag: i32| tag.try_into())
                .transpose()?;
            let edge_tag: Option<u16> = row
                .get::<_, Option<i32>>(3)
                .map(|tag: i32| tag.try_into())
                .transpose()?;
            let table_name: Box<str> = row.get(4);

            let pg_table = PgTable {
                key,
                table_name,
                data_fields: Default::default(),
                edge_cardinals: Default::default(),
                datafield_indexes: Default::default(),
            };

            match (def_domain_key, def_tag, edge_tag) {
                (Some(def_domain_key), Some(def_tag), None) => {
                    assert_eq!(def_domain_key, domain_key);
                    pg_datatables.insert(DefId(pkg_id, def_tag), pg_table);
                }
                (None, None, Some(edge_tag)) => {
                    pg_edges.insert(edge_tag, pg_table);
                }
                _ => unreachable!(),
            }
        }

        let pg_domain = PgDomain {
            key: Some(domain_key),
            schema_name,
            datatables: pg_datatables,
            edgetables: pg_edges,
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
                edgetables: Default::default(),
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

    migrate_domain_edges_steps(pkg_id, domain, domain_ids, ontology, txn, ctx).await?;

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
                    FROM m6mreg.datafield
                    WHERE domaintable_key = $1
                "},
                &[&datatable.key],
            )
            .await
            .context("read data fields")?
            .into_iter()
            .map(|row| -> anyhow::Result<_> {
                let key: PgRegKey = row.get(0);
                let rel_tag = DefRelTag(row.get::<_, i32>(1).try_into()?);
                let pg_type = row.get(2);
                let col_name: Box<str> = row.get(3);

                Ok((
                    rel_tag,
                    PgDataField {
                        key,
                        col_name,
                        pg_type,
                    },
                ))
            })
            .try_collect()?;

        let pg_indexes: FnvHashMap<(DefId, PgIndexType), PgIndexData> = txn
            .query(
                indoc! {"
                    SELECT
                        domaintable_key,
                        def_domain_key,
                        def_tag,
                        index_type,
                        datafield_keys
                    FROM m6mreg.domaintable_index
                    WHERE domaintable_key = $1
                "},
                &[&datatable.key],
            )
            .await
            .context("read indexes")?
            .into_iter()
            .map(|row| -> anyhow::Result<_> {
                let _domaintable_key: PgRegKey = row.get(0);
                let def_domain_key: PgRegKey = row.get(1);
                let def_tag: u16 = row.get::<_, i32>(2).try_into()?;
                let index_type: PgIndexType = row.get(3);
                let datafield_keys: Vec<PgRegKey> = row.get(4);

                assert_eq!(def_domain_key, pg_domain.key.unwrap());
                let index_def_id = DefId(def.id.package_id(), def_tag);

                Ok(((index_def_id, index_type), PgIndexData { datafield_keys }))
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
        datatable.datafield_indexes = pg_indexes;
    } else {
        ctx.steps.push((
            domain_ids,
            MigrationStep::DeployVertex {
                vertex_def_id,
                table_name: table_name.clone(),
            },
        ));
    }

    migrate_datafields_steps(
        domain_ids,
        PgTableIdUnion::Def(vertex_def_id),
        def,
        ontology,
        ctx,
    )?;

    Ok(())
}

async fn migrate_domain_edges_steps<'t>(
    pkg_id: PackageId,
    domain: &Domain,
    domain_ids: PgDomainIds,
    ontology: &Ontology,
    txn: &Transaction<'t>,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
    let mut data_def_ids: Vec<(EdgeId, DefId)> = vec![];
    for (edge_id, edge_info) in domain.edges() {
        let edge_tag = edge_id.1;
        let table_name = format!("e_{edge_tag}").into_boxed_str();

        if let Some(pg_table) = pg_domain.edgetables.get_mut(&edge_tag) {
            let pg_cardinals: BTreeMap<CardinalIdx, PgEdgeCardinal> = txn
                .query(
                    indoc! {"
                        SELECT key, ordinal, ident, def_column_name, pinned_domaintable_key, key_column_name
                        FROM m6mreg.edgecardinal
                        WHERE domaintable_key = $1
                        ORDER BY ordinal
                    "},
                    &[&pg_table.key],
                )
                .await?
                .into_iter()
                .map(|row| -> anyhow::Result<_> {
                    let key = row.get(0);
                    let ordinal: i32 = row.get(1);
                    let ident = row.get(2);
                    let def_col_name: Option<Box<str>> = row.get(3);
                    let pinned_domaintable_key: Option<PgRegKey> = row.get(4);
                    let key_col_name: Option<Box<str>> = row.get(5);

                    let pinned_domaintable_def_id = pinned_domaintable_key.map(|key|
                        *pg_domain.datatables.iter().find(|(_, dt)| dt.key == key).unwrap()
                            .0
                    );

                    Ok((
                        CardinalIdx(ordinal.try_into()?),
                        PgEdgeCardinal {
                            key,
                            ident,
                            kind: match (key_col_name, pinned_domaintable_def_id, def_col_name) {
                                (Some(key_col_name), Some(pinned_domaintable_def_id), None) => {
                                    PgEdgeCardinalKind::PinnedDef {
                                        def_id: pinned_domaintable_def_id,
                                        key_col_name
                                    }
                                }
                                (Some(key_col_name), None, Some(def_col_name)) => {
                                    PgEdgeCardinalKind::Dynamic { def_col_name, key_col_name }
                                }
                                (None, None, None) => {
                                    // updated below
                                    PgEdgeCardinalKind::Parameters(DefId::unit())
                                }
                                _ => {
                                    unreachable!()
                                }
                            }
                        },
                    ))
                })
                .try_collect()?;

            if edge_info.cardinals.len() != pg_cardinals.len() {
                todo!("adjust edge arity");
            }

            pg_table.edge_cardinals = pg_cardinals;
        } else {
            ctx.steps.push((
                domain_ids,
                MigrationStep::DeployEdge {
                    edge_tag,
                    table_name,
                },
            ));
        }

        for (index, cardinal) in edge_info.cardinals.iter().enumerate() {
            let index = CardinalIdx(index.try_into()?);
            let ident = format!("cardinal{index}").into_boxed_str();

            let edge_cardinal_kind = if cardinal.flags.contains(EdgeCardinalFlags::ENTITY) {
                // FIXME: ontology must provide human readable name for cardinal
                let key_col_name = format!("_key{index}").into_boxed_str();

                if cardinal.flags.contains(EdgeCardinalFlags::PINNED_DEF) {
                    PgEdgeCardinalKind::PinnedDef {
                        key_col_name,
                        def_id: *cardinal.target.iter().next().unwrap(),
                    }
                } else {
                    let def_col_name = format!("_def{index}").into_boxed_str();
                    PgEdgeCardinalKind::Dynamic {
                        key_col_name,
                        def_col_name,
                    }
                }
            } else {
                if cardinal.target.len() != 1 {
                    return Err(anyhow!("data cardinal should have unambiguous DefId"));
                }
                let param_def_id = *cardinal.target.iter().next().unwrap();

                data_def_ids.push((*edge_id, param_def_id));

                PgEdgeCardinalKind::Parameters(param_def_id)
            };

            if let Some(deployed_pg_cardinal) = pg_domain
                .edgetables
                .get_mut(&edge_tag)
                .and_then(|edge_table| edge_table.edge_cardinals.get_mut(&index))
            {
                match (&mut deployed_pg_cardinal.kind, &edge_cardinal_kind) {
                    (
                        deployed @ PgEdgeCardinalKind::Dynamic { .. },
                        generated @ PgEdgeCardinalKind::Dynamic { .. },
                    ) => {
                        assert_eq!(deployed, generated);
                    }
                    (
                        deployed @ PgEdgeCardinalKind::PinnedDef { .. },
                        generated @ PgEdgeCardinalKind::PinnedDef { .. },
                    ) => {
                        assert_eq!(deployed, generated);
                    }
                    (
                        PgEdgeCardinalKind::Parameters(param_def_id),
                        PgEdgeCardinalKind::Parameters(generated_param_def_id),
                    ) => {
                        *param_def_id = *generated_param_def_id;
                    }
                    _ => {
                        return Err(anyhow!("TODO: cardinal change detected"));
                    }
                }
            } else {
                ctx.steps.push((
                    domain_ids,
                    MigrationStep::DeployEdgeCardinal {
                        edge_tag,
                        index,
                        ident,
                        kind: edge_cardinal_kind,
                        index_type: None,
                    },
                ));
            }
        }
    }

    for (edge_id, data_def_id) in data_def_ids {
        migrate_datafields_steps(
            domain_ids,
            PgTableIdUnion::Edge(edge_id),
            ontology.def(data_def_id),
            ontology,
            ctx,
        )?;
    }

    Ok(())
}

fn migrate_datafields_steps(
    domain_ids: PgDomainIds,
    table_id: PgTableIdUnion,
    def: &Def,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let pg_domain = ctx.domains.get_mut(&domain_ids.pkg_id).unwrap();
    let mut tree_relationships: Vec<_> = def
        .data_relationships
        .iter()
        .filter_map(|(rel_id, rel)| match &rel.kind {
            DataRelationshipKind::Id | DataRelationshipKind::Tree => Some((rel_id.tag(), rel)),
            DataRelationshipKind::Edge(_) => None,
        })
        .collect_vec();

    tree_relationships.sort_by_key(|(rel_tag, _)| rel_tag.0);

    // fields
    for (rel_tag, rel_info) in &tree_relationships {
        let pg_data_field = pg_domain
            .get_table(&table_id)
            .and_then(|datatable| datatable.data_fields.get(rel_tag));

        // FIXME: should mix columns on the root _and_ child structures,
        // so there needs to be some disambiguation in place
        let column_name = ontology[rel_info.name].to_string().into_boxed_str();

        let pg_type = match rel_info.target {
            DataRelationshipTarget::Unambiguous(def_id) => {
                match PgType::from_def_id(def_id, ontology).map_err(|err| anyhow!("{err:?}"))? {
                    Some(pg_type) => pg_type,
                    None => {
                        // This is a unit type, does not need to be represented
                        continue;
                    }
                }
            }
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
                    table_id,
                    rel_tag: *rel_tag,
                    pg_type,
                    column_name,
                },
            ));
        }
    }

    // indexes
    for (rel_tag, rel_info) in &tree_relationships {
        if let DataRelationshipKind::Id = &rel_info.kind {
            let index_type = PgIndexType::Unique;

            let DataRelationshipTarget::Unambiguous(def_id) = rel_info.target else {
                return Err(anyhow!("the ID must be unambiguously typed"));
            };

            let pg_datatable = pg_domain.get_table(&table_id);

            let pg_index_data = pg_datatable
                .and_then(|datatable| datatable.datafield_indexes.get(&(def_id, index_type)));

            match pg_index_data {
                Some(pg_index_data) => {
                    let pg_datatable = &pg_domain.get_table(&table_id).unwrap();
                    for datafield_key in &pg_index_data.datafield_keys {
                        assert!(pg_datatable.field_by_key(*datafield_key).is_some());
                    }
                }
                None => {
                    ctx.steps.push((
                        domain_ids,
                        MigrationStep::DeployDataIndex {
                            table_id,
                            index_def_id: def_id,
                            index_type,
                            field_tuple: vec![*rel_tag],
                        },
                    ));
                }
            }
        }
    }

    Ok(())
}
