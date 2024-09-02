use anyhow::anyhow;
use itertools::Itertools;
use ontol_runtime::{
    debug::OntolDebug,
    ontology::{
        domain::{
            DataRelationshipKind, DataRelationshipTarget, Def, DefRepr, Domain, EdgeCardinalFlags,
        },
        Ontology,
    },
    property::ValueCardinality,
    tuple::CardinalIdx,
    DefId, DefPropTag, EdgeId, PackageId,
};
use tracing::{error, info, trace_span, Instrument};

use crate::{
    migrate::{MigrationStep, PgDomain},
    pg_error::PgMigrationError,
    pg_model::{
        PgEdgeCardinalKind, PgIndexType, PgProperty, PgPropertyData, PgRepr, PgTableIdUnion,
    },
};

use super::{MigrationCtx, PgDomainIds, Stage};

pub async fn migrate_domain_steps<'t>(
    pkg_id: PackageId,
    domain: &Domain,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let domain_uid = domain.domain_id().ulid;
    let domain_ids = PgDomainIds {
        pkg_id,
        uid: domain_uid,
    };
    let unique_name = &ontology[domain.unique_name()];
    let schema = format!("m6md_{unique_name}").into_boxed_str();

    if let Some(pg_domain) = ctx.domains.get_mut(&pkg_id) {
        info!("domain already deployed");

        if pg_domain.schema_name != schema {
            ctx.steps.extend(
                Stage::Domain,
                domain_ids,
                [MigrationStep::RenameDomainSchema {
                    old: pg_domain.schema_name.clone(),
                    new: schema,
                }],
            );
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

        ctx.steps.extend(
            Stage::Domain,
            domain_ids,
            [MigrationStep::DeployDomain {
                name: ontology[domain.unique_name()].into(),
                schema_name: schema,
            }],
        );
    };

    for def in domain.defs() {
        match def.repr() {
            // Struct means vertex in this context
            Some(DefRepr::Struct) => {}
            _ => continue,
        }
        let Some(ident) = def.ident() else {
            continue;
        };
        let ident = &ontology[ident];

        migrate_vertex_steps(domain_ids, def.id, def, ident, ontology, ctx)
            .instrument(trace_span!("vtx", ident))
            .await?;
    }

    migrate_domain_edges_steps(pkg_id, domain, domain_ids, ontology, ctx).await?;

    if let Some(abstract_scalar_tags) = ctx.abstract_scalars.remove(&pkg_id) {
        let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();

        for (tag, pg_type) in abstract_scalar_tags {
            if pg_domain.datatables.contains_key(&tag.def_id()) {
                continue;
            }

            ctx.steps.extend(
                Stage::Vertex,
                domain_ids,
                [
                    MigrationStep::DeployVertex {
                        vertex_def_id: tag.def_id(),
                        table_name: format!("s_{:?}", tag.debug(ontology)).into_boxed_str(),
                    },
                    MigrationStep::DeployVertexFKey {
                        vertex_def_id: tag.def_id(),
                    },
                    MigrationStep::DeployProperty {
                        table_id: PgTableIdUnion::Def(tag.def_id()),
                        prop_tag: DefPropTag(0),
                        data: PgPropertyData::Scalar {
                            col_name: "value".to_string().into_boxed_str(),
                            pg_type,
                        },
                    },
                ],
            );
        }
    }

    Ok(())
}

async fn migrate_vertex_steps<'t>(
    domain_ids: PgDomainIds,
    vertex_def_id: DefId,
    def: &Def,
    ident: &str,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let table_name = format!("v_{}", ident).into_boxed_str();
    let pg_domain = ctx.domains.get_mut(&domain_ids.pkg_id).unwrap();

    let exists = if let Some(datatable) = pg_domain.datatables.get_mut(&vertex_def_id) {
        if datatable.table_name != table_name {
            ctx.steps.extend(
                Stage::Vertex,
                domain_ids,
                [MigrationStep::RenameDataTable {
                    def_id: vertex_def_id,
                    old: datatable.table_name.clone(),
                    new: table_name.clone(),
                }],
            );
        }

        true
    } else {
        ctx.steps.extend(
            Stage::Vertex,
            domain_ids,
            [MigrationStep::DeployVertex {
                vertex_def_id,
                table_name: table_name.clone(),
            }],
        );
        false
    };

    let id_count = def
        .data_relationships
        .values()
        .filter(|rel_info| matches!(&rel_info.kind, DataRelationshipKind::Id))
        .count();
    let needs_fkey = id_count == 0;
    if !exists && needs_fkey {
        ctx.steps.extend(
            Stage::Vertex,
            domain_ids,
            [MigrationStep::DeployVertexFKey { vertex_def_id }],
        );
    }

    migrate_datafields_steps(
        Stage::Vertex,
        domain_ids,
        PgTableIdUnion::Def(vertex_def_id),
        def,
        ontology,
        ctx,
    )?;

    let pg_domain = ctx.domains.get(&domain_ids.pkg_id).unwrap();
    if let Some(pg_table) = pg_domain.datatables.get(&vertex_def_id) {
        if pg_table.has_fkey != needs_fkey {
            return Err(anyhow!("fkey state has changed"));
        }
    }

    Ok(())
}

async fn migrate_domain_edges_steps<'t>(
    pkg_id: PackageId,
    domain: &Domain,
    domain_ids: PgDomainIds,
    ontology: &Ontology,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let pg_domain = ctx.domains.get_mut(&pkg_id).unwrap();
    let mut data_def_ids: Vec<(EdgeId, DefId)> = vec![];
    for (edge_id, edge_info) in domain.edges() {
        let edge_tag = edge_id.1;
        let table_name = format!("e_{edge_tag}").into_boxed_str();

        if let Some(pg_table) = pg_domain.edgetables.get_mut(&edge_tag) {
            if edge_info.cardinals.len() != pg_table.edge_cardinals.len() {
                todo!("adjust edge arity");
            }
        } else {
            ctx.steps.extend(
                Stage::Edge,
                domain_ids,
                [MigrationStep::DeployEdge {
                    edge_tag,
                    table_name,
                }],
            );
        }

        for (index, cardinal) in edge_info.cardinals.iter().enumerate() {
            let index = CardinalIdx(index.try_into()?);
            let ident = format!("cardinal{index}").into_boxed_str();
            let mut index_type: Option<PgIndexType> = None;

            let edge_cardinal_kind = if cardinal.flags.contains(EdgeCardinalFlags::ENTITY) {
                // FIXME: ontology must provide human readable name for cardinal
                let key_col_name = format!("_key{index}").into_boxed_str();

                if cardinal.flags.contains(EdgeCardinalFlags::PINNED_DEF) {
                    PgEdgeCardinalKind::PinnedDef {
                        key_col_name,
                        pinned_def_id: *cardinal.target.iter().next().unwrap(),
                    }
                } else {
                    index_type = Some(if cardinal.flags.contains(EdgeCardinalFlags::UNIQUE) {
                        PgIndexType::Unique
                    } else {
                        PgIndexType::BTree
                    });

                    let def_col_name = format!("_def{index}").into_boxed_str();
                    PgEdgeCardinalKind::Dynamic {
                        key_col_name,
                        def_col_name,
                    }
                }
            } else {
                if cardinal.target.len() != 1 {
                    Err(PgMigrationError::AmbiguousEdgeCardinal)?;
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
                        Err(PgMigrationError::CardinalChangeDetected)?;
                    }
                }

                if deployed_pg_cardinal.index_type != index_type {
                    Err(PgMigrationError::ChangeIndexTypeNotImplemented)?;
                }
            } else {
                ctx.steps.extend(
                    Stage::Edge,
                    domain_ids,
                    [MigrationStep::DeployEdgeCardinal {
                        edge_tag,
                        index,
                        ident,
                        kind: edge_cardinal_kind,
                        index_type,
                    }],
                );
            }
        }
    }

    for (edge_id, data_def_id) in data_def_ids {
        migrate_datafields_steps(
            Stage::Edge,
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
    stage: Stage,
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
        .filter_map(|(prop_id, rel_info)| match &rel_info.kind {
            DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                Some((prop_id.tag(), rel_info))
            }
            DataRelationshipKind::Edge(_) => None,
        })
        .collect_vec();

    tree_relationships.sort_by_key(|(prop_tag, _)| prop_tag.0);

    // properties
    for (prop_tag, rel_info) in &tree_relationships {
        // FIXME: should mix columns on the root _and_ child structures,
        // so there needs to be some disambiguation in place
        let column_name = &ontology[rel_info.name];

        let target_def_id = match rel_info.target {
            DataRelationshipTarget::Unambiguous(def_id) => def_id,
            DataRelationshipTarget::Union(def_id) => def_id,
        };

        match (
            pg_domain
                .get_table(&table_id)
                .and_then(|datatable| datatable.properties.get(prop_tag)),
            PgRepr::classify(target_def_id, ontology),
            rel_info.cardinality.1,
        ) {
            (_, PgRepr::Unit, _) => {
                // This is a unit type, does not need to be represented
            }
            (
                Some(PgProperty::Column(pg_column)),
                PgRepr::Scalar(pg_type, _),
                ValueCardinality::Unit,
            ) => {
                assert_eq!(
                    pg_column.col_name.as_ref(),
                    column_name,
                    "TODO: rename column"
                );
                assert_eq!(
                    pg_column.pg_type, pg_type,
                    "TODO: change data field pg_type",
                );
            }
            (Some(PgProperty::Abstract(_)), PgRepr::Abstract, _) => {
                // OK
            }
            (Some(PgProperty::Abstract(_)), PgRepr::Scalar(..), _) => {
                // OK
            }
            (Some(PgProperty::Column(_)), PgRepr::Scalar(..), _) => {
                // OK
            }
            (Some(PgProperty::Column(_)), PgRepr::Abstract, _) => {
                todo!("migrate from column to abstract")
            }
            (
                None,
                PgRepr::Scalar(pg_type, scalar_tag),
                ValueCardinality::IndexSet | ValueCardinality::List,
            ) => {
                ctx.steps.extend(
                    stage,
                    domain_ids,
                    [MigrationStep::DeployProperty {
                        table_id,
                        prop_tag: *prop_tag,
                        data: PgPropertyData::Abstract,
                    }],
                );
                ctx.abstract_scalars
                    .entry(domain_ids.pkg_id)
                    .or_default()
                    .insert(scalar_tag, pg_type);
            }
            (None, PgRepr::Scalar(pg_type, _), _) => {
                ctx.steps.extend(
                    stage,
                    domain_ids,
                    [MigrationStep::DeployProperty {
                        table_id,
                        prop_tag: *prop_tag,
                        data: PgPropertyData::Scalar {
                            col_name: column_name.to_string().into_boxed_str(),
                            pg_type,
                        },
                    }],
                );
            }
            (None, PgRepr::Abstract, _) => {
                ctx.steps.extend(
                    stage,
                    domain_ids,
                    [MigrationStep::DeployProperty {
                        table_id,
                        prop_tag: *prop_tag,
                        data: PgPropertyData::Abstract,
                    }],
                );
            }
            (_, PgRepr::NotSupported(msg), _) => {
                error!("pg repr error for {table_id:?}:`{column_name}`: `{msg:?}`");
                // return Err(PgMigrationError::IncompatibleProperty(
                //     PropId(def.id, *prop_tag),
                //     column_name.to_string().into_boxed_str(),
                // )
                // .into());
            }
        }
    }

    // indexes
    for (prop_tag, rel_info) in &tree_relationships {
        if let DataRelationshipKind::Id = &rel_info.kind {
            let index_type = PgIndexType::Unique;

            let DataRelationshipTarget::Unambiguous(def_id) = rel_info.target else {
                return Err(PgMigrationError::AmbiguousId(def.id).into());
            };

            let pg_datatable = pg_domain.get_table(&table_id);

            let pg_index_data = pg_datatable
                .and_then(|datatable| datatable.property_indexes.get(&(def_id, index_type)));

            match pg_index_data {
                Some(pg_index_data) => {
                    let pg_datatable = &pg_domain.get_table(&table_id).unwrap();
                    for property_key in &pg_index_data.property_keys {
                        assert!(pg_datatable.column_by_key(*property_key).is_some());
                    }
                }
                None => {
                    ctx.steps.extend(
                        stage,
                        domain_ids,
                        [MigrationStep::DeployPropertyIndex {
                            table_id,
                            index_def_id: def_id,
                            index_type,
                            field_tuple: vec![*prop_tag],
                        }],
                    );
                }
            }
        }
    }

    Ok(())
}
