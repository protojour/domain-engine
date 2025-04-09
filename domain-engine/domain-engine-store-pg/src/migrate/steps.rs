use anyhow::anyhow;
use itertools::Itertools;
use ontol_runtime::{
    DefId, DefPropTag, DomainIndex, OntolDefTagExt,
    debug::OntolDebug,
    ontology::{
        aspects::DefsAspect,
        domain::{
            DataRelationshipKind, DataRelationshipTarget, DataTreeRepr, Def, DefRepr, Domain,
            EdgeCardinalFlags,
        },
    },
    property::ValueCardinality,
    tuple::CardinalIdx,
};
use tracing::{Instrument, error, trace_span};

use crate::{
    migrate::{MigrationStep, PgDomain},
    pg_error::PgMigrationError,
    pg_model::{
        EdgeId, PgEdgeCardinalKind, PgIndexType, PgProperty, PgPropertyData, PgRepr, PgTableIdUnion,
    },
};

use super::{MigrationCtx, PgDomainIds, Stage};

pub async fn migrate_domain_steps(
    domain_index: DomainIndex,
    domain: &Domain,
    ontology_defs: &DefsAspect,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let domain_id = domain.domain_id().id;
    let domain_ids = PgDomainIds {
        index: domain_index,
        id: domain_id,
    };
    let unique_name = &ontology_defs[domain.unique_name()];
    let pg_domain = ctx.domains.get_mut(&domain_index);

    let schema_name = {
        // disambiguate schema names with a unique number, in case domains have the same name.
        // (unique_name might disappear some day, as requiring unique names is unrealistic)
        let schema_disambiguator = if let Some(pg_domain) = pg_domain.as_ref() {
            pg_domain.key.unwrap()
        } else {
            let key = ctx.next_schema_disambiguator;
            ctx.next_schema_disambiguator += 1;
            key
        };
        format!("m6md{schema_disambiguator}_{unique_name}").into_boxed_str()
    };

    if let Some(pg_domain) = pg_domain {
        ctx.stats.domains_already_deployed += 1;

        if pg_domain.schema_name != schema_name {
            ctx.steps.extend(
                Stage::Domain,
                domain_ids,
                [MigrationStep::RenameDomainSchema {
                    old: pg_domain.schema_name.clone(),
                    new: schema_name,
                }],
            );
        }
    } else {
        ctx.stats.new_domains_deployed += 1;
        ctx.domains.insert(
            domain_index,
            PgDomain {
                key: None,
                schema_name: schema_name.clone(),
                datatables: Default::default(),
                edgetables: Default::default(),
                has_crdt: false,
            },
        );

        ctx.steps.extend(
            Stage::Domain,
            domain_ids,
            [MigrationStep::DeployDomain {
                name: ontology_defs[domain.unique_name()].into(),
                schema_name,
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
        let ident = &ontology_defs[ident];

        migrate_vertex_steps(domain_ids, def.id, def, ident, ontology_defs, ctx)
            .instrument(trace_span!("vtx", ident))
            .await?;
    }

    migrate_domain_edges_steps(domain_index, domain, domain_ids, ontology_defs, ctx).await?;

    if let Some(abstract_scalar_tags) = ctx.abstract_scalars.remove(&domain_index) {
        let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();

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
                        table_name: format!("s_{:?}", tag.debug(ontology_defs)).into_boxed_str(),
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

async fn migrate_vertex_steps(
    domain_ids: PgDomainIds,
    vertex_def_id: DefId,
    def: &Def,
    ident: &str,
    ontology_defs: &DefsAspect,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let table_name = format!("v_{}", ident).into_boxed_str();

    let pg_domain = ctx.domains.get_mut(&domain_ids.index).unwrap();

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
        ontology_defs,
        ctx,
    )?;

    let pg_domain = ctx.domains.get(&domain_ids.index).unwrap();
    if let Some(pg_table) = pg_domain.datatables.get(&vertex_def_id) {
        if pg_table.has_fkey != needs_fkey {
            return Err(anyhow!("fkey state has changed"));
        }
    }

    Ok(())
}

async fn migrate_domain_edges_steps(
    domain_index: DomainIndex,
    domain: &Domain,
    domain_ids: PgDomainIds,
    ontology_defs: &DefsAspect,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let pg_domain = ctx.domains.get_mut(&domain_index).unwrap();
    let mut param_def_ids: Vec<(EdgeId, DefId)> = vec![];
    for (edge_id, edge_info) in domain.edges() {
        let edge_tag = edge_id.tag();
        let ident = &ontology_defs[edge_info.ident];
        let table_name = format!("e_{ident}").into_boxed_str();

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

                param_def_ids.push((EdgeId(*edge_id), param_def_id));

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

    for (edge_id, data_def_id) in param_def_ids {
        migrate_datafields_steps(
            Stage::Edge,
            domain_ids,
            PgTableIdUnion::Edge(edge_id),
            ontology_defs.def(data_def_id),
            ontology_defs,
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
    ontology_defs: &DefsAspect,
    ctx: &mut MigrationCtx,
) -> anyhow::Result<()> {
    let pg_domain = ctx.domains.get_mut(&domain_ids.index).unwrap();
    let mut tree_relationships: Vec<_> = def
        .data_relationships
        .iter()
        .filter_map(|(prop_id, rel_info)| match &rel_info.kind {
            DataRelationshipKind::Id | DataRelationshipKind::Tree(_) => {
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
        let column_name = &ontology_defs[rel_info.name];

        let target_def_id = match rel_info.target {
            DataRelationshipTarget::Unambiguous(def_id) => def_id,
            DataRelationshipTarget::Union(def_id) => def_id,
        };

        match (
            pg_domain
                .get_table(&table_id)
                .and_then(|datatable| datatable.properties.get(prop_tag)),
            PgRepr::classify_property(rel_info, target_def_id, ontology_defs),
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
            (Some(PgProperty::AbstractStruct(_)), PgRepr::Abstract(DataTreeRepr::Plain), _) => {
                // OK
            }
            (Some(PgProperty::AbstractStruct(_)), PgRepr::Scalar(..), _) => {
                // OK
            }
            (Some(PgProperty::AbstractCrdt(_)), PgRepr::Abstract(DataTreeRepr::Crdt), _) => {
                // OK
            }
            (Some(PgProperty::Column(_)), PgRepr::Scalar(..), _) => {
                // OK
            }
            (Some(PgProperty::AbstractCrdt(_)), _, _) => {
                todo!("move out of CRDT")
            }
            (Some(PgProperty::AbstractStruct(_)), PgRepr::Abstract(DataTreeRepr::Crdt), _) => {
                todo!("convert to CRDT")
            }
            (Some(PgProperty::Column(_)), PgRepr::Abstract(_proto), _) => {
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
                        data: PgPropertyData::AbstractStruct,
                    }],
                );
                ctx.abstract_scalars
                    .entry(domain_ids.index)
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
            (_, PgRepr::CreatedAtColumn | PgRepr::UpdatedAtColumn, _) => {
                // has standard columns ("_created", "_updated")
            }
            (None, PgRepr::Abstract(DataTreeRepr::Plain), _) => {
                ctx.steps.extend(
                    stage,
                    domain_ids,
                    [MigrationStep::DeployProperty {
                        table_id,
                        prop_tag: *prop_tag,
                        data: PgPropertyData::AbstractStruct,
                    }],
                );
            }
            (None, PgRepr::Abstract(DataTreeRepr::Crdt), _) => {
                ctx.steps.extend(
                    stage,
                    domain_ids,
                    [MigrationStep::DeployProperty {
                        table_id,
                        prop_tag: *prop_tag,
                        data: PgPropertyData::AbstractCrdt,
                    }],
                );
                if !pg_domain.has_crdt {
                    ctx.steps.extend(
                        stage,
                        domain_ids,
                        [MigrationStep::DeployCrdt {
                            domain_index: domain_ids.index,
                        }],
                    );
                    pg_domain.has_crdt = true;
                }
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
