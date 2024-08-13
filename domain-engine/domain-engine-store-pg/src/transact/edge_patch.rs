use std::collections::{BTreeMap, BTreeSet};

use domain_engine_core::{domain_error::DomainErrorKind, DomainResult};
use futures_util::{TryFutureExt, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipKind, DataRelationshipTarget},
    query::select::Select,
    tuple::CardinalIdx,
    value::Value,
    DefId, EdgeId, RelId,
};
use postgres_types::ToSql;
use tracing::{debug, trace};

use crate::{
    ds_bad_req, ds_err, map_row_error,
    pg_model::{InDomain, PgDataKey, PgEdgeCardinalKind, PgTable},
    sql,
    sql_value::SqlVal,
    transact::{data::Data, insert::InsertMode},
};

use super::{struct_analyzer::EdgeProjection, TransactCtx};

impl<'a> TransactCtx<'a> {
    pub async fn patch_edges(
        &self,
        subject_datatable: &PgTable,
        subject_data_key: PgDataKey,
        edge_projections: BTreeMap<EdgeId, EdgeProjection>,
    ) -> DomainResult<()> {
        enum Dynamic {
            Yes,
            No,
        }

        enum ProjectedEdgeCardinal {
            Subject(Dynamic),
            Object(CardinalIdx, Dynamic),
            Parameters,
        }

        let mut edge_params: Vec<SqlVal> = vec![];

        for (edge_id, projection) in edge_projections {
            let subject_index = projection.subject;

            let pg_edge = self.pg_model.pg_domain_edgetable(&edge_id)?;

            let mut sql_insert = sql::Insert {
                into: pg_edge.table_name(),
                column_names: vec![],
                on_conflict: None,
                returning: vec![],
            };

            let mut param_order: BTreeSet<(RelId, DefId)> = Default::default();
            let mut projected_cardinals: Vec<ProjectedEdgeCardinal> = vec![];

            for (index, pg_cardinal) in &pg_edge.table.edge_cardinals {
                match &pg_cardinal.kind {
                    PgEdgeCardinalKind::Dynamic {
                        def_col_name,
                        key_col_name,
                    } => {
                        sql_insert
                            .column_names
                            .extend([def_col_name.as_ref(), key_col_name]);
                        projected_cardinals.push(if *index == subject_index {
                            ProjectedEdgeCardinal::Subject(Dynamic::Yes)
                        } else {
                            ProjectedEdgeCardinal::Object(*index, Dynamic::Yes)
                        });
                    }
                    PgEdgeCardinalKind::Unique { key_col_name, .. } => {
                        sql_insert.column_names.push(key_col_name);
                        projected_cardinals.push(if *index == subject_index {
                            ProjectedEdgeCardinal::Subject(Dynamic::No)
                        } else {
                            ProjectedEdgeCardinal::Object(*index, Dynamic::No)
                        });
                    }
                    PgEdgeCardinalKind::Parameters(params_def_id) => {
                        let def = self.ontology.def(*params_def_id);
                        for (rel_id, rel_info) in &def.data_relationships {
                            if let DataRelationshipKind::Tree = &rel_info.kind {
                                match &rel_info.target {
                                    DataRelationshipTarget::Unambiguous(def_id) => {
                                        let Some(pg_field) =
                                            pg_edge.table.data_fields.get(&rel_id.1)
                                        else {
                                            return Err(ds_err(
                                                "no data store field for edge parameter",
                                            ));
                                        };

                                        sql_insert.column_names.push(&pg_field.col_name);
                                        param_order.insert((*rel_id, *def_id));
                                    }
                                    DataRelationshipTarget::Union(_) => {
                                        todo!("union in params");
                                    }
                                }
                            }
                        }
                        projected_cardinals.push(ProjectedEdgeCardinal::Parameters);
                    }
                }
            }

            let sql = sql_insert.to_string();

            for tuple in projection.tuples {
                edge_params.clear();

                let mut element_iter = tuple.into_elements();

                for projected_cardinal in &projected_cardinals {
                    match projected_cardinal {
                        ProjectedEdgeCardinal::Subject(Dynamic::Yes) => {
                            edge_params.extend([
                                SqlVal::I32(subject_datatable.key),
                                SqlVal::I64(subject_data_key),
                            ]);
                        }
                        ProjectedEdgeCardinal::Subject(Dynamic::No) => {
                            edge_params.push(SqlVal::I64(subject_data_key));
                        }
                        ProjectedEdgeCardinal::Object(_index, dynamic) => {
                            let (foreign_def_id, foreign_key) = self
                                .resolve_linked_vertex(
                                    element_iter.next().unwrap(),
                                    InsertMode::Insert,
                                    Select::EntityId,
                                )
                                .await?;

                            if matches!(dynamic, Dynamic::Yes) {
                                let datatable = self
                                    .pg_model
                                    .datatable(foreign_def_id.package_id(), foreign_def_id)?;
                                edge_params.push(SqlVal::I32(datatable.key));
                            }

                            edge_params.push(SqlVal::I64(foreign_key));
                        }
                        ProjectedEdgeCardinal::Parameters => {
                            let Some(Value::Struct(mut map, _)) = element_iter.next() else {
                                return Err(ds_bad_req("edge params must be a struct"));
                            };

                            for (rel_id, _def_id) in &param_order {
                                let data = match map.remove(rel_id) {
                                    Some(Attr::Unit(value)) => self.data_from_value(value)?,
                                    Some(_) => {
                                        return Err(ds_bad_req(
                                            "non-scalar attribute in edge parameter",
                                        ))
                                    }
                                    None => Data::Sql(SqlVal::Null),
                                };

                                match data {
                                    Data::Sql(sql_val) => {
                                        edge_params.push(sql_val);
                                    }
                                    Data::Compound(_) => {
                                        return Err(ds_bad_req(
                                            "non-scalar value in edge parameter",
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }

                debug!("{sql}");
                trace!("{edge_params:?}");

                self.client()
                    .query_raw(&sql, edge_params.iter().map(|param| param as &dyn ToSql))
                    .await
                    .map_err(|e| ds_err(format!("unable to insert edge(1): {e:?}")))?
                    .try_collect::<IgnoreRows>()
                    .map_err(map_row_error)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn resolve_linked_vertex(
        &self,
        value: Value,
        mode: InsertMode,
        select: Select,
    ) -> DomainResult<(DefId, PgDataKey)> {
        let def_id = value.type_def_id();

        enum ResolveMode {
            VertexData,
            Id(RelId, IdResolveMode),
        }

        enum IdResolveMode {
            Plain,
            SelfIdentifying,
        }

        let (resolve_mode, vertex_def_id, value) = if self
            .pg_model
            .find_datatable(def_id.package_id(), def_id)
            .is_some()
        {
            let entity = self.ontology.def(def_id).entity().unwrap();

            if entity.is_self_identifying {
                let Value::Struct(mut map, _) = value else {
                    return Err(ds_bad_req("must be a struct"));
                };
                let Some(Attr::Unit(id)) = map.remove(&entity.id_relationship_id) else {
                    return Err(ds_bad_req("self-identifying vertex without id"));
                };

                (
                    ResolveMode::Id(entity.id_relationship_id, IdResolveMode::SelfIdentifying),
                    def_id,
                    id,
                )
            } else {
                (ResolveMode::VertexData, def_id, value)
            }
        } else if let Some(vertex_def_id) = self.pg_model.entity_id_to_entity.get(&def_id) {
            let entity = self.ontology.def(*vertex_def_id).entity().unwrap();
            let id_rel_id = entity.id_relationship_id;
            let id_resolve_mode = if entity.is_self_identifying {
                IdResolveMode::SelfIdentifying
            } else {
                IdResolveMode::Plain
            };

            (
                ResolveMode::Id(id_rel_id, id_resolve_mode),
                *vertex_def_id,
                value,
            )
        } else {
            return Err(ds_bad_req("bad foreign key"));
        };

        match resolve_mode {
            ResolveMode::VertexData => {
                let row_value = self
                    .insert_vertex(
                        InDomain {
                            pkg_id: def_id.package_id(),
                            value,
                        },
                        mode,
                        &select,
                    )
                    .await?;

                Ok((def_id, row_value.data_key))
            }
            ResolveMode::Id(id_rel_id, id_resolve_mode) => {
                let pg = self
                    .pg_model
                    .pg_domain_datatable(vertex_def_id.package_id(), vertex_def_id)?;

                let pg_id_field = pg.table.field(&id_rel_id)?;

                let Data::Sql(id_param) = self.data_from_value(value)? else {
                    return Err(ds_bad_req("compound foreign key"));
                };

                let sql = match id_resolve_mode {
                    IdResolveMode::Plain => sql::Select {
                        expressions: sql::Expressions {
                            items: vec![sql::Expr::path1("_key")],
                            multiline: false,
                        },
                        from: vec![pg.table_name().into()],
                        where_: Some(sql::Expr::eq(
                            sql::Expr::path1(pg_id_field.col_name.as_ref()),
                            sql::Expr::param(0),
                        )),
                        ..Default::default()
                    }
                    .to_string(),
                    IdResolveMode::SelfIdentifying => {
                        // upsert
                        // TODO: It might actually be better to do SELECT + optional INSERT.
                        // this approach (DO UPDATE SET) always re-appends the row.
                        sql::Insert {
                            into: pg.table_name(),
                            column_names: vec![&pg_id_field.col_name],
                            on_conflict: Some(sql::OnConflict {
                                target: Some(sql::ConflictTarget::Columns(vec![
                                    &pg_id_field.col_name,
                                ])),
                                action: sql::ConflictAction::DoUpdateSet(vec![sql::UpdateColumn(
                                    &pg_id_field.col_name,
                                    sql::Expr::param(0),
                                )]),
                            }),
                            returning: vec![sql::Expr::path1("_key")],
                        }
                        .to_string()
                    }
                };

                debug!("{sql}");

                let row = self
                    .client()
                    .query_opt(&sql, &[&id_param])
                    .await
                    .map_err(|e| ds_err(format!("could not look up foreign key: {e:?}")))?
                    .ok_or_else(|| {
                        let value = match self.deserialize_sql(def_id, id_param) {
                            Ok(value) => value,
                            Err(error) => return error,
                        };
                        DomainErrorKind::UnresolvedForeignKey(self.ontology.format_value(&value))
                            .into_error()
                    })?;

                Ok((vertex_def_id, row.get(0)))
            }
        }
    }
}

#[derive(Default)]
struct IgnoreRows;

impl<T> Extend<T> for IgnoreRows {
    fn extend<I: IntoIterator<Item = T>>(&mut self, _iter: I) {}
}
