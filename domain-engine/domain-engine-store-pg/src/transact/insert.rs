use std::collections::{hash_map::Entry, BTreeMap};

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    transact::DataOperation,
    DomainResult,
};
use fnv::FnvHashMap;
use futures_util::{future::BoxFuture, TryStreamExt};
use itertools::Itertools;
use ontol_runtime::{attr::Attr, query::select::Select, value::Value, DefId, EdgeId, RelId};
use pin_utils::pin_mut;
use tokio_postgres::types::ToSql;
use tracing::{debug, error, info, trace, warn};

use crate::{
    ds_bad_req, ds_err,
    pg_model::{InDomain, PgDataKey, PgDomain, PgEdgeCardinalKind, PgTable, PgType},
    sql::{self, TableName},
    sql_value::{Layout, RowDecodeIterator, SqlVal},
    transact::data::Data,
};

use super::{data::RowValue, struct_analyzer::EdgeProjection, TransactCtx};

pub enum InsertMode {
    Insert,
    Upsert,
}

impl<'a> TransactCtx<'a> {
    /// Returns BoxFuture because of potential recursion
    pub fn insert_vertex(
        &self,
        value: InDomain<Value>,
        mode: InsertMode,
        select: &'a Select,
    ) -> BoxFuture<'_, DomainResult<RowValue>> {
        Box::pin(async move {
            let _def_id = value.value.type_def_id();
            self.insert_vertex_impl(value, mode, select)
                // .instrument(debug_span!("ins", id = ?def_id))
                .await
        })
    }

    async fn insert_vertex_impl(
        &self,
        mut value: InDomain<Value>,
        mode: InsertMode,
        select: &'a Select,
    ) -> DomainResult<RowValue> {
        let def_id = value.type_def_id();
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(value.type_def_id()).into_error()
        })?;

        if let Value::Struct(map, _) = &mut value.value {
            if let Entry::Vacant(vacant) = map.entry(entity.id_relationship_id) {
                match mode {
                    InsertMode::Insert => {
                        let value_generator = entity
                            .id_value_generator
                            .ok_or_else(|| ds_bad_req("no id provided and no ID generator"))?;

                        let (generated_id, _container) = try_generate_entity_id(
                            entity.id_operator_addr,
                            value_generator,
                            self.ontology,
                            self.system,
                        )?;
                        if let GeneratedId::Generated(value) = generated_id {
                            vacant.insert(Attr::Unit(value));
                        }
                    }
                    InsertMode::Upsert => {
                        return Err(DomainErrorKind::InherentIdNotFound.into_error())
                    }
                }
            }
        }

        let pkg_id = value.pkg_id;
        let pg_domain = self.pg_model.pg_domain(pkg_id)?;
        let analyzed = self.analyze_struct(value, def)?;

        // TODO: prepared statement for each entity type/select
        let mut insert = sql::Insert {
            into: TableName(
                &pg_domain.schema_name,
                &analyzed.root_attrs.datatable.table_name,
            ),
            column_names: analyzed.root_attrs.column_selection()?,
            on_conflict: None,
            returning: vec![sql::Expr::path1("_key")],
        };
        let mut layout: Vec<Layout> = vec![Layout::Scalar(PgType::BigInt)];

        match select {
            Select::EntityId => {
                let id_rel_tag = entity.id_relationship_id.tag();
                if let Some(field) = analyzed.root_attrs.datatable.data_fields.get(&id_rel_tag) {
                    insert
                        .returning
                        .push(sql::Expr::path1(field.col_name.as_ref()));
                    layout.push(Layout::Scalar(field.pg_type));
                }
            }
            Select::Struct(_sel) => {
                self.select_inherent_struct_fields(
                    def,
                    analyzed.root_attrs.datatable,
                    None,
                    &mut insert.returning,
                    &mut layout,
                )?;
            }
            _ => {
                todo!()
            }
        }

        let row = {
            let sql = insert.to_string();
            debug!("{sql}");

            let stream = self
                .txn
                .query_raw(&sql, analyzed.root_attrs.as_params())
                .await
                .map_err(|err| ds_err(format!("{err}")))?;
            pin_mut!(stream);

            stream
                .try_next()
                .await
                .map_err(|e| {
                    if let Some(db_error) = e.as_db_error() {
                        if db_error
                            .message()
                            .starts_with("duplicate key value violates unique constraint")
                        {
                            DomainErrorKind::EntityAlreadyExists.into_error()
                        } else {
                            info!("row fetch error: {db_error:?}");
                            ds_err("could not fetch row")
                        }
                    } else {
                        error!("row fetch error: {e:?}");
                        ds_err("could not fetch row")
                    }
                })?
                .ok_or_else(|| ds_err("no rows returned"))?
        };

        let mut row = RowDecodeIterator::new(&row, &layout);
        let data_key = SqlVal::next_column(&mut row)?.into_i64()?;

        self.write_edges(
            pg_domain,
            analyzed.root_attrs.datatable,
            data_key,
            analyzed.edge_projections,
        )
        .await?;

        match select {
            Select::EntityId => {
                let sql_val = SqlVal::next_column(&mut row)?.non_null()?;

                trace!("deserialized entity ID: {sql_val:?}");
                Ok(RowValue {
                    value: self.deserialize_sql(entity.id_value_def_id, sql_val)?,
                    data_key,
                    op: DataOperation::Inserted,
                })
            }
            Select::Struct(sel) => {
                let mut attrs: FnvHashMap<RelId, Attr> =
                    FnvHashMap::with_capacity_and_hasher(sel.properties.len(), Default::default());

                self.read_inherent_struct_fields(def, &mut row, &mut attrs)?;

                Ok(RowValue {
                    value: Value::Struct(Box::new(attrs), def.id.into()),
                    data_key,
                    op: DataOperation::Inserted,
                })
            }
            _ => Ok(RowValue {
                value: Value::unit(),
                data_key,
                op: DataOperation::Inserted,
            }),
        }
    }

    async fn write_edges(
        &self,
        pg_domain: &PgDomain,
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
            Object(usize, Dynamic),
            Parameters,
        }

        let mut edge_params: Vec<SqlVal> = vec![];

        for (edge_id, projection) in edge_projections {
            let subject_index = projection.subject;

            let pg_edge = pg_domain.edges.get(&edge_id.1).unwrap();

            let mut sql_insert = sql::Insert {
                into: TableName(&pg_domain.schema_name, &pg_edge.table_name),
                column_names: vec![],
                on_conflict: None,
                returning: vec![],
            };

            let projected_cardinals = pg_edge
                .edge_cardinals
                .iter()
                .map(|(index, pg_cardinal)| match &pg_cardinal.kind {
                    PgEdgeCardinalKind::Dynamic {
                        def_col_name,
                        key_col_name,
                    } => {
                        sql_insert
                            .column_names
                            .extend([def_col_name.as_ref(), key_col_name]);
                        if *index == subject_index.0 as usize {
                            ProjectedEdgeCardinal::Subject(Dynamic::Yes)
                        } else {
                            ProjectedEdgeCardinal::Object(*index, Dynamic::Yes)
                        }
                    }
                    PgEdgeCardinalKind::Unique { key_col_name, .. } => {
                        sql_insert.column_names.push(key_col_name);
                        if *index == subject_index.0 as usize {
                            ProjectedEdgeCardinal::Subject(Dynamic::No)
                        } else {
                            ProjectedEdgeCardinal::Object(*index, Dynamic::No)
                        }
                    }
                    PgEdgeCardinalKind::Parameters => ProjectedEdgeCardinal::Parameters,
                })
                .collect_vec();

            let sql = sql_insert.to_string();
            debug!("{sql}");

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
                            todo!("PARAMETERS");
                        }
                    }
                }

                self.txn
                    .query_raw(&sql, edge_params.iter().map(|param| param as &dyn ToSql))
                    .await
                    .map_err(|e| ds_err(format!("unable to insert edge: {e:?}")))?;
            }
        }

        Ok(())
    }

    async fn resolve_linked_vertex(
        &self,
        value: Value,
        mode: InsertMode,
        select: Select,
    ) -> DomainResult<(DefId, PgDataKey)> {
        let def_id = value.type_def_id();

        debug!(
            "resolve linked vertex: {def_id:?} table: {:#?}",
            self.pg_model.entity_id_to_entity
        );

        if self
            .pg_model
            .find_datatable(def_id.package_id(), def_id)
            .is_some()
        {
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
        } else if let Some(entity_def_id) = self.pg_model.entity_id_to_entity.get(&def_id) {
            let pg = self
                .pg_model
                .pg_domain_table(entity_def_id.package_id(), *entity_def_id)?;

            let entity = self.ontology.def(*entity_def_id).entity().unwrap();
            let pg_id_field = pg.table.field(&entity.id_relationship_id)?;

            let Data::Sql(id_param) = self.data_from_value(value)? else {
                return Err(ds_bad_req("compound foreign key"));
            };

            let sql = if entity.is_self_identifying {
                // upsert
                // TODO: It might actually be better to do SELECT + optional INSERT.
                // this approach (DO UPDATE SET) always re-appends the row.
                sql::Insert {
                    into: pg.table_name(),
                    column_names: vec![&pg_id_field.col_name],
                    on_conflict: Some(sql::OnConflict {
                        target: Some(sql::ConflictTarget::Columns(vec![&pg_id_field.col_name])),
                        action: sql::ConflictAction::DoUpdateSet(vec![sql::UpdateColumn(
                            &pg_id_field.col_name,
                            sql::Expr::param(0),
                        )]),
                    }),
                    returning: vec![sql::Expr::path1("_key")],
                }
                .to_string()
            } else {
                sql::Select {
                    expressions: vec![sql::Expr::path1("_key")],
                    from: vec![pg.table_name().into()],
                    where_: Some(sql::Expr::eq(
                        sql::Expr::path1(pg_id_field.col_name.as_ref()),
                        sql::Expr::param(0),
                    )),
                    ..Default::default()
                }
                .to_string()
            };

            debug!("{sql}");

            let row = self
                .txn
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

            let key: PgDataKey = row.get(0);

            Ok((*entity_def_id, key))
        } else {
            Err(ds_bad_req("bad foreign key"))
        }
    }
}
