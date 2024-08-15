use std::{collections::hash_map::Entry, ops::Deref};

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    transact::DataOperation,
    DomainResult,
};
use futures_util::{future::BoxFuture, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipKind, Def},
    query::select::Select,
    value::Value,
    DefId, PackageId,
};
use pin_utils::pin_mut;
use tracing::{debug, warn};

use crate::{
    pg_error::{map_row_error, PgError, PgInputError},
    pg_model::{InDomain, PgTable},
    sql::{self},
    sql_record::SqlColumnStream,
    sql_value::SqlVal,
    statement::{Prepare, PreparedStatement},
    transact::query::IncludeEdgeAttrs,
};

use super::{
    data::{Data, RowValue},
    edge_patch::{EdgeEndoTuplePatch, EdgePatches},
    query::{QueryBuildCtx, QuerySelect},
    InsertMode, MutationMode, TransactCtx,
};

#[derive(Clone)]
pub struct PreparedInsert {
    inherent_stmt: PreparedStatement,
    edge_select_stmt: Option<PreparedStatement>,
}

struct AnalyzedInput<'a, 'b> {
    pub field_buf: Vec<SqlVal<'b>>,
    pub edges: EdgePatches,
    pub query_select: QuerySelect<'a>,
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
        let pkg_id = value.pkg_id;
        let def_id = value.value.type_def_id();

        let prepared = if let Some(prepared) =
            self.stmt_cache_locked(|c| c.insert.get(&(pkg_id, def_id)).cloned())
        {
            prepared
        } else {
            let prepared = self.prepare_insert(pkg_id, def_id, select).await?;
            self.stmt_cache_locked(|c| c.insert.insert((pkg_id, def_id), prepared.clone()));
            prepared
        };

        self.preprocess_insert_value(mode, &mut value.value)?;

        let def = self.ontology.def(value.value.type_def_id());
        let (analyzed, pg_table) = self.analyze_input(value, def, select)?;

        let row = {
            debug!("{}", prepared.inherent_stmt);

            let stream = self
                .client()
                .query_raw(prepared.inherent_stmt.deref(), &analyzed.field_buf)
                .await
                .map_err(PgError::InsertQuery)?;
            pin_mut!(stream);

            let row = stream
                .try_next()
                .await
                .map_err(map_row_error)?
                .ok_or(PgError::NothingInserted)?;

            stream
                .try_next()
                .await
                .map_err(PgError::InsertRowStreamNotClosed)?;

            match stream.rows_affected() {
                Some(affected) => {
                    if affected != 1 {
                        return Err(PgError::InsertIncorrectAffectCount.into());
                    }
                }
                None => {
                    return Err(PgError::InsertNoRowsAffected.into());
                }
            }

            row
        };

        let mut row_value = self.read_row_value_as_vertex(
            SqlColumnStream::new(&row),
            Some(analyzed.query_select),
            IncludeEdgeAttrs::No,
            DataOperation::Inserted,
        )?;

        self.patch_edges(pg_table, row_value.data_key, analyzed.edges)
            .await?;

        if let (Some(edge_select_stmt), QuerySelect::Struct(properties)) =
            (prepared.edge_select_stmt, analyzed.query_select)
        {
            debug!("{edge_select_stmt}");

            let row = self
                .client()
                .query_one(
                    edge_select_stmt.deref(),
                    &[&SqlVal::I64(row_value.data_key)],
                )
                .await
                .map_err(PgError::InsertEdgeFetch)?;

            let mut column_stream = SqlColumnStream::new(&row);

            let Value::Struct(attrs, _) = &mut row_value.value else {
                unreachable!();
            };

            self.read_edge_attributes(&mut column_stream, def, properties, attrs.as_mut())?;
        }

        Ok(row_value)
    }

    fn preprocess_insert_value(&self, mode: InsertMode, value: &mut Value) -> DomainResult<()> {
        let def_id = value.type_def_id();
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(value.type_def_id()).into_error()
        })?;

        if let Value::Struct(map, _) = value {
            if let Entry::Vacant(vacant) = map.entry(entity.id_relationship_id) {
                match mode {
                    InsertMode::Insert => {
                        let value_generator = entity
                            .id_value_generator
                            .ok_or(PgInputError::MissingValueWithoutGenerator)?;

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

        Ok(())
    }

    pub async fn prepare_insert(
        &self,
        pkg_id: PackageId,
        def_id: DefId,
        select: &'a Select,
    ) -> DomainResult<PreparedInsert> {
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(def_id).into_error()
        })?;

        let pg = self.pg_model.pg_domain_datatable(pkg_id, def_id)?;

        let mut ctx = QueryBuildCtx::default();
        let root_alias = ctx.alias;
        ctx.with_def_aliases.insert(def_id, root_alias);

        let mut edge_select_stmt: Option<PreparedStatement> = None;

        let mut insert_returning = vec![];
        let mut column_names = vec![];

        // insert columns in the order of data relationships
        for (rel_id, rel_info) in &def.data_relationships {
            if matches!(
                &rel_info.kind,
                DataRelationshipKind::Id | DataRelationshipKind::Tree
            ) {
                if let Some(pg_field) = pg.table.data_fields.get(&rel_id.1) {
                    if !pg_field.pg_type.skip_insert() {
                        column_names.push(pg_field.col_name.as_ref());
                    }
                }
            }
        }

        // RETURNING is based on select, etc
        match select {
            Select::EntityId => {
                let id_rel_tag = entity.id_relationship_id.tag();
                if let Some(field) = pg.table.data_fields.get(&id_rel_tag) {
                    insert_returning.extend(self.initial_standard_data_fields(pg));
                    insert_returning.push(sql::Expr::path1(field.col_name.as_ref()));
                }
            }
            Select::Struct(sel) => {
                insert_returning.extend(self.initial_standard_data_fields(pg));
                let select_stats =
                    self.select_inherent_struct_fields(def, pg.table, &mut insert_returning, None)?;

                if select_stats.edge_count > 0 {
                    let mut ctx = QueryBuildCtx::default();
                    let root_alias = ctx.alias;
                    let mut expressions = sql::Expressions {
                        items: vec![],
                        multiline: true,
                    };

                    self.sql_select_edge_properties(
                        def,
                        root_alias,
                        &sel.properties,
                        pg,
                        &mut ctx,
                        &mut expressions.items,
                    )?;

                    if !expressions.items.is_empty() {
                        edge_select_stmt = Some(
                            sql::Select {
                                with: ctx.with(),
                                expressions,
                                from: vec![sql::FromItem::TableNameAs(
                                    pg.table_name(),
                                    sql::Name::Alias(root_alias),
                                )],
                                where_: Some(sql::Expr::eq(
                                    sql::Expr::path1("_key"),
                                    sql::Expr::param(0),
                                )),
                                limit: sql::Limit {
                                    limit: Some(1),
                                    offset: None,
                                },
                            }
                            .to_string()
                            .prepare(self.client())
                            .await?,
                        );
                    }
                }
            }
            _ => {
                todo!()
            }
        };

        let insert = sql::Insert {
            with: ctx.with(),
            into: pg.table_name(),
            as_: None,
            column_names,
            on_conflict: None,
            returning: insert_returning,
        };

        Ok(PreparedInsert {
            inherent_stmt: insert.to_string().prepare(self.client()).await?,
            edge_select_stmt,
        })
    }

    fn analyze_input(
        &self,
        value: InDomain<Value>,
        def: &Def,
        select: &'a Select,
    ) -> DomainResult<(AnalyzedInput<'a, '_>, &PgTable)> {
        let pg_table = self.pg_model.datatable(value.pkg_id, value.type_def_id())?;

        let Value::Struct(mut attrs, _struct_tag) = value.value else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        let mut field_buf: Vec<SqlVal> = vec![];
        let mut edge_patches = EdgePatches::default();

        for (rel_id, rel_info) in &def.data_relationships {
            match rel_info.kind {
                DataRelationshipKind::Id | DataRelationshipKind::Tree => {
                    let Some(pg_field) = pg_table.data_fields.get(&rel_id.1) else {
                        continue;
                    };
                    if pg_field.pg_type.skip_insert() {
                        continue;
                    }

                    match attrs.remove(rel_id) {
                        Some(Attr::Unit(value)) => match self.data_from_value(value)? {
                            Data::Sql(scalar) => {
                                field_buf.push(scalar);
                            }
                            Data::Compound(comp) => {
                                todo!("compound: {comp:?}");
                            }
                        },
                        None => {
                            field_buf.push(SqlVal::Null);
                        }
                        Some(_) => {
                            debug!("edge ignored");
                        }
                    }
                }
                DataRelationshipKind::Edge(proj) => {
                    let patch = edge_patches.patch(proj.id, proj.subject);

                    match attrs.remove(rel_id) {
                        Some(Attr::Unit(value)) => {
                            if patch.tuples.is_empty() {
                                patch.tuples.push(EdgeEndoTuplePatch { elements: vec![] });
                            }
                            patch.tuples[0].insert_element(
                                proj.object,
                                value,
                                MutationMode::insert(),
                            )?;
                        }
                        Some(Attr::Tuple(tuple)) => {
                            patch.tuples.push(EdgeEndoTuplePatch::from_tuple(
                                tuple
                                    .elements
                                    .into_iter()
                                    .map(|val| (val, MutationMode::insert())),
                            ));
                        }
                        Some(Attr::Matrix(matrix)) => {
                            patch.tuples.extend(matrix.into_rows().map(|tuple| {
                                EdgeEndoTuplePatch::from_tuple(
                                    tuple
                                        .elements
                                        .into_iter()
                                        .map(|val| (val, MutationMode::insert())),
                                )
                            }))
                        }
                        None => {}
                    }
                }
            }
        }

        let query_select = match select {
            Select::EntityId => {
                let entity = def.entity().ok_or_else(|| {
                    warn!("not an entity");
                    DomainErrorKind::NotAnEntity(def.id).into_error()
                })?;

                QuerySelect::Field(entity.id_relationship_id)
            }
            Select::Struct(sel) => QuerySelect::Struct(&sel.properties),
            _ => todo!(),
        };

        Ok((
            AnalyzedInput {
                field_buf,
                edges: edge_patches,
                query_select,
            },
            pg_table,
        ))
    }
}
