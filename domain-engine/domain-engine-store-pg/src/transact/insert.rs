use std::collections::hash_map::Entry;

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    transact::DataOperation,
    DomainResult,
};
use futures_util::{future::BoxFuture, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipInfo, DataRelationshipKind, Def},
    query::select::Select,
    value::Value,
    RelId,
};
use pin_utils::pin_mut;
use tracing::{debug, warn};

use crate::{
    pg_error::{map_row_error, PgError, PgInputError},
    pg_model::{InDomain, PgType},
    sql::{self},
    sql_record::SqlColumnStream,
    sql_value::Layout,
};

use super::{
    data::{Data, RowValue, ScalarAttrs},
    edge_patch::{EdgeEndoTuplePatch, EdgePatches},
    query::{QueryBuildCtx, QueryContext, QuerySelect},
    InsertMode, MutationMode, TransactCtx,
};

pub struct AnalyzedStruct<'m, 'b> {
    pub root_attrs: ScalarAttrs<'m, 'b>,
    pub edges: EdgePatches,
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
        self.preprocess_insert_value(mode, &mut value.value)?;

        let mut layout = vec![];
        let (sql, analyzed, query_select) =
            self.prepare_insert_query(value, select, &mut layout)?;
        debug!("{sql}");

        let row = {
            let stream = self
                .client()
                .query_raw(&sql, analyzed.root_attrs.as_params())
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

        let row_value = self.read_row_value_as_vertex(
            SqlColumnStream::new(&row),
            Some(query_select),
            DataOperation::Inserted,
        )?;

        self.patch_edges(
            analyzed.root_attrs.datatable,
            row_value.data_key,
            analyzed.edges,
        )
        .await?;

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

    // TODO: This should not depend on the value
    fn prepare_insert_query(
        &self,
        value: InDomain<Value>,
        select: &'a Select,
        layout: &mut Vec<Layout>,
    ) -> DomainResult<(String, AnalyzedStruct, QuerySelect<'a>)> {
        let def_id = value.type_def_id();
        let def = self.ontology.def(def_id);
        let entity = def.entity().ok_or_else(|| {
            warn!("not an entity");
            DomainErrorKind::NotAnEntity(value.type_def_id()).into_error()
        })?;

        let pkg_id = value.pkg_id;
        let pg = self.pg_model.pg_domain_datatable(pkg_id, def_id)?;
        let analyzed = self.analyze_struct(value, def)?;

        let mut ctx = QueryBuildCtx::default();
        let root_alias = ctx.alias;
        ctx.with_def_aliases.insert(def_id, root_alias);

        let mut returning = vec![];

        let query_select = match select {
            Select::EntityId => {
                let id_rel_tag = entity.id_relationship_id.tag();
                if let Some(field) = analyzed.root_attrs.datatable.data_fields.get(&id_rel_tag) {
                    returning.extend(self.initial_standard_data_fields(pg));
                    returning.push(sql::Expr::path1(field.col_name.as_ref()));
                    layout.extend([
                        Layout::Scalar(PgType::Integer),
                        Layout::Scalar(PgType::BigInt),
                        Layout::Scalar(field.pg_type),
                    ]);
                }
                QuerySelect::Field(entity.id_relationship_id)
            }
            Select::Struct(sel) => {
                let (.., expressions) = self.sql_select_expressions(
                    def_id,
                    &sel.properties,
                    QueryContext::Insert,
                    pg,
                    &mut ctx,
                )?;
                returning.extend(expressions);
                QuerySelect::Struct(&sel.properties)
            }
            _ => {
                todo!()
            }
        };

        // TODO: prepared statement for each entity type/select
        let insert = sql::Insert {
            with: ctx.with(),
            into: pg.table_name(),
            as_: Some(root_alias),
            column_names: analyzed.root_attrs.column_selection()?,
            on_conflict: None,
            returning,
        };

        Ok((insert.to_string(), analyzed, query_select))
    }

    pub(super) fn analyze_struct(
        &self,
        value: InDomain<Value>,
        def: &Def,
    ) -> DomainResult<AnalyzedStruct> {
        let datatable = self.pg_model.datatable(value.pkg_id, value.type_def_id())?;

        let Value::Struct(attrs, _struct_tag) = value.value else {
            return Err(DomainErrorKind::EntityMustBeStruct.into_error());
        };

        let mut root_attrs = ScalarAttrs {
            map: Default::default(),
            datatable,
        };

        let mut edge_patches = EdgePatches::default();

        for (rel_id, attr) in *attrs {
            let rel_info = find_data_relationship(def, &rel_id)?;

            match (rel_info.kind, attr) {
                (DataRelationshipKind::Id | DataRelationshipKind::Tree, Attr::Unit(value)) => {
                    match self.data_from_value(value)? {
                        Data::Sql(scalar) => {
                            root_attrs.map.insert(rel_id, scalar);
                        }
                        Data::Compound(comp) => {
                            todo!("compound: {comp:?}");
                        }
                    }
                }
                (DataRelationshipKind::Edge(proj), attr) => {
                    let patch = edge_patches.patch(proj.id, proj.subject);

                    match attr {
                        Attr::Unit(value) => {
                            if patch.tuples.is_empty() {
                                patch.tuples.push(EdgeEndoTuplePatch { elements: vec![] });
                            }
                            patch.tuples[0].insert_element(
                                proj.object,
                                value,
                                MutationMode::insert(),
                            )?;
                        }
                        Attr::Tuple(tuple) => {
                            patch.tuples.push(EdgeEndoTuplePatch::from_tuple(
                                tuple
                                    .elements
                                    .into_iter()
                                    .map(|val| (val, MutationMode::insert())),
                            ));
                        }
                        Attr::Matrix(matrix) => {
                            patch.tuples.extend(matrix.into_rows().map(|tuple| {
                                EdgeEndoTuplePatch::from_tuple(
                                    tuple
                                        .elements
                                        .into_iter()
                                        .map(|val| (val, MutationMode::insert())),
                                )
                            }))
                        }
                    }
                }
                _ => {
                    debug!("edge ignored");
                }
            }
        }

        Ok(AnalyzedStruct {
            root_attrs,
            edges: edge_patches,
        })
    }
}

fn find_data_relationship<'d>(
    def: &'d Def,
    rel_id: &RelId,
) -> DomainResult<&'d DataRelationshipInfo> {
    Ok(def
        .data_relationships
        .get(rel_id)
        .ok_or(PgInputError::DataRelationshipNotFound(*rel_id))?)
}
