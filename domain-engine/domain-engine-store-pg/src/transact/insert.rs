use std::collections::hash_map::Entry;

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    transact::DataOperation,
    DomainResult,
};
use fnv::FnvHashMap;
use futures_util::{future::BoxFuture, TryStreamExt};
use ontol_runtime::{
    attr::Attr,
    ontology::domain::{DataRelationshipInfo, DataRelationshipKind, Def},
    query::select::Select,
    value::Value,
    RelId,
};
use pin_utils::pin_mut;
use tracing::{debug, trace, warn};

use crate::{
    map_row_error,
    pg_error::{PgDataError, PgInputError},
    pg_model::{InDomain, PgType},
    sql::{self, TableName},
    sql_record::{SqlColumnStream, SqlRecordIterator},
    sql_value::Layout,
};

use super::{
    data::{Data, RowValue, ScalarAttrs},
    edge_patch::{EdgeEndoTuplePatch, EdgePatches},
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

        let pkg_id = value.pkg_id;
        let pg_domain = self.pg_model.pg_domain(pkg_id)?;
        let analyzed = self.analyze_struct(value, def)?;

        // TODO: prepared statement for each entity type/select
        let mut insert = sql::Insert {
            with: None,
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
                    &mut insert.returning,
                    None,
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
                .client()
                .query_raw(&sql, analyzed.root_attrs.as_params())
                .await
                .map_err(PgDataError::InsertQuery)?;
            pin_mut!(stream);

            let row = stream
                .try_next()
                .await
                .map_err(map_row_error)?
                .ok_or(PgDataError::NothingInserted)?;

            stream
                .try_next()
                .await
                .map_err(PgDataError::InsertRowStreamNotClosed)?;

            match stream.rows_affected() {
                Some(affected) => {
                    if affected != 1 {
                        return Err(PgDataError::InsertIncorrectAffectCount.into());
                    }
                }
                None => {
                    return Err(PgDataError::InsertNoRowsAffected.into());
                }
            }

            row
        };

        let mut row = SqlColumnStream::new(&row);
        let data_key = row
            .next_field(&Layout::Scalar(PgType::BigInt))?
            .into_i64()?;

        self.patch_edges(analyzed.root_attrs.datatable, data_key, analyzed.edges)
            .await?;

        match select {
            Select::EntityId => {
                let sql_val = row.next_field(&layout[1])?.non_null()?;

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
