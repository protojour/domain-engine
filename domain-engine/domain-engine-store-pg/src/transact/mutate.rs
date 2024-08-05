use std::collections::hash_map::Entry;

use domain_engine_core::{
    domain_error::DomainErrorKind,
    entity_id_utils::{try_generate_entity_id, GeneratedId},
    object_generator::ObjectGenerator,
    transact::DataOperation,
    DomainError, DomainResult,
};
use fnv::FnvHashMap;
use futures_util::{future::BoxFuture, TryStreamExt};
use ontol_runtime::{
    attr::Attr, interface::serde::processor::ProcessorMode,
    ontology::domain::DataRelationshipTarget, query::select::Select, value::Value, DefId, RelId,
};
use pin_utils::pin_mut;
use tokio_postgres::types::ToSql;
use tracing::{debug, trace, warn};

use crate::{
    pg_model::{InDomain, PgDataKey},
    sql::{self, Param, TableName},
    transact::data::{Data, Scalar},
};

use super::{data::RowValue, TransactCtx};

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
        ObjectGenerator::new(ProcessorMode::Create, self.ontology, self.system)
            .generate_objects(&mut value.value);

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
                        let value_generator = entity.id_value_generator.ok_or_else(|| {
                            DomainError::data_store_bad_request(
                                "no id provided and no ID generator",
                            )
                        })?;

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
            table_name: TableName(
                &pg_domain.schema_name,
                &analyzed.root_attrs.datatable.table_name,
            ),
            column_names: analyzed.root_attrs.column_selection()?,
            returning: vec!["_key"],
        };

        match select {
            Select::EntityId => {
                let id_rel_tag = entity.id_relationship_id.tag();
                if let Some(field) = analyzed.root_attrs.datatable.data_fields.get(&id_rel_tag) {
                    insert.returning.push(&field.col_name);
                }
            }
            Select::Struct(sel) => {
                for rel_id in sel.properties.keys() {
                    if let Some(field) =
                        analyzed.root_attrs.datatable.data_fields.get(&rel_id.tag())
                    {
                        insert.returning.push(&field.col_name);
                    }
                }
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
                .map_err(|err| DomainError::data_store(format!("{err}")))?;
            pin_mut!(stream);

            stream
                .try_next()
                .await
                .map_err(|_| DomainError::data_store("could not fetch row"))?
                .ok_or_else(|| DomainError::data_store("no rows returned"))?
        };

        let key: PgDataKey = row.get(0);

        // write edges
        for (edge_id, projection) in analyzed.edge_projections {
            let subject_index = projection.subject;

            let pg_edge = pg_domain.edges.get(&edge_id.1).unwrap();

            let mut insert = sql::Insert {
                table_name: TableName(&pg_domain.schema_name, &pg_edge.table_name),
                column_names: vec![],
                returning: vec![],
            };

            for pg_cardinal in pg_edge.cardinals.values() {
                insert.column_names.extend([
                    pg_cardinal.def_col_name.as_ref(),
                    pg_cardinal.key_col_name.as_ref(),
                ]);
            }

            let sql = insert.to_string();

            let mut edge_row_values: Vec<Scalar> = vec![];

            for tuple in projection.tuples {
                edge_row_values.clear();
                let tuple_width = tuple.len();

                let subject_pair = [
                    Scalar::I32(analyzed.root_attrs.datatable.key),
                    Scalar::I64(key),
                ];

                for (index, value) in tuple.into_iter().enumerate() {
                    let (foreign_def_id, foreign_key) = self
                        .resolve_linked_vertex(value, InsertMode::Insert, Select::EntityId)
                        .await?;

                    if index == subject_index.0 as usize {
                        edge_row_values.extend(subject_pair.clone());
                    }

                    let datatable = self
                        .pg_model
                        .datatable(foreign_def_id.package_id(), foreign_def_id)?;

                    edge_row_values.extend([Scalar::I32(datatable.key), Scalar::I64(foreign_key)]);
                }

                // subject at the end of the tuple
                if subject_index.0 as usize == tuple_width {
                    edge_row_values.extend(subject_pair);
                }

                debug!("{sql}");
                self.txn
                    .query_raw(&sql, edge_row_values.iter().map(|sc| sc as &dyn ToSql))
                    .await
                    .map_err(|e| {
                        DomainError::data_store(format!("unable to insert edge: {e:?}"))
                    })?;
            }
        }

        match select {
            Select::EntityId => {
                let scalar: Scalar = row.get(1);
                trace!("deserialized entity ID: {scalar:?}");
                Ok(RowValue {
                    value: self.deserialize_scalar(entity.id_value_def_id, scalar)?,
                    key,
                    op: DataOperation::Inserted,
                })
            }
            Select::Struct(sel) => {
                let mut attrs: FnvHashMap<RelId, Attr> =
                    FnvHashMap::with_capacity_and_hasher(sel.properties.len(), Default::default());

                for (idx, rel_id) in sel.properties.keys().enumerate() {
                    let scalar: Scalar = row.get(idx + 1);
                    let data_relationship = def.data_relationships.get(rel_id).unwrap();

                    match data_relationship.target {
                        DataRelationshipTarget::Unambiguous(def_id) => {
                            attrs.insert(
                                *rel_id,
                                Attr::Unit(self.deserialize_scalar(def_id, scalar)?),
                            );
                        }
                        DataRelationshipTarget::Union(_) => {}
                    }
                }

                Ok(RowValue {
                    value: Value::Struct(Box::new(attrs), def.id.into()),
                    key,
                    op: DataOperation::Inserted,
                })
            }
            _ => Ok(RowValue {
                value: Value::unit(),
                key,
                op: DataOperation::Inserted,
            }),
        }
    }

    async fn resolve_linked_vertex(
        &self,
        value: Value,
        mode: InsertMode,
        select: Select,
    ) -> DomainResult<(DefId, PgDataKey)> {
        let def_id = value.type_def_id();

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

            Ok((def_id, row_value.key))
        } else if let Some(entity_def_id) = self.pg_model.entity_id_to_entity.get(&def_id) {
            let pg_domain = self.pg_model.pg_domain(entity_def_id.package_id())?;
            let pg_datatable = self
                .pg_model
                .datatable(entity_def_id.package_id(), *entity_def_id)?;
            let entity = self.ontology.def(*entity_def_id).entity().unwrap();

            let id_field = pg_datatable.find_data_field(&entity.id_relationship_id)?;

            let select = sql::Select {
                expressions: vec![sql::Expression::Column("_key")],
                from: vec![sql::TableName(&pg_domain.schema_name, &pg_datatable.table_name).into()],
                where_: Some(sql::Expression::Eq(
                    Box::new(sql::Expression::Column(&id_field.col_name)),
                    Box::new(sql::Expression::Param(Param(0))),
                )),
                limit: None,
            };

            let Data::Scalar(id_param) = self.data_from_value(value)? else {
                return Err(DomainError::data_store_bad_request("compound foreign key"));
            };

            let sql = select.to_string();
            debug!("{sql}");

            let row = self
                .txn
                .query_opt(&sql, &[&id_param])
                .await
                .map_err(|e| {
                    DomainError::data_store(format!("could not look up foreign key: {e:?}"))
                })?
                .ok_or_else(|| {
                    let value = match self.deserialize_scalar(def_id, id_param) {
                        Ok(value) => value,
                        Err(error) => return error,
                    };
                    DomainErrorKind::UnresolvedForeignKey(self.ontology.format_value(&value))
                        .into_error()
                })?;

            let key: PgDataKey = row.get(0);

            Ok((*entity_def_id, key))
        } else {
            Err(DomainError::data_store_bad_request("bad foreign key"))
        }
    }
}
