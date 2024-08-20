use std::collections::BTreeSet;

use fnv::FnvHashMap;
use ontol_runtime::{
    attr::{Attr, AttrMatrix},
    ontology::domain::{DataRelationshipKind, Def, EdgeCardinalProjection},
    property::ValueCardinality,
    query::{
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    sequence::{Sequence, SubSequence},
    value::Value,
    DefId, PropId,
};
use tracing::{debug, debug_span, error};

use domain_engine_core::{domain_error::DomainErrorKind, DomainError, DomainResult};

use crate::{
    core::{find_data_relationship, DbContext, DynamicKey, EdgeData, EdgeVectorData},
    filter::FilterVal,
    sort::sort_props_vec,
};

use super::core::{InMemoryStore, VertexKey};

#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Cursor {
    offset: usize,
}

pub struct Limit(pub usize);

pub struct IncludeTotalLen(pub bool);

impl InMemoryStore {
    pub fn query_entities(
        &self,
        select: &EntitySelect,
        ctx: &DbContext,
    ) -> DomainResult<Sequence<Value>> {
        match &select.source {
            StructOrUnionSelect::Struct(struct_select) => self.query_single_vertex_collection(
                struct_select,
                &select.filter,
                Limit(select.limit),
                select
                    .after_cursor
                    .as_deref()
                    .map(bincode::deserialize)
                    .transpose()
                    .map_err(|_| DomainError::data_store("Invalid cursor format"))?,
                IncludeTotalLen(select.include_total_len),
                ctx,
            ),
            StructOrUnionSelect::Union(..) => Err(DomainError::data_store(
                "Query entity union at root level not supported",
            )),
        }
    }

    pub fn query_single_vertex_collection(
        &self,
        struct_select: &StructSelect,
        filter: &Filter,
        Limit(limit): Limit,
        after_cursor: Option<Cursor>,
        IncludeTotalLen(include_total_len): IncludeTotalLen,
        ctx: &DbContext,
    ) -> DomainResult<Sequence<Value>> {
        debug!("query single vertex collection: {struct_select:?}");
        let collection = self
            .vertices
            .get(&struct_select.def_id)
            .ok_or(DomainErrorKind::InvalidEntityDefId.into_error())?;

        let def = ctx.ontology.def(struct_select.def_id);
        let _entity = def
            .entity()
            .ok_or(DomainErrorKind::NotAnEntity(struct_select.def_id).into_error())?;

        // let filter_plan = compute_filter_plan(condition, &ctx.ontology).unwrap();
        debug!("eval filter: {filter}");

        let mut raw_props_vec = {
            let mut vec = vec![];

            for (key, props) in collection {
                let filter_val = FilterVal::Struct {
                    type_def_id: def.id,
                    dynamic_key: Some(key),
                    prop_tree: props,
                };
                if self.eval_condition(filter_val, filter.condition(), ctx)? {
                    vec.push((key, props.clone()));
                }
            }
            vec
        };

        sort_props_vec(&mut raw_props_vec, struct_select.def_id, filter, ctx)?;

        let total_size = raw_props_vec.len();

        let start_offset = match after_cursor {
            None => 0,
            Some(Cursor { offset }) => offset + 1,
        };

        if start_offset > 0 {
            raw_props_vec = raw_props_vec
                .into_iter()
                .skip(start_offset)
                .take(limit)
                .collect();
        } else if limit < total_size {
            raw_props_vec = raw_props_vec.into_iter().take(limit).collect();
        }

        let mut entity_sequence =
            Sequence::with_capacity(raw_props_vec.len()).with_sub(SubSequence {
                end_cursor: if limit > 0 {
                    Some(
                        bincode::serialize(&Cursor {
                            offset: start_offset + limit - 1,
                        })
                        .unwrap()
                        .into(),
                    )
                } else {
                    after_cursor
                        .map(|after_cursor| bincode::serialize(&after_cursor).unwrap().into())
                },
                has_next: start_offset + limit < total_size,
                total_len: if include_total_len {
                    Some(total_size)
                } else {
                    None
                },
            });

        for (dynamic_key, properties) in raw_props_vec {
            let value = self.apply_struct_select(
                def,
                VertexKey {
                    type_def_id: struct_select.def_id,
                    dynamic_key,
                },
                properties,
                struct_select.def_id,
                &struct_select.properties,
                ctx,
            )?;

            entity_sequence.push(value);
        }

        Ok(entity_sequence)
    }

    fn apply_struct_select(
        &self,
        def: &Def,
        vertex_key: VertexKey<&DynamicKey>,
        mut properties: FnvHashMap<PropId, Attr>,
        struct_def_id: DefId,
        select_properties: &FnvHashMap<PropId, Select>,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        let _entered = debug_span!("struct_sel", id = ?struct_def_id).entered();

        for (prop_id, subselect) in select_properties {
            if properties.contains_key(prop_id) {
                continue;
            }

            let data_relationship = find_data_relationship(def, prop_id)?;

            let DataRelationshipKind::Edge(projection) = data_relationship.kind else {
                continue;
            };

            match data_relationship.cardinality.1 {
                ValueCardinality::Unit => {
                    let matrix =
                        self.sub_query_edge(projection, subselect, vertex_key, false, ctx)?;
                    if let Some(row) = matrix.into_rows().next() {
                        properties.insert(*prop_id, row.into());
                    }
                }
                ValueCardinality::IndexSet | ValueCardinality::List => {
                    let matrix =
                        self.sub_query_edge(projection, subselect, vertex_key, true, ctx)?;
                    properties.insert(*prop_id, Attr::Matrix(matrix));
                }
            }
        }

        Ok(Value::Struct(Box::new(properties), struct_def_id.into()))
    }

    fn sub_query_edge(
        &self,
        projection: EdgeCardinalProjection,
        select: &Select,
        parent_key: VertexKey<&DynamicKey>,
        include_null: bool,
        ctx: &DbContext,
    ) -> DomainResult<AttrMatrix> {
        let edge_store = self.edges.get(&projection.id).expect("No edge store");

        let mut out = AttrMatrix::default();

        let mut edge_set = BTreeSet::default();

        edge_store.collect_column_eq(
            projection.subject,
            &EdgeData::Key(parent_key),
            &mut edge_set,
        );

        let value_vector = edge_store
            .columns
            .iter()
            .find_map(|column| match &column.data {
                EdgeVectorData::Values(values) => Some(values),
                _ => None,
            });

        out.columns.push(Default::default());

        if value_vector.is_some() {
            out.columns.push(Default::default());
        }

        if let EdgeVectorData::Keys(object_keys) =
            &edge_store.columns[projection.object.0 as usize].data
        {
            for edge_idx in edge_set {
                let key = &object_keys[edge_idx];

                let entity = self.sub_query_entity(
                    VertexKey {
                        type_def_id: key.type_def_id,
                        dynamic_key: &key.dynamic_key,
                    },
                    select,
                    ctx,
                )?;

                if let Some(entity) = entity {
                    out.columns[0].push(entity);

                    if let Some(value_vector) = value_vector {
                        out.columns[1].push(value_vector[edge_idx].clone());
                    }
                } else if include_null {
                    out.columns[0].push(Value::Void(DefId::unit().into()));

                    if value_vector.is_some() {
                        out.columns[1].push(Value::Void(DefId::unit().into()));
                    }
                }
            }
        }

        Ok(out)
    }

    fn sub_query_entity(
        &self,
        vertex_key: VertexKey<&DynamicKey>,
        select: &Select,
        ctx: &DbContext,
    ) -> DomainResult<Option<Value>> {
        if let Select::Struct(struct_select) = select {
            // sanity check
            if !self.vertices.contains_key(&struct_select.def_id) {
                error!(
                    "Store does not contain a collection for {:?}",
                    struct_select.def_id
                );
                return Err(DomainErrorKind::InvalidEntityDefId.into_error());
            }
        }

        let def = ctx.ontology.def(vertex_key.type_def_id);
        let entity = def
            .entity()
            .ok_or(DomainErrorKind::NotAnEntity(vertex_key.type_def_id).into_error())?;

        match select {
            Select::Unit => Ok(Some(Value::unit())),
            Select::Leaf => {
                if let Some(properties) =
                    self.look_up_vertex(vertex_key.type_def_id, vertex_key.dynamic_key)
                {
                    // Entity leaf only includes the ID of that entity, not its other fields
                    let id_attr = properties.get(&entity.id_prop).unwrap();
                    Ok(Some(id_attr.as_unit().unwrap().clone()))
                } else if let Some(id_value) = ctx
                    .check
                    .get_foreign_deferred(vertex_key.type_def_id, vertex_key.dynamic_key)
                {
                    // it's a "tentative vertex", but we have its id and can return a value for it for now.
                    // The check is deferred for later.
                    Ok(Some(id_value.clone()))
                } else {
                    Err(DomainErrorKind::InherentIdNotFound.into_error())
                }
            }
            Select::Struct(struct_select) => {
                let properties = self
                    .look_up_vertex(vertex_key.type_def_id, vertex_key.dynamic_key)
                    .ok_or(DomainErrorKind::InherentIdNotFound.into_error())?;

                Some(self.apply_struct_select(
                    def,
                    vertex_key,
                    properties.clone(),
                    vertex_key.type_def_id,
                    &struct_select.properties,
                    ctx,
                ))
                .transpose()
            }
            Select::StructUnion(_, variant_selects) => {
                for variant_select in variant_selects {
                    if variant_select.def_id == def.id {
                        let properties = self
                            .look_up_vertex(vertex_key.type_def_id, vertex_key.dynamic_key)
                            .ok_or(DomainErrorKind::InherentIdNotFound.into_error())?;

                        return Some(self.apply_struct_select(
                            def,
                            vertex_key,
                            properties.clone(),
                            vertex_key.type_def_id,
                            &variant_select.properties,
                            ctx,
                        ))
                        .transpose();
                    }
                }

                Ok(None)
            }
            Select::EntityId => Err(DomainError::data_store("entity id")),
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => {
                    let properties = self
                        .look_up_vertex(vertex_key.type_def_id, vertex_key.dynamic_key)
                        .ok_or(DomainErrorKind::InherentIdNotFound.into_error())?;

                    let value = self.apply_struct_select(
                        def,
                        vertex_key,
                        properties.clone(),
                        struct_select.def_id,
                        &struct_select.properties,
                        ctx,
                    )?;
                    Ok(Some(value))
                }
                StructOrUnionSelect::Union(_, candidates) => {
                    for variant_select in candidates {
                        if variant_select.def_id == def.id {
                            let properties = self
                                .look_up_vertex(vertex_key.type_def_id, vertex_key.dynamic_key)
                                .ok_or(DomainErrorKind::InherentIdNotFound.into_error())?;

                            return Some(self.apply_struct_select(
                                def,
                                vertex_key,
                                properties.clone(),
                                vertex_key.type_def_id,
                                &variant_select.properties,
                                ctx,
                            ))
                            .transpose();
                        }
                    }

                    Ok(None)
                }
            },
        }
    }
}
