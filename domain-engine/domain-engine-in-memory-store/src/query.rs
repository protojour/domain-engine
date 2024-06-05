use std::collections::BTreeSet;

use anyhow::anyhow;
use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::domain::{
        DataRelationshipKind, DataRelationshipTarget, EdgeCardinalProjection, TypeInfo,
    },
    property::ValueCardinality,
    query::{
        filter::Filter,
        select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    },
    sequence::{Sequence, SubSequence},
    value::{Attribute, Value},
    DefId, RelationshipId,
};
use tracing::{debug, error};

use domain_engine_core::{DomainError, DomainResult};

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
    pub fn query_entities(&self, select: &EntitySelect, ctx: &DbContext) -> DomainResult<Sequence> {
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
                    .map_err(|_| DomainError::DataStore(anyhow!("Invalid cursor format")))?,
                IncludeTotalLen(select.include_total_len),
                ctx,
            ),
            StructOrUnionSelect::Union(..) => Err(DomainError::DataStoreBadRequest(anyhow!(
                "Query entity union at root level not supported"
            ))),
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
    ) -> DomainResult<Sequence> {
        debug!("query single vertex collection: {struct_select:?}");
        let collection = self
            .vertices
            .get(&struct_select.def_id)
            .ok_or(DomainError::InvalidEntityDefId)?;

        let type_info = ctx.ontology.get_type_info(struct_select.def_id);
        let _entity_info = type_info
            .entity_info()
            .ok_or(DomainError::NotAnEntity(struct_select.def_id))?;

        // let filter_plan = compute_filter_plan(condition, &ctx.ontology).unwrap();
        debug!("eval filter: {filter}");

        let mut raw_props_vec = {
            let mut vec = vec![];

            for (key, props) in collection {
                let filter_val = FilterVal::Struct {
                    type_def_id: type_info.def_id,
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
                type_info,
                VertexKey {
                    type_def_id: struct_select.def_id,
                    dynamic_key,
                },
                properties,
                struct_select.def_id,
                &struct_select.properties,
                ctx,
            )?;

            entity_sequence.push(value.into());
        }

        Ok(entity_sequence)
    }

    fn apply_struct_select(
        &self,
        type_info: &TypeInfo,
        vertex_key: VertexKey<&DynamicKey>,
        mut properties: FnvHashMap<RelationshipId, Attribute>,
        struct_def_id: DefId,
        select_properties: &FnvHashMap<RelationshipId, Select>,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        for (rel_id, subselect) in select_properties {
            if properties.contains_key(rel_id) {
                continue;
            }

            let data_relationship = find_data_relationship(type_info, rel_id)?;

            let DataRelationshipKind::Edge(projection) = data_relationship.kind else {
                continue;
            };

            let attrs = self.sub_query_attributes(projection, subselect, vertex_key, ctx)?;

            match data_relationship.cardinality.1 {
                ValueCardinality::Unit => {
                    if let Some(attribute) = attrs.into_iter().next() {
                        properties.insert(*rel_id, attribute);
                    }
                }
                ValueCardinality::IndexSet | ValueCardinality::List => {
                    properties.insert(
                        *rel_id,
                        Value::Sequence(
                            Sequence::from_iter(attrs),
                            match data_relationship.target {
                                DataRelationshipTarget::Unambiguous(def_id) => def_id,
                                DataRelationshipTarget::Union(union_def_id) => union_def_id,
                            },
                        )
                        .into(),
                    );
                }
            }
        }

        Ok(Value::Struct(Box::new(properties), struct_def_id))
    }

    fn sub_query_attributes(
        &self,
        projection: EdgeCardinalProjection,
        select: &Select,
        parent_key: VertexKey<&DynamicKey>,
        ctx: &DbContext,
    ) -> DomainResult<Vec<Attribute>> {
        let edge_store = self.edges.get(&projection.id).expect("No edge collection");

        let mut out = vec![];

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

                out.push(Attribute {
                    rel: value_vector
                        .map(|vector| vector[edge_idx].clone())
                        .unwrap_or(Value::unit()),
                    val: entity,
                });
            }
        }

        Ok(out)
    }

    fn sub_query_entity(
        &self,
        vertex_key: VertexKey<&DynamicKey>,
        select: &Select,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        if let Select::Struct(struct_select) = select {
            // sanity check
            if !self.vertices.contains_key(&struct_select.def_id) {
                error!(
                    "Store does not contain a collection for {:?}",
                    struct_select.def_id
                );
                return Err(DomainError::InvalidEntityDefId);
            }
        }

        let properties = self
            .vertices
            .get(&vertex_key.type_def_id)
            .ok_or(DomainError::InherentIdNotFound)?
            .get(vertex_key.dynamic_key)
            .ok_or(DomainError::InherentIdNotFound)?;

        let type_info = ctx.ontology.get_type_info(vertex_key.type_def_id);
        let entity_info = type_info
            .entity_info()
            .ok_or(DomainError::NotAnEntity(vertex_key.type_def_id))?;

        match select {
            Select::Leaf => {
                // Entity leaf only includes the ID of that entity, not its other fields
                let id_attribute = properties.get(&entity_info.id_relationship_id).unwrap();
                Ok(id_attribute.val.clone())
            }
            Select::Struct(struct_select) => self.apply_struct_select(
                type_info,
                vertex_key,
                properties.clone(),
                vertex_key.type_def_id,
                &struct_select.properties,
                ctx,
            ),
            Select::StructUnion(_, variant_selects) => {
                for variant_select in variant_selects {
                    if variant_select.def_id == type_info.def_id {
                        return self.apply_struct_select(
                            type_info,
                            vertex_key,
                            properties.clone(),
                            vertex_key.type_def_id,
                            &variant_select.properties,
                            ctx,
                        );
                    }
                }

                Ok(Value::Struct(
                    Box::new(properties.clone()),
                    type_info.def_id,
                ))
            }
            Select::EntityId => Err(DomainError::DataStore(anyhow!("entity id"))),
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => self.apply_struct_select(
                    type_info,
                    vertex_key,
                    properties.clone(),
                    struct_select.def_id,
                    &struct_select.properties,
                    ctx,
                ),
                StructOrUnionSelect::Union(_, candidates) => {
                    let struct_select = candidates
                        .iter()
                        .find(|struct_select| struct_select.def_id == vertex_key.type_def_id)
                        .expect("Union variant not found");

                    self.apply_struct_select(
                        type_info,
                        vertex_key,
                        properties.clone(),
                        struct_select.def_id,
                        &struct_select.properties,
                        ctx,
                    )
                }
            },
        }
    }
}
