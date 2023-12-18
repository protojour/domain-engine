use std::collections::BTreeMap;

use anyhow::anyhow;
use ontol_runtime::{
    condition::{CondTerm, Condition},
    ontology::{
        DataRelationshipKind, DataRelationshipTarget, PropertyCardinality, TypeInfo,
        ValueCardinality,
    },
    select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    sequence::{Sequence, SubSequence},
    value::{Attribute, Data, PropertyId, Value},
    Role,
};
use tracing::{debug, error};

use domain_engine_core::{
    filter::plan::compute_filter_plan, DomainEngine, DomainError, DomainResult,
};

use crate::{core::find_data_relationship, filter::FilterVal};

use super::core::{DynamicKey, EntityKey, InMemoryStore};

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
        engine: &DomainEngine,
    ) -> DomainResult<Sequence> {
        match &select.source {
            StructOrUnionSelect::Struct(struct_select) => self.query_single_entity_collection(
                struct_select,
                &select.condition,
                Limit(select.limit),
                select
                    .after_cursor
                    .as_deref()
                    .map(bincode::deserialize)
                    .transpose()
                    .map_err(|_| DomainError::DataStore(anyhow!("Invalid cursor format")))?,
                IncludeTotalLen(select.include_total_len),
                engine,
            ),
            StructOrUnionSelect::Union(..) => Err(DomainError::DataStoreBadRequest(anyhow!(
                "Query entity union at root level not supported"
            ))),
        }
    }

    pub fn query_single_entity_collection(
        &self,
        struct_select: &StructSelect,
        condition: &Condition<CondTerm>,
        Limit(limit): Limit,
        after_cursor: Option<Cursor>,
        IncludeTotalLen(include_total_len): IncludeTotalLen,
        engine: &DomainEngine,
    ) -> DomainResult<Sequence> {
        debug!("query single entity collection: {struct_select:?}");
        let collection = self
            .collections
            .get(&struct_select.def_id)
            .ok_or(DomainError::InvalidEntityDefId)?;

        let type_info = engine.ontology().get_type_info(struct_select.def_id);
        let _entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(struct_select.def_id))?;

        let filter_plan = compute_filter_plan(condition, engine.ontology()).unwrap();
        debug!("eval filter plan: {filter_plan:#?}");

        let mut raw_props_vec = {
            let mut vec = vec![];

            for (key, props) in collection {
                let filter_val = FilterVal::Struct {
                    type_def_id: type_info.def_id,
                    dynamic_key: Some(key),
                    prop_tree: props,
                };
                if self.eval_filter_plan(&filter_val, &filter_plan)? {
                    vec.push((key.clone(), props.clone()));
                }
            }
            vec
        };

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

        let mut entity_sequence = Sequence::new_with_capacity(raw_props_vec.len());

        entity_sequence.sub_seq = Some(Box::new(SubSequence {
            end_cursor: if limit > 0 {
                Some(
                    bincode::serialize(&Cursor {
                        offset: start_offset + limit - 1,
                    })
                    .unwrap()
                    .into(),
                )
            } else {
                after_cursor.map(|after_cursor| bincode::serialize(&after_cursor).unwrap().into())
            },
            has_next: start_offset + limit < total_size,
            total_len: if include_total_len {
                Some(total_size)
            } else {
                None
            },
        }));

        for (entity_key, properties) in raw_props_vec {
            let value = self.apply_struct_select(
                type_info,
                &entity_key,
                properties,
                struct_select,
                engine,
            )?;

            entity_sequence.attrs.push(value.into());
        }

        Ok(entity_sequence)
    }

    fn apply_struct_select(
        &self,
        type_info: &TypeInfo,
        entity_key: &DynamicKey,
        mut properties: BTreeMap<PropertyId, Attribute>,
        struct_select: &StructSelect,
        engine: &DomainEngine,
    ) -> DomainResult<Value> {
        for (property_id, subselect) in &struct_select.properties {
            if properties.contains_key(property_id) {
                continue;
            }

            let data_relationship = find_data_relationship(type_info, property_id)?;

            if !matches!(
                data_relationship.kind,
                DataRelationshipKind::EntityGraph { .. }
            ) {
                continue;
            }

            let attrs = self.sub_query_attributes(*property_id, subselect, entity_key, engine)?;

            match data_relationship.cardinality.1 {
                ValueCardinality::One => {
                    if let Some(attribute) = attrs.into_iter().next() {
                        properties.insert(*property_id, attribute);
                    }
                }
                ValueCardinality::Many => {
                    properties.insert(
                        *property_id,
                        Value::new(
                            Data::Sequence(Sequence::new(attrs)),
                            match data_relationship.target {
                                DataRelationshipTarget::Unambiguous(def_id) => def_id,
                                DataRelationshipTarget::Union { union_def_id, .. } => union_def_id,
                            },
                        )
                        .into(),
                    );
                }
            }
        }

        Ok(Value::new(Data::Struct(properties), struct_select.def_id))
    }

    fn sub_query_attributes(
        &self,
        property_id: PropertyId,
        select: &Select,
        parent_key: &DynamicKey,
        engine: &DomainEngine,
    ) -> DomainResult<Vec<Attribute>> {
        let relationship_id = property_id.relationship_id;
        let edge_collection = self
            .edge_collections
            .get(&relationship_id)
            .expect("No edge collection");

        let mut out = vec![];

        for edge in &edge_collection.edges {
            let entity = match property_id.role {
                Role::Subject => {
                    if &edge.from.dynamic_key != parent_key {
                        continue;
                    }
                    self.sub_query_entity(&edge.to, select, engine)?
                }
                Role::Object => {
                    if &edge.to.dynamic_key != parent_key {
                        continue;
                    }
                    self.sub_query_entity(&edge.from, select, engine)?
                }
            };

            out.push(Attribute {
                value: entity,
                rel_params: edge.params.clone(),
            });
        }

        Ok(out)
    }

    fn sub_query_entity(
        &self,
        entity_key: &EntityKey,
        select: &Select,
        engine: &DomainEngine,
    ) -> DomainResult<Value> {
        if let Select::Struct(struct_select) = select {
            // sanity check
            if !self.collections.contains_key(&struct_select.def_id) {
                error!(
                    "Store does not contain a collection for {:?}",
                    struct_select.def_id
                );
                return Err(DomainError::InvalidEntityDefId);
            }
        }

        let properties = self
            .collections
            .get(&entity_key.type_def_id)
            .ok_or(DomainError::InherentIdNotFound)?
            .get(&entity_key.dynamic_key)
            .ok_or(DomainError::InherentIdNotFound)?;

        let type_info = engine.ontology().get_type_info(entity_key.type_def_id);
        let entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(entity_key.type_def_id))?;

        match select {
            Select::Leaf => {
                // Entity leaf only includes the ID of that entity, not its other fields
                let id_attribute = properties
                    .get(&PropertyId::subject(entity_info.id_relationship_id))
                    .unwrap();
                Ok(id_attribute.value.clone())
            }
            Select::Struct(struct_select) => self.sub_query_entity_struct(
                entity_key,
                struct_select,
                type_info,
                properties,
                engine,
            ),
            Select::StructUnion(_, variant_selects) => {
                for variant_select in variant_selects {
                    if variant_select.def_id == type_info.def_id {
                        return self.sub_query_entity_struct(
                            entity_key,
                            variant_select,
                            type_info,
                            properties,
                            engine,
                        );
                    }
                }

                Ok(Value {
                    data: Data::Struct(properties.clone()),
                    type_def_id: type_info.def_id,
                })
            }
            Select::EntityId => return Err(DomainError::DataStore(anyhow!("entity id"))),
            Select::Entity(entity_select) => match &entity_select.source {
                StructOrUnionSelect::Struct(struct_select) => self.apply_struct_select(
                    type_info,
                    &entity_key.dynamic_key,
                    properties.clone(),
                    struct_select,
                    engine,
                ),
                StructOrUnionSelect::Union(_, candidates) => {
                    let struct_select = candidates
                        .iter()
                        .find(|struct_select| struct_select.def_id == entity_key.type_def_id)
                        .expect("Union variant not found");

                    self.apply_struct_select(
                        type_info,
                        &entity_key.dynamic_key,
                        properties.clone(),
                        struct_select,
                        engine,
                    )
                }
            },
        }
    }

    fn sub_query_entity_struct(
        &self,
        entity_key: &EntityKey,
        struct_select: &StructSelect,
        type_info: &TypeInfo,
        properties: &BTreeMap<PropertyId, Attribute>,
        engine: &DomainEngine,
    ) -> DomainResult<Value> {
        let mut select_properties = struct_select.properties.clone();

        // Need to "infer" mandatory entity properties, because JSON serializer expects that
        for (property_id, data_relationship) in type_info.entity_relationships() {
            if matches!(
                data_relationship.cardinality.0,
                PropertyCardinality::Mandatory
            ) && !select_properties.contains_key(property_id)
            {
                select_properties.insert(*property_id, Select::Leaf);
            }
        }

        self.apply_struct_select(
            type_info,
            &entity_key.dynamic_key,
            properties.clone(),
            &StructSelect {
                def_id: entity_key.type_def_id,
                properties: select_properties,
            },
            engine,
        )
    }
}
