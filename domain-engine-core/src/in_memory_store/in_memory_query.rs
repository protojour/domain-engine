use std::collections::BTreeMap;

use itertools::Itertools;
use ontol_runtime::{
    condition::{CondTerm, Condition},
    ontology::{DataRelationshipKind, PropertyCardinality, TypeInfo, ValueCardinality},
    select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    sequence::{Cursor, Sequence, SubSequence},
    value::{Attribute, Data, PropertyId, Value},
    Role,
};
use tracing::{debug, error};

use crate::{
    entity_id_utils::find_inherent_entity_id, filter::plan::compute_filter_plan,
    in_memory_store::in_memory_filter::FilterVal, DomainEngine, DomainError, DomainResult,
};

use super::in_memory_core::{DynamicKey, EntityKey, InMemoryStore};

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
                select.limit,
                select.after_cursor.as_ref(),
                select.include_total_len,
                engine,
            ),
            StructOrUnionSelect::Union(..) => todo!(),
        }
    }

    fn query_single_entity_collection(
        &self,
        struct_select: &StructSelect,
        condition: &Condition<CondTerm>,
        limit: usize,
        after_cursor: Option<&Cursor>,
        include_total_len: bool,
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

        let mut raw_props_vec = collection
            .iter()
            .filter(|(key, props)| {
                self.eval_filter_plan(
                    &FilterVal::Struct {
                        type_def_id: type_info.def_id,
                        dynamic_key: Some(key),
                        prop_tree: props,
                    },
                    &filter_plan,
                )
            })
            .map(|(key, props)| (key.clone(), props.clone()))
            .collect_vec();

        let total_size = raw_props_vec.len();

        let start_offset = match after_cursor {
            None => 0,
            Some(Cursor::Offset(after_offset)) => *after_offset + 1,
            Some(Cursor::Custom(_)) => {
                return Err(DomainError::NotImplemented);
            }
        };
        let mut end_cursor = None;
        let has_next = start_offset + limit < total_size;

        if start_offset + limit < total_size && limit > 0 {
            end_cursor = Some(Cursor::Offset(start_offset + limit - 1))
        }

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
            end_cursor,
            has_next,
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

            if let Some(data_relationship) = type_info.data_relationships.get(property_id) {
                if !matches!(
                    data_relationship.kind,
                    DataRelationshipKind::EntityGraph { .. }
                ) {
                    continue;
                }

                let attrs =
                    self.sub_query_attributes(*property_id, subselect, entity_key, engine)?;

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
                                data_relationship.target,
                            )
                            .into(),
                        );
                    }
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

        match property_id.role {
            Role::Subject => edge_collection
                .edges
                .iter()
                .filter(|edge| &edge.from.dynamic_key == parent_key)
                .map(|edge| -> DomainResult<Attribute> {
                    let entity = self.sub_query_entity(&edge.to, select, engine)?;
                    Ok(Attribute {
                        value: entity,
                        rel_params: edge.params.clone(),
                    })
                })
                .collect(),
            Role::Object => edge_collection
                .edges
                .iter()
                .filter(|edge| &edge.to.dynamic_key == parent_key)
                .map(|edge| -> DomainResult<Attribute> {
                    let entity = self.sub_query_entity(&edge.from, select, engine)?;
                    Ok(Attribute {
                        value: entity,
                        rel_params: edge.params.clone(),
                    })
                })
                .collect(),
        }
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
            Select::Struct(struct_select) => {
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
            Select::StructUnion(_, _) => todo!(),
            Select::EntityId => todo!(),
            Select::Entity(entity_select) => {
                let entity_key = entity_key.dynamic_key.clone();
                let entity_seq = self.query_entities(entity_select, engine)?;
                for entity_attr in entity_seq.attrs {
                    let id = find_inherent_entity_id(engine.ontology(), &entity_attr.value)?;
                    if let Some(id) = id {
                        let dynamic_key = Self::extract_dynamic_key(&id.data)?;

                        if dynamic_key == entity_key {
                            return Ok(entity_attr.value);
                        }
                    }
                }

                panic!("Not found")
            }
        }
    }
}
