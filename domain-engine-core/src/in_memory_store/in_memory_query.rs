use std::collections::BTreeMap;

use ontol_runtime::{
    condition::{CondTerm, Condition},
    ontology::{DataRelationshipKind, PropertyCardinality, TypeInfo, ValueCardinality},
    select::{EntitySelect, Select, StructOrUnionSelect, StructSelect},
    value::{Attribute, Data, PropertyId, Value},
    DefId, Role,
};
use tracing::{debug, error};

use crate::{
    entity_id_utils::find_inherent_entity_id,
    filter::plan::{compute_filter_plan, PlanEntry, Scalar},
    DomainEngine, DomainError, DomainResult,
};

use super::in_memory_core::{DynamicKey, InMemoryStore};

impl InMemoryStore {
    pub fn query_entities(
        &self,
        select: &EntitySelect,
        engine: &DomainEngine,
    ) -> DomainResult<Vec<Value>> {
        match &select.source {
            StructOrUnionSelect::Struct(struct_select) => {
                self.query_single_entity_collection(struct_select, &select.condition, engine)
            }
            StructOrUnionSelect::Union(..) => todo!(),
        }
    }

    fn query_single_entity_collection(
        &self,
        struct_select: &StructSelect,
        condition: &Condition<CondTerm>,
        engine: &DomainEngine,
    ) -> DomainResult<Vec<Value>> {
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

        let raw_props_vec: Vec<_> = collection
            .iter()
            .filter(|(_key, props)| {
                self.eval_filter_plan(
                    &DbVal::Struct {
                        type_def_id: type_info.def_id,
                        map: props,
                    },
                    &filter_plan,
                )
            })
            .map(|(key, props)| (key.clone(), props.clone()))
            .collect();

        raw_props_vec
            .into_iter()
            .map(|(entity_key, properties)| {
                self.apply_struct_select(type_info, &entity_key, properties, struct_select, engine)
            })
            .collect()
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

                let attributes =
                    self.sub_query_attributes(*property_id, subselect, entity_key, engine)?;

                match data_relationship.cardinality.1 {
                    ValueCardinality::One => {
                        if let Some(attribute) = attributes.into_iter().next() {
                            properties.insert(*property_id, attribute);
                        }
                    }
                    ValueCardinality::Many => {
                        properties.insert(
                            *property_id,
                            Value::new(Data::Sequence(attributes), data_relationship.target).into(),
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
                .filter(|edge| &edge.from == parent_key)
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
                .filter(|edge| &edge.to == parent_key)
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
        entity_key: &DynamicKey,
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

        // Find the def_id of the dynamic key. This is a little inefficient.
        let (def_id, properties) = self
            .collections
            .iter()
            .find_map(|(def_id, collection)| {
                collection
                    .get(entity_key)
                    .map(|properties| (*def_id, properties.clone()))
            })
            .ok_or(DomainError::InherentIdNotFound)?;

        let type_info = engine.ontology().get_type_info(def_id);
        let entity_info = type_info
            .entity_info
            .as_ref()
            .ok_or(DomainError::NotAnEntity(def_id))?;

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
                    entity_key,
                    properties,
                    &StructSelect {
                        def_id,
                        properties: select_properties,
                    },
                    engine,
                )
            }
            Select::StructUnion(_, _) => todo!(),
            Select::EntityId => todo!(),
            Select::Entity(entity_select) => {
                let entity_key = entity_key.clone();
                let entities = self.query_entities(entity_select, engine)?;
                for entity in entities {
                    let id = find_inherent_entity_id(engine.ontology(), &entity)?;
                    if let Some(id) = id {
                        let dynamic_key = Self::extract_dynamic_key(&id.data)?;

                        if dynamic_key == entity_key {
                            return Ok(entity);
                        }
                    }
                }

                panic!("Not found")
            }
        }
    }

    fn eval_filter_plan(&self, val: &DbVal, plan: &[PlanEntry]) -> bool {
        if plan.is_empty() {
            return true;
        }

        for entry in plan {
            if self.eval_filter_plan_entry(val, entry) {
                return true;
            }
        }

        false
    }

    fn eval_filter_plan_entry(&self, val: &DbVal, entry: &PlanEntry) -> bool {
        match (entry, val) {
            (PlanEntry::EntitiesOf(def_id, entries), DbVal::Struct { type_def_id, .. }) => {
                if def_id != type_def_id {
                    return false;
                }

                if entries.is_empty() {
                    return true;
                }

                for entry in entries {
                    if !self.eval_filter_plan_entry(val, entry) {
                        return false;
                    }
                }

                true
            }
            (PlanEntry::JoinRoot(..), _) => todo!(),
            (PlanEntry::Attr(prop_id, entries), DbVal::Struct { map, .. }) => {
                let Some(attr) = map.get(prop_id) else {
                    return false;
                };

                self.eval_attr_entries(attr, entries)
            }
            (PlanEntry::AllAttrs(prop_id, entries), DbVal::Struct { map, .. }) => {
                let Some(attr) = map.get(prop_id) else {
                    return false;
                };
                let Data::Sequence(seq) = &attr.value.data else {
                    return false;
                };

                for attr in seq {
                    if !self.eval_attr_entries(attr, entries) {
                        return false;
                    }
                }

                true
            }
            (PlanEntry::Edge(..), _) => todo!(),
            (PlanEntry::AllEdges(..), _) => todo!(),
            (PlanEntry::Eq(pred_scalar), DbVal::Scalar(val_scalar)) => {
                match (&val_scalar.data, pred_scalar) {
                    (Data::Text(data), Scalar::Text(pred)) => data.as_str() == pred.as_ref(),
                    _ => false,
                }
            }
            (PlanEntry::In(..), _) => todo!(),
            (PlanEntry::Join(_), _) => todo!(),
            _ => false,
        }
    }

    fn eval_attr_entries(&self, attr: &Attribute, entries: &[PlanEntry]) -> bool {
        if entries.is_empty() {
            return true;
        }

        for entry in entries {
            if !self.eval_filter_plan_entry(&DbVal::from_value(&attr.value), entry) {
                return false;
            }
        }

        true
    }
}

enum DbVal<'d> {
    Struct {
        type_def_id: DefId,
        map: &'d BTreeMap<PropertyId, Attribute>,
    },
    Sequence(&'d [Attribute]),
    Scalar(&'d Value),
}

impl<'d> DbVal<'d> {
    fn from_value(value: &'d Value) -> Self {
        match &value.data {
            Data::Struct(map) => Self::Struct {
                type_def_id: value.type_def_id,
                map,
            },
            Data::Sequence(seq) => Self::Sequence(seq),
            _ => Self::Scalar(value),
        }
    }
}
