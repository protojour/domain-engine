use std::collections::BTreeMap;

use anyhow::anyhow;
use fnv::FnvHashMap;
use itertools::Itertools;
use ontol_runtime::{
    interface::serde::{operator::SerdeOperatorAddr, processor::ProcessorMode},
    ontology::{
        domain::{DataRelationshipInfo, DataRelationshipKind, DataRelationshipTarget, EntityInfo},
        ontol::ValueGenerator,
    },
    property::{PropertyId, Role, ValueCardinality},
    query::{filter::Filter, select::Select},
    smart_format,
    value::{Attribute, Serial, Value, ValueDebug},
    DefId,
};
use smartstring::alias::String;
use tracing::{debug, warn};

use domain_engine_core::{
    entity_id_utils::{find_inherent_entity_id, try_generate_entity_id, GeneratedId},
    DomainError, DomainResult,
};

use crate::{
    core::{find_data_relationship, DbContext, Edge, EdgeCollection, EntityKey},
    query::{Cursor, IncludeTotalLen, Limit},
};

use super::core::{DynamicKey, InMemoryStore};

enum EdgeWriteMode {
    Insert,
    Overwrite,
    UpdateExisting,
}

impl InMemoryStore {
    pub fn write_new_entity(
        &mut self,
        entity: Value,
        select: &Select,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        let entity_id = self.write_new_entity_inner(entity, ctx)?;
        self.post_write_select(entity_id, select, ctx)
    }

    pub fn update_entity(
        &mut self,
        value: Value,
        select: &Select,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        debug!("update entity: {:#?}", value);

        let entity_id = find_inherent_entity_id(&value, &ctx.ontology)?
            .ok_or_else(|| DomainError::EntityNotFound)?;
        let type_info = ctx.ontology.get_type_info(value.type_def_id());
        let dynamic_key = Self::extract_dynamic_key(&entity_id)?;

        if !self
            .collections
            .get(&type_info.def_id)
            .unwrap()
            .contains_key(&dynamic_key)
        {
            return Err(DomainError::EntityNotFound);
        }

        let mut raw_props_update: BTreeMap<PropertyId, Attribute> = Default::default();

        let Value::StructUpdate(data_struct, _) = value else {
            return Err(DomainError::BadInput(anyhow!("Expected a struct update")));
        };
        for (property_id, attribute) in *data_struct {
            let data_relationship = find_data_relationship(type_info, &property_id)?;

            match data_relationship.kind {
                DataRelationshipKind::Id => {
                    warn!("ID should not be updated");
                }
                DataRelationshipKind::Tree => {
                    raw_props_update.insert(property_id, attribute);
                }
                DataRelationshipKind::EntityGraph { .. } => {
                    match data_relationship.cardinality_by_role(property_id.role).1 {
                        ValueCardinality::Unit => {
                            self.insert_entity_relationship(
                                type_info.def_id,
                                &dynamic_key,
                                (property_id, Some(EdgeWriteMode::Overwrite)),
                                attribute,
                                data_relationship,
                                ctx,
                            )?;
                        }
                        ValueCardinality::OrderedSet | ValueCardinality::List => {
                            match attribute.val {
                                Value::Sequence(_sequence, _) => {
                                    return Err(DomainError::DataStore(anyhow!(
                                        "Multi-relation overwrite not yet implemented"
                                    )));
                                }
                                Value::Patch(patch_attributes, _) => {
                                    for attribute in patch_attributes {
                                        if matches!(attribute.rel, Value::DeleteRelationship(_)) {
                                            self.delete_entity_relationship(
                                                type_info.def_id,
                                                &dynamic_key,
                                                property_id,
                                                attribute.val,
                                                data_relationship,
                                                ctx,
                                            )?;
                                        } else {
                                            self.insert_entity_relationship(
                                                type_info.def_id,
                                                &dynamic_key,
                                                (property_id, None),
                                                attribute,
                                                data_relationship,
                                                ctx,
                                            )?;
                                        }
                                    }
                                }
                                _ => {
                                    return Err(DomainError::DataStoreBadRequest(anyhow!(
                                        "Invalid input for multi-relation write"
                                    )));
                                }
                            }
                        }
                    }
                }
            }
        }

        let collection = self.collections.get_mut(&type_info.def_id).unwrap();
        let raw_props = collection.get_mut(&dynamic_key).unwrap();

        for (property_id, attr) in raw_props_update {
            raw_props.insert(property_id, attr);
        }

        self.post_write_select(entity_id, select, ctx)
    }

    fn post_write_select(
        &mut self,
        entity_id: Value,
        select: &Select,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        match select {
            Select::EntityId => Ok(entity_id),
            Select::Struct(struct_select) => {
                let target_dynamic_key = Self::extract_dynamic_key(&entity_id)?;
                let entity_seq = self.query_single_entity_collection(
                    struct_select,
                    &Filter::default(),
                    Limit(usize::MAX),
                    Option::<Cursor>::None,
                    IncludeTotalLen(false),
                    ctx,
                )?;

                for attr in entity_seq.attrs {
                    let id = find_inherent_entity_id(&attr.val, &ctx.ontology)?;
                    if let Some(id) = id {
                        let dynamic_key = Self::extract_dynamic_key(&id)?;

                        if dynamic_key == target_dynamic_key {
                            return Ok(attr.val);
                        }
                    }
                }

                Err(DomainError::DataStore(anyhow!(
                    "Entity did not get written"
                )))
            }
            _ => Err(DomainError::DataStoreBadRequest(anyhow!(
                "post-write select kind not supported"
            ))),
        }
    }

    /// Returns the entity ID
    fn write_new_entity_inner(&mut self, entity: Value, ctx: &DbContext) -> DomainResult<Value> {
        debug!(
            "write new entity {:?}: {}",
            entity.type_def_id(),
            ValueDebug(&entity)
        );

        let type_info = ctx.ontology.get_type_info(entity.type_def_id());
        let entity_info = type_info
            .entity_info()
            .ok_or(DomainError::NotAnEntity(entity.type_def_id()))?;

        let (id, id_generated) = match find_inherent_entity_id(&entity, &ctx.ontology)? {
            Some(id) => (id, false),
            None => {
                let value_generator = entity_info.id_value_generator.ok_or_else(|| {
                    DomainError::DataStoreBadRequest(anyhow!("No id provided and no ID generator"))
                })?;

                (
                    self.generate_entity_id(entity_info.id_operator_addr, value_generator, ctx)?,
                    true,
                )
            }
        };

        debug!("write entity_id={}", ValueDebug(&id));

        let Value::Struct(mut struct_map, type_def_id) = entity else {
            return Err(DomainError::EntityMustBeStruct);
        };

        if id_generated {
            struct_map.insert(
                PropertyId::subject(entity_info.id_relationship_id),
                id.clone().into(),
            );
        }

        let mut raw_props: FnvHashMap<PropertyId, Attribute> = Default::default();

        let entity_key = Self::extract_dynamic_key(&id)?;

        for (property_id, attribute) in *struct_map {
            let data_relationship = find_data_relationship(type_info, &property_id)?;

            match data_relationship.kind {
                DataRelationshipKind::Id => {}
                DataRelationshipKind::Tree => {
                    raw_props.insert(property_id, attribute);
                }
                DataRelationshipKind::EntityGraph { .. } => {
                    match data_relationship.cardinality_by_role(property_id.role).1 {
                        ValueCardinality::Unit => {
                            self.insert_entity_relationship(
                                type_info.def_id,
                                &entity_key,
                                (property_id, Some(EdgeWriteMode::Insert)),
                                attribute,
                                data_relationship,
                                ctx,
                            )?;
                        }
                        ValueCardinality::OrderedSet | ValueCardinality::List => {
                            let Value::Sequence(seq, _) = attribute.val else {
                                return Err(DomainError::DataStoreBadRequest(anyhow!(
                                    "Expected sequence for ValueCardinality::Many"
                                )));
                            };

                            for attribute in seq.attrs {
                                self.insert_entity_relationship(
                                    type_info.def_id,
                                    &entity_key,
                                    (property_id, Some(EdgeWriteMode::Insert)),
                                    attribute,
                                    data_relationship,
                                    ctx,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        let collection = self.collections.get_mut(&type_def_id).unwrap();

        if collection.contains_key(&entity_key) {
            return Err(DomainError::EntityAlreadyExists);
        }

        collection.insert(entity_key, raw_props);

        Ok(id)
    }

    fn insert_entity_relationship(
        &mut self,
        entity_def_id: DefId,
        entity_key: &DynamicKey,
        (property_id, write_mode): (PropertyId, Option<EdgeWriteMode>),
        Attribute { rel, val }: Attribute,
        data_relationship: &DataRelationshipInfo,
        ctx: &DbContext,
    ) -> DomainResult<()> {
        debug!(
            "entity rel attribute: ({rel:?}, {val:?}). Data relationship: {data_relationship:?}",
            data_relationship = ctx.ontology.debug(data_relationship)
        );

        fn write_mode_from_value(value: &Value) -> EdgeWriteMode {
            match value {
                Value::Struct(..) => EdgeWriteMode::Insert,
                Value::StructUpdate(..) => EdgeWriteMode::UpdateExisting,
                _ => EdgeWriteMode::Overwrite,
            }
        }

        let (write_mode, foreign_key) = match &data_relationship.target {
            DataRelationshipTarget::Unambiguous(entity_def_id) => {
                if &val.type_def_id() == entity_def_id {
                    let write_mode = write_mode.unwrap_or(write_mode_from_value(&val));
                    let foreign_id = self.write_new_entity_inner(val, ctx)?;
                    let dynamic_key = Self::extract_dynamic_key(&foreign_id)?;

                    (
                        write_mode,
                        EntityKey {
                            type_def_id: *entity_def_id,
                            dynamic_key,
                        },
                    )
                } else {
                    (
                        write_mode.unwrap_or(write_mode_from_value(&rel)),
                        self.resolve_foreign_key_for_edge(
                            *entity_def_id,
                            ctx.ontology
                                .get_type_info(*entity_def_id)
                                .entity_info()
                                .unwrap(),
                            val,
                            ctx,
                        )?,
                    )
                }
            }
            DataRelationshipTarget::Union(union_def_id) => {
                let variants = ctx.ontology.union_variants(*union_def_id);
                if variants.contains(&val.type_def_id()) {
                    // Explicit data struct of a given variant

                    let write_mode = write_mode.unwrap_or(write_mode_from_value(&val));
                    let entity_def_id = val.type_def_id();
                    let foreign_id = self.write_new_entity_inner(val, ctx)?;
                    let dynamic_key = Self::extract_dynamic_key(&foreign_id)?;

                    let key = EntityKey {
                        type_def_id: entity_def_id,
                        dynamic_key,
                    };

                    (write_mode, key)
                } else {
                    let (variant_def_id, entity_info) = variants
                        .iter()
                        .find_map(|variant_def_id| {
                            let entity_info = ctx
                                .ontology
                                .get_type_info(*variant_def_id)
                                .entity_info()
                                .unwrap();

                            if entity_info.id_value_def_id == val.type_def_id() {
                                Some((*variant_def_id, entity_info))
                            } else {
                                None
                            }
                        })
                        .expect("Corresponding entity def id not found for the given ID");

                    (
                        write_mode.unwrap_or(write_mode_from_value(&rel)),
                        self.resolve_foreign_key_for_edge(variant_def_id, entity_info, val, ctx)?,
                    )
                }
            }
        };

        let edge_collection = self
            .edge_collections
            .get_mut(&property_id.relationship_id)
            .expect("No edge collection");

        let local_key = EntityKey {
            type_def_id: entity_def_id,
            dynamic_key: entity_key.clone(),
        };

        match (property_id.role, write_mode) {
            (Role::Subject, EdgeWriteMode::Insert) => {
                enforce_cardinality_pre_insert(edge_collection, &local_key, &foreign_key);
                edge_collection.edges.push(Edge {
                    from: local_key,
                    to: foreign_key,
                    params: rel,
                });
            }
            (Role::Subject, EdgeWriteMode::Overwrite) => {
                edge_collection.edges.retain(|edge| edge.from != local_key);
                enforce_cardinality_pre_insert(edge_collection, &local_key, &foreign_key);
                edge_collection.edges.push(Edge {
                    from: local_key,
                    to: foreign_key,
                    params: rel,
                });
            }
            (Role::Subject, EdgeWriteMode::UpdateExisting) => {
                let edge = edge_collection
                    .edges
                    .iter_mut()
                    .find(|edge| edge.from == local_key && edge.to == foreign_key)
                    .ok_or(DomainError::EntityNotFound)?;

                edge.params = rel;
            }
            (Role::Object, EdgeWriteMode::Insert) => {
                enforce_cardinality_pre_insert(edge_collection, &foreign_key, &local_key);
                edge_collection.edges.push(Edge {
                    from: foreign_key,
                    to: local_key,
                    params: rel,
                });
            }
            (Role::Object, EdgeWriteMode::Overwrite) => {
                edge_collection.edges.retain(|edge| edge.to != local_key);
                enforce_cardinality_pre_insert(edge_collection, &foreign_key, &local_key);
                edge_collection.edges.push(Edge {
                    from: foreign_key,
                    to: local_key,
                    params: rel,
                });
            }
            (Role::Object, EdgeWriteMode::UpdateExisting) => {
                let edge = edge_collection
                    .edges
                    .iter_mut()
                    .find(|edge| edge.from == foreign_key && edge.to == local_key)
                    .ok_or(DomainError::EntityNotFound)?;

                edge.params = rel;
            }
        }

        Ok(())
    }

    fn delete_entity_relationship(
        &mut self,
        entity_def_id: DefId,
        entity_key: &DynamicKey,
        property_id: PropertyId,
        foreign_id: Value,
        data_relationship: &DataRelationshipInfo,
        ctx: &DbContext,
    ) -> DomainResult<()> {
        let foreign_key = match &data_relationship.target {
            DataRelationshipTarget::Unambiguous(entity_def_id) => self
                .resolve_foreign_key_for_edge(
                    *entity_def_id,
                    ctx.ontology
                        .get_type_info(*entity_def_id)
                        .entity_info()
                        .unwrap(),
                    foreign_id,
                    ctx,
                )?,
            DataRelationshipTarget::Union(union_def_id) => {
                let variants = ctx.ontology.union_variants(*union_def_id);
                let (variant_def_id, entity_info) = variants
                    .iter()
                    .find_map(|variant_def_id| {
                        let entity_info = ctx
                            .ontology
                            .get_type_info(*variant_def_id)
                            .entity_info()
                            .unwrap();

                        if entity_info.id_value_def_id == foreign_id.type_def_id() {
                            Some((*variant_def_id, entity_info))
                        } else {
                            None
                        }
                    })
                    .expect("Corresponding entity def id not found for the given ID");

                self.resolve_foreign_key_for_edge(variant_def_id, entity_info, foreign_id, ctx)?
            }
        };

        let edge_collection = self
            .edge_collections
            .get_mut(&property_id.relationship_id)
            .expect("No edge collection");
        let edges = &mut edge_collection.edges;

        let local_key = EntityKey {
            type_def_id: entity_def_id,
            dynamic_key: entity_key.clone(),
        };

        let edge_index = match property_id.role {
            Role::Subject => edges
                .iter()
                .find_position(|edge| edge.from == local_key && edge.to == foreign_key),
            Role::Object => edges
                .iter()
                .find_position(|edge| edge.from == foreign_key && edge.to == local_key),
        };

        match edge_index {
            Some((index, _)) => {
                edges.remove(index);
                Ok(())
            }
            None => Err(DomainError::EntityNotFound),
        }
    }

    /// This is for creating a relationship to an existing entity, using only a "foreign key".
    fn resolve_foreign_key_for_edge(
        &mut self,
        foreign_entity_def_id: DefId,
        entity_info: &EntityInfo,
        id_value: Value,
        ctx: &DbContext,
    ) -> DomainResult<EntityKey> {
        let foreign_key = Self::extract_dynamic_key(&id_value)?;
        let entity_data = self.look_up_entity(foreign_entity_def_id, &foreign_key);

        if entity_data.is_none() && entity_info.is_self_identifying {
            // This type has UPSERT semantics.
            // Synthesize the entity, write it and move on..

            let entity_data = FnvHashMap::from_iter([(
                PropertyId::subject(entity_info.id_relationship_id),
                Attribute::from(id_value),
            )]);
            self.write_new_entity_inner(
                Value::Struct(Box::new(entity_data), foreign_entity_def_id),
                ctx,
            )?;
        } else if entity_data.is_none() {
            let type_info = ctx.ontology.get_type_info(id_value.type_def_id());
            let repr = if let Some(operator_addr) = type_info.operator_addr {
                // TODO: Easier way to report values in "human readable"/JSON format

                let processor = ctx
                    .ontology
                    .new_serde_processor(operator_addr, ProcessorMode::Read);

                let mut buf: Vec<u8> = vec![];
                processor
                    .serialize_value(&id_value, None, &mut serde_json::Serializer::new(&mut buf))
                    .unwrap();
                String::from(std::str::from_utf8(&buf).unwrap())
            } else {
                smart_format!("N/A")
            };

            return Err(DomainError::UnresolvedForeignKey(repr));
        }

        Ok(EntityKey {
            type_def_id: foreign_entity_def_id,
            dynamic_key: foreign_key,
        })
    }

    fn generate_entity_id(
        &mut self,
        id_operator_addr: SerdeOperatorAddr,
        value_generator: ValueGenerator,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        match try_generate_entity_id(
            id_operator_addr,
            value_generator,
            &ctx.ontology,
            ctx.system.as_ref(),
        )? {
            GeneratedId::Generated(value) => Ok(value),
            GeneratedId::AutoIncrementSerial(def_id) => {
                let serial_value = self.serial_counter;
                self.serial_counter += 1;
                Ok(Value::Serial(Serial(serial_value), def_id))
            }
        }
    }
}

/// Ensure none of the keys are in the edge collection given there's some
/// singleton cardinality that should be enforced.
fn enforce_cardinality_pre_insert(
    edge_collection: &mut EdgeCollection,
    from_key: &EntityKey,
    to_key: &EntityKey,
) {
    if matches!(
        edge_collection.subject_cardinality.1,
        ValueCardinality::Unit
    ) {
        edge_collection.edges.retain(|edge| &edge.from != from_key);
    }

    if matches!(edge_collection.object_cardinality.1, ValueCardinality::Unit) {
        edge_collection.edges.retain(|edge| &edge.to != to_key);
    }
}
