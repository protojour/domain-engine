use std::collections::{BTreeMap, BTreeSet};

use anyhow::anyhow;
use fnv::FnvHashMap;
use ontol_runtime::{
    interface::serde::{operator::SerdeOperatorAddr, processor::ProcessorMode},
    ontology::{
        domain::{
            DataRelationshipInfo, DataRelationshipKind, DataRelationshipTarget,
            EdgeCardinalProjection, EntityInfo,
        },
        ontol::ValueGenerator,
    },
    property::ValueCardinality,
    query::{filter::Filter, select::Select},
    value::{Attribute, Serial, Value, ValueDebug},
    DefId, RelationshipId,
};
use tracing::{debug, warn};

use domain_engine_core::{
    entity_id_utils::{find_inherent_entity_id, try_generate_entity_id, GeneratedId},
    DomainError, DomainResult,
};

use crate::{
    core::{find_data_relationship, DbContext, EdgeData, EdgeVectorData, EntityKey},
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

        let mut raw_props_update: BTreeMap<RelationshipId, Attribute> = Default::default();

        let Value::StructUpdate(data_struct, _) = value else {
            return Err(DomainError::BadInput(anyhow!("Expected a struct update")));
        };
        for (rel_id, attribute) in *data_struct {
            let data_relationship = find_data_relationship(type_info, &rel_id)?;

            match data_relationship.kind {
                DataRelationshipKind::Id => {
                    warn!("ID should not be updated");
                }
                DataRelationshipKind::Tree => {
                    raw_props_update.insert(rel_id, attribute);
                }
                DataRelationshipKind::Edge(projection) => match data_relationship.cardinality.1 {
                    ValueCardinality::Unit => {
                        self.insert_entity_relationship(
                            type_info.def_id,
                            &dynamic_key,
                            (projection, Some(EdgeWriteMode::Overwrite)),
                            attribute,
                            data_relationship,
                            ctx,
                        )?;
                    }
                    ValueCardinality::IndexSet | ValueCardinality::List => match attribute.val {
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
                                        projection,
                                        attribute.val,
                                        data_relationship,
                                        ctx,
                                    )?;
                                } else {
                                    self.insert_entity_relationship(
                                        type_info.def_id,
                                        &dynamic_key,
                                        (projection, None),
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
                    },
                },
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

                for attr in entity_seq.into_attrs() {
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
            struct_map.insert(entity_info.id_relationship_id, id.clone().into());
        }

        let mut raw_props: FnvHashMap<RelationshipId, Attribute> = Default::default();

        let entity_key = Self::extract_dynamic_key(&id)?;

        for (rel_id, attribute) in *struct_map {
            let data_relationship = find_data_relationship(type_info, &rel_id)?;

            match data_relationship.kind {
                DataRelationshipKind::Id => {}
                DataRelationshipKind::Tree => {
                    raw_props.insert(rel_id, attribute);
                }
                DataRelationshipKind::Edge(projection) => match data_relationship.cardinality.1 {
                    ValueCardinality::Unit => {
                        self.insert_entity_relationship(
                            type_info.def_id,
                            &entity_key,
                            (projection, Some(EdgeWriteMode::Insert)),
                            attribute,
                            data_relationship,
                            ctx,
                        )?;
                    }
                    ValueCardinality::IndexSet | ValueCardinality::List => {
                        let Value::Sequence(seq, _) = attribute.val else {
                            return Err(DomainError::DataStoreBadRequest(anyhow!(
                                "Expected sequence for ValueCardinality::Many"
                            )));
                        };

                        for attribute in seq.into_attrs() {
                            self.insert_entity_relationship(
                                type_info.def_id,
                                &entity_key,
                                (projection, Some(EdgeWriteMode::Insert)),
                                attribute,
                                data_relationship,
                                ctx,
                            )?;
                        }
                    }
                },
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
        (projection, write_mode): (EdgeCardinalProjection, Option<EdgeWriteMode>),
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

        let edge_store = self
            .edges
            .get_mut(&projection.id)
            .expect("No edge collection");

        let local_key = EntityKey {
            type_def_id: entity_def_id,
            dynamic_key: entity_key.clone(),
        };

        let mut edge_tuple: Vec<EdgeData<EntityKey>> = edge_store
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| match &column.data {
                EdgeVectorData::Keys(_) => EdgeData::Key(if idx == projection.subject.0 as usize {
                    local_key.clone()
                } else if idx == projection.object.0 as usize {
                    foreign_key.clone()
                } else {
                    panic!()
                }),
                EdgeVectorData::Values(_) => EdgeData::Value(rel.clone()),
            })
            .collect();

        match write_mode {
            EdgeWriteMode::Insert => {
                let mut to_delete = BTreeSet::default();
                edge_store.collect_unique_violations(&edge_tuple, &mut to_delete);
                edge_store.delete_edges(to_delete);

                edge_store.push_tuple(edge_tuple);
            }
            EdgeWriteMode::Overwrite => {
                let mut to_delete = BTreeSet::default();
                edge_store.collect_unique_violations(&edge_tuple, &mut to_delete);
                edge_store.collect_column_eq(
                    projection.subject,
                    &edge_tuple[projection.subject.0 as usize],
                    &mut to_delete,
                );
                edge_store.delete_edges(to_delete);

                edge_store.push_tuple(edge_tuple);
            }
            EdgeWriteMode::UpdateExisting => {
                let mut subject_matching = BTreeSet::default();
                let mut object_matching = BTreeSet::default();

                edge_store.collect_column_eq(
                    projection.subject,
                    &edge_tuple[projection.subject.0 as usize],
                    &mut subject_matching,
                );
                edge_store.collect_column_eq(
                    projection.object,
                    &edge_tuple[projection.object.0 as usize],
                    &mut object_matching,
                );

                for column in edge_store.columns.iter_mut().rev() {
                    let data = edge_tuple.pop().unwrap();

                    if let (EdgeVectorData::Values(values), EdgeData::Value(value)) =
                        (&mut column.data, data)
                    {
                        let mut written = 0;

                        for edge_index in subject_matching.intersection(&object_matching) {
                            values[*edge_index] = value.clone();
                            written += 1;
                        }

                        if written == 0 {
                            return Err(DomainError::EntityNotFound);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn delete_entity_relationship(
        &mut self,
        entity_def_id: DefId,
        entity_key: &DynamicKey,
        projection: EdgeCardinalProjection,
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

        let edge_store = self
            .edges
            .get_mut(&projection.id)
            .expect("No edge collection");

        let local_key = EntityKey {
            type_def_id: entity_def_id,
            dynamic_key: entity_key.clone(),
        };

        let (subject_data, object_data) = match projection.proj() {
            (0, 1) => (EdgeData::Key(local_key), EdgeData::Key(foreign_key)),
            (1, 0) => (EdgeData::Key(foreign_key), EdgeData::Key(local_key)),
            _ => todo!(),
        };

        let mut subjects_matching = BTreeSet::new();
        let mut objects_matching = BTreeSet::new();

        edge_store.collect_column_eq(projection.subject, &subject_data, &mut subjects_matching);
        edge_store.collect_column_eq(projection.object, &object_data, &mut objects_matching);

        let did_delete = edge_store.delete_edges(
            subjects_matching
                .intersection(&objects_matching)
                .copied()
                .collect(),
        );

        if did_delete {
            Ok(())
        } else {
            Err(DomainError::EntityNotFound)
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
                entity_info.id_relationship_id,
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
                "N/A".to_string()
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
        let (generated_id, container) = try_generate_entity_id(
            id_operator_addr,
            value_generator,
            &ctx.ontology,
            ctx.system.as_ref(),
        )?;

        Ok(container.wrap(match generated_id {
            GeneratedId::Generated(value) => value,
            GeneratedId::AutoIncrementSerial(def_id) => {
                let serial_value = self.serial_counter;
                self.serial_counter += 1;
                Value::Serial(Serial(serial_value), def_id)
            }
        }))
    }
}
