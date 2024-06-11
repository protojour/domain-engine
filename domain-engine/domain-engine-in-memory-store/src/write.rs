use std::collections::{BTreeMap, BTreeSet};

use anyhow::anyhow;
use fnv::FnvHashMap;
use ontol_runtime::{
    attr::{Attr, AttrRef},
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
    tuple::EndoTuple,
    value::{Serial, Value, ValueDebug},
    DefId, RelationshipId,
};
use smallvec::smallvec;
use tracing::{debug, warn};

use domain_engine_core::{
    entity_id_utils::{find_inherent_entity_id, try_generate_entity_id, GeneratedId},
    DomainError, DomainResult,
};

use crate::{
    core::{find_data_relationship, DbContext, EdgeData, EdgeVectorData, VertexKey},
    query::{Cursor, IncludeTotalLen, Limit},
};

use super::core::InMemoryStore;

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
        let entity_id = self.write_new_vertex_inner(entity, ctx)?;
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
        let vertex_key = VertexKey {
            type_def_id: type_info.def_id,
            dynamic_key: Self::extract_dynamic_key(&entity_id)?,
        };

        if !self
            .vertices
            .get(&type_info.def_id)
            .unwrap()
            .contains_key(&vertex_key.dynamic_key)
        {
            return Err(DomainError::EntityNotFound);
        }

        let mut raw_props_update: BTreeMap<RelationshipId, Attr> = Default::default();

        let Value::StructUpdate(data_struct, _) = value else {
            return Err(DomainError::BadInput(anyhow!("Expected a struct update")));
        };
        for (rel_id, attr) in *data_struct {
            let data_relationship = find_data_relationship(type_info, &rel_id)?;

            match (
                data_relationship.kind,
                attr,
                data_relationship.cardinality.1,
            ) {
                (DataRelationshipKind::Id, ..) => {
                    warn!("ID should not be updated");
                }
                (DataRelationshipKind::Tree, attr, _) => {
                    raw_props_update.insert(rel_id, attr);
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Unit(unit),
                    ValueCardinality::Unit,
                ) => {
                    self.insert_entity_relationship(
                        vertex_key.clone(),
                        (projection, Some(EdgeWriteMode::Overwrite)),
                        EndoTuple {
                            elements: smallvec![unit],
                        },
                        data_relationship,
                        ctx,
                    )?;
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Matrix(matrix),
                    ValueCardinality::IndexSet | ValueCardinality::List,
                ) => {
                    for tuple in matrix.into_rows() {
                        if tuple.elements.iter().any(|value| value.tag().is_delete()) {
                            let mut iter = tuple.elements.into_iter();
                            let foreign_key = if projection.object.0 == 0 {
                                iter.next().unwrap()
                            } else {
                                iter.next().unwrap();
                                iter.next().unwrap()
                            };

                            self.delete_entity_relationship(
                                vertex_key.clone(),
                                projection,
                                foreign_key,
                                data_relationship,
                                ctx,
                            )?;
                        } else {
                            self.insert_entity_relationship(
                                vertex_key.clone(),
                                (projection, Some(EdgeWriteMode::Insert)),
                                tuple,
                                data_relationship,
                                ctx,
                            )?;
                        }
                    }
                }
                (
                    DataRelationshipKind::Edge(_projection),
                    Attr::Unit(_unit),
                    ValueCardinality::IndexSet | ValueCardinality::List,
                ) => {
                    // if let Value::Patch(patch_attrs, _) = unit {
                    //     for attr in patch_attrs {
                    //         if matches!(attr.rel, Value::DeleteRelationship(_)) {
                    //             self.delete_entity_relationship(
                    //                 vertex_key.clone(),
                    //                 projection,
                    //                 attr.val,
                    //                 data_relationship,
                    //                 ctx,
                    //             )?;
                    //         } else {
                    //             self.insert_entity_relationship(
                    //                 vertex_key.clone(),
                    //                 (projection, None),
                    //                 attr,
                    //                 data_relationship,
                    //                 ctx,
                    //             )?;
                    //         }
                    //     }
                    // } else {
                    return Err(DomainError::DataStoreBadRequest(anyhow!(
                        "invalid input for multi-relation write"
                    )));
                    // }
                }
                _ => {
                    return Err(DomainError::DataStoreBadRequest(anyhow!(
                        "invalid combination for edge update"
                    )));
                }
            }
        }

        let collection = self.vertices.get_mut(&type_info.def_id).unwrap();
        let raw_props = collection.get_mut(&vertex_key.dynamic_key).unwrap();

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
                let entity_seq = self.query_single_vertex_collection(
                    struct_select,
                    &Filter::default(),
                    Limit(usize::MAX),
                    Option::<Cursor>::None,
                    IncludeTotalLen(false),
                    ctx,
                )?;

                for value in entity_seq.into_elements() {
                    let id = find_inherent_entity_id(&value, &ctx.ontology)?;
                    if let Some(id) = id {
                        let dynamic_key = Self::extract_dynamic_key(&id)?;

                        if dynamic_key == target_dynamic_key {
                            return Ok(value);
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
    fn write_new_vertex_inner(&mut self, vertex: Value, ctx: &DbContext) -> DomainResult<Value> {
        debug!(
            "write new vertex {:?}: {}",
            vertex.type_def_id(),
            ValueDebug(&vertex)
        );

        let type_info = ctx.ontology.get_type_info(vertex.type_def_id());
        let entity_info = type_info
            .entity_info()
            .ok_or(DomainError::NotAnEntity(vertex.type_def_id()))?;

        let (id, id_generated) = match find_inherent_entity_id(&vertex, &ctx.ontology)? {
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

        debug!("write vertex_id={}", ValueDebug(&id));

        let Value::Struct(mut struct_map, struct_tag) = vertex else {
            return Err(DomainError::EntityMustBeStruct);
        };

        if id_generated {
            struct_map.insert(entity_info.id_relationship_id, id.clone().into());
        }

        let mut raw_props: FnvHashMap<RelationshipId, Attr> = Default::default();

        let vertex_key = VertexKey {
            type_def_id: type_info.def_id,
            dynamic_key: Self::extract_dynamic_key(&id)?,
        };

        for (rel_id, attr) in *struct_map {
            let data_relationship = find_data_relationship(type_info, &rel_id)?;

            match (
                data_relationship.kind,
                attr,
                data_relationship.cardinality.1,
            ) {
                (DataRelationshipKind::Id, ..) => {}
                (DataRelationshipKind::Tree, attr, _) => {
                    raw_props.insert(rel_id, attr);
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Unit(unit),
                    ValueCardinality::Unit,
                ) => {
                    self.insert_entity_relationship(
                        vertex_key.clone(),
                        (projection, Some(EdgeWriteMode::Insert)),
                        EndoTuple {
                            elements: smallvec![unit],
                        },
                        data_relationship,
                        ctx,
                    )?;
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Tuple(tuple),
                    ValueCardinality::Unit,
                ) => {
                    self.insert_entity_relationship(
                        vertex_key.clone(),
                        (projection, Some(EdgeWriteMode::Insert)),
                        *tuple,
                        data_relationship,
                        ctx,
                    )?;
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Matrix(matrix),
                    ValueCardinality::IndexSet | ValueCardinality::List,
                ) => {
                    for tuple in matrix.into_rows() {
                        self.insert_entity_relationship(
                            vertex_key.clone(),
                            (projection, Some(EdgeWriteMode::Insert)),
                            tuple,
                            data_relationship,
                            ctx,
                        )?;
                    }
                }
                (DataRelationshipKind::Edge(proj), a, c) => {
                    todo!("{proj:?} {a:?} {c:?}")
                }
            }
        }

        let collection = self.vertices.get_mut(&struct_tag.def()).unwrap();

        if collection.contains_key(&vertex_key.dynamic_key) {
            return Err(DomainError::EntityAlreadyExists);
        }

        collection.insert(vertex_key.dynamic_key, raw_props);

        Ok(id)
    }

    fn insert_entity_relationship(
        &mut self,
        subject_key: VertexKey,
        (projection, write_mode): (EdgeCardinalProjection, Option<EdgeWriteMode>),
        tuple: EndoTuple<Value>,
        data_relationship: &DataRelationshipInfo,
        ctx: &DbContext,
    ) -> DomainResult<()> {
        debug!(
            "entity rel tuple: ({tuple:?}). Data relationship: {data_relationship:?}",
            data_relationship = ctx.ontology.debug(data_relationship)
        );

        fn write_mode_from_value(value: &Value) -> EdgeWriteMode {
            match value {
                Value::Struct(..) => EdgeWriteMode::Insert,
                Value::StructUpdate(..) => EdgeWriteMode::UpdateExisting,
                _ => EdgeWriteMode::Overwrite,
            }
        }

        let mut tuple_iter = tuple.elements.into_iter();
        let value = tuple_iter
            .next()
            .ok_or_else(|| DomainError::BadInput(anyhow!("misconfigured tuple")))?;
        let params = tuple_iter.next().unwrap_or_else(|| Value::unit());

        let (write_mode, foreign_key) = match &data_relationship.target {
            DataRelationshipTarget::Unambiguous(entity_def_id) => {
                if value.type_def_id() == *entity_def_id {
                    let write_mode = write_mode.unwrap_or(write_mode_from_value(&value));
                    let foreign_id = self.write_new_vertex_inner(value, ctx)?;
                    let foreign_key = VertexKey {
                        type_def_id: *entity_def_id,
                        dynamic_key: Self::extract_dynamic_key(&foreign_id)?,
                    };

                    (write_mode, foreign_key)
                } else {
                    (
                        write_mode.unwrap_or(write_mode_from_value(&params)),
                        self.resolve_foreign_key_for_edge(
                            *entity_def_id,
                            ctx.ontology
                                .get_type_info(*entity_def_id)
                                .entity_info()
                                .unwrap(),
                            value,
                            ctx,
                        )?,
                    )
                }
            }
            DataRelationshipTarget::Union(union_def_id) => {
                let variants = ctx.ontology.union_variants(*union_def_id);
                if variants.contains(&value.type_def_id()) {
                    // Explicit data struct of a given variant

                    let write_mode = write_mode.unwrap_or(write_mode_from_value(&value));
                    let entity_def_id = value.type_def_id();
                    let foreign_id = self.write_new_vertex_inner(value, ctx)?;
                    let foreign_key = VertexKey {
                        type_def_id: entity_def_id,
                        dynamic_key: Self::extract_dynamic_key(&foreign_id)?,
                    };

                    (write_mode, foreign_key)
                } else {
                    let (variant_def_id, entity_info) = variants
                        .iter()
                        .find_map(|variant_def_id| {
                            let entity_info = ctx
                                .ontology
                                .get_type_info(*variant_def_id)
                                .entity_info()
                                .unwrap();

                            if entity_info.id_value_def_id == value.type_def_id() {
                                Some((*variant_def_id, entity_info))
                            } else {
                                None
                            }
                        })
                        .expect("Corresponding entity def id not found for the given ID");

                    (
                        write_mode.unwrap_or(write_mode_from_value(&params)),
                        self.resolve_foreign_key_for_edge(variant_def_id, entity_info, value, ctx)?,
                    )
                }
            }
        };

        let edge_store = self
            .edges
            .get_mut(&projection.id)
            .expect("No edge collection");

        let mut edge_tuple: Vec<EdgeData<VertexKey>> = edge_store
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| match &column.data {
                EdgeVectorData::Keys(_) => EdgeData::Key(if idx == projection.subject.0 as usize {
                    subject_key.clone()
                } else if idx == projection.object.0 as usize {
                    foreign_key.clone()
                } else {
                    panic!()
                }),
                EdgeVectorData::Values(_) => EdgeData::Value(params.clone()),
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
                // Delete the edge(s) that match in the subject column:
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
        subject_key: VertexKey,
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

        let (subject_data, object_data) = match projection.proj() {
            (0, 1) => (EdgeData::Key(subject_key), EdgeData::Key(foreign_key)),
            (1, 0) => (EdgeData::Key(foreign_key), EdgeData::Key(subject_key)),
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
    ) -> DomainResult<VertexKey> {
        let foreign_key = Self::extract_dynamic_key(&id_value)?;
        let entity_data = self.look_up_vertex(foreign_entity_def_id, &foreign_key);

        if entity_data.is_none() && entity_info.is_self_identifying {
            // This type has UPSERT semantics.
            // Synthesize the entity, write it and move on..

            let entity_data =
                FnvHashMap::from_iter([(entity_info.id_relationship_id, Attr::from(id_value))]);
            self.write_new_vertex_inner(
                Value::Struct(Box::new(entity_data), foreign_entity_def_id.into()),
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
                    .serialize_attr(
                        AttrRef::Unit(&id_value),
                        &mut serde_json::Serializer::new(&mut buf),
                    )
                    .unwrap();
                String::from(std::str::from_utf8(&buf).unwrap())
            } else {
                "N/A".to_string()
            };

            return Err(DomainError::UnresolvedForeignKey(repr));
        }

        Ok(VertexKey {
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
                Value::Serial(Serial(serial_value), def_id.into())
            }
        }))
    }
}
