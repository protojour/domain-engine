use std::collections::{BTreeMap, BTreeSet};

use anyhow::anyhow;
use fnv::{FnvHashMap, FnvHashSet};
use itertools::Itertools;
use ontol_runtime::{
    attr::{Attr, AttrRef},
    interface::serde::{operator::SerdeOperatorAddr, processor::ProcessorMode},
    ontology::{
        domain::{
            DataRelationshipInfo, DataRelationshipKind, DataRelationshipTarget,
            EdgeCardinalProjection, Entity,
        },
        ontol::ValueGenerator,
    },
    property::ValueCardinality,
    query::{filter::Filter, select::Select},
    tuple::{CardinalIdx, EndoTuple},
    value::{Serial, Value, ValueDebug},
    DefId, EdgeId, RelId,
};
use tracing::{debug, debug_span, warn};

use domain_engine_core::{
    entity_id_utils::{find_inherent_entity_id, try_generate_entity_id, GeneratedId},
    transact::DataOperation,
    DomainError, DomainResult,
};

use crate::{
    core::{
        find_data_relationship, DbContext, EdgeColumnMatch, EdgeData, EdgeVectorData, VertexKey,
    },
    query::{Cursor, IncludeTotalLen, Limit},
};

use super::core::InMemoryStore;

#[derive(Clone, Copy, Debug)]
enum EdgeWriteMode {
    Insert,
    Overwrite,
    UpdateExisting,
}

impl InMemoryStore {
    pub fn upsert_entity(
        &mut self,
        value: Value,
        select: &Select,
        ctx: &DbContext,
    ) -> DomainResult<(Value, DataOperation)> {
        let entity_id = find_inherent_entity_id(&value, &ctx.ontology)?
            .ok_or_else(|| DomainError::EntityNotFound)?;
        let def = ctx.ontology.def(value.type_def_id());
        let dynamic_key = Self::extract_dynamic_key(&entity_id)?;

        if self
            .vertices
            .get(&def.id)
            .unwrap()
            .contains_key(&dynamic_key)
        {
            Ok((
                self.update_entity(value, select, ctx)?,
                DataOperation::Updated,
            ))
        } else {
            Ok((
                self.write_new_entity(value, select, ctx)?,
                DataOperation::Inserted,
            ))
        }
    }

    pub fn write_new_entity(
        &mut self,
        value: Value,
        select: &Select,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        let entity_id = self.write_new_vertex_inner(value, ctx)?;
        self.post_write_select(entity_id, select, ctx)
    }

    pub fn update_entity(
        &mut self,
        value: Value,
        select: &Select,
        ctx: &DbContext,
    ) -> DomainResult<Value> {
        let _entered = debug_span!("upd_vtx", id = ?value.type_def_id()).entered();

        debug!("update entity: {:#?}", value);

        let entity_id = find_inherent_entity_id(&value, &ctx.ontology)?
            .ok_or_else(|| DomainError::EntityNotFound)?;
        let def = ctx.ontology.def(value.type_def_id());
        let vertex_key = VertexKey {
            type_def_id: def.id,
            dynamic_key: Self::extract_dynamic_key(&entity_id)?,
        };

        if !self
            .vertices
            .get(&def.id)
            .unwrap()
            .contains_key(&vertex_key.dynamic_key)
        {
            return Err(DomainError::EntityNotFound);
        }

        let mut raw_props_update: BTreeMap<RelId, Attr> = Default::default();

        let Value::Struct(data_struct, _) = value else {
            return Err(DomainError::BadInputData(anyhow!("Expected a struct")));
        };

        let mut edge_builders: FnvHashMap<
            EdgeId,
            BTreeMap<CardinalIdx, (EdgeWriteMode, EdgeData<VertexKey>)>,
        > = Default::default();

        for (rel_id, attr) in *data_struct {
            let data_relationship = find_data_relationship(def, &rel_id)?;

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
                    edge_builders
                        .entry(projection.id)
                        .or_insert_with(|| {
                            BTreeMap::from_iter([(
                                projection.subject,
                                (EdgeWriteMode::Overwrite, EdgeData::Key(vertex_key.clone())),
                            )])
                        })
                        .insert(
                            projection.object,
                            (EdgeWriteMode::Overwrite, EdgeData::Value(unit)),
                        );
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Matrix(matrix),
                    ValueCardinality::IndexSet | ValueCardinality::List,
                ) => {
                    for tuple in matrix.into_rows() {
                        if tuple.elements.iter().any(|value| value.tag().is_delete()) {
                            let mut iter = tuple.elements.into_iter();
                            let foreign_key = iter.next().unwrap();

                            self.delete_edge(
                                vertex_key.clone(),
                                projection,
                                foreign_key,
                                data_relationship,
                                ctx,
                            )?;
                        } else {
                            self.write_edge(
                                projection.id,
                                endo_tuple_to_edge_input(
                                    projection,
                                    &vertex_key,
                                    tuple,
                                    EdgeWriteMode::Overwrite,
                                ),
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

        for (edge_id, data) in edge_builders {
            self.write_edge(edge_id, data, ctx)?;
        }

        let collection = self.vertices.get_mut(&def.id).unwrap();
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
        let _entered = debug_span!("wr_vtx", id = ?vertex.type_def_id()).entered();

        debug!("value: {}", ValueDebug(&vertex));

        let def = ctx.ontology.def(vertex.type_def_id());
        let entity = def
            .entity()
            .ok_or(DomainError::NotAnEntity(vertex.type_def_id()))?;

        let (id, id_generated) = match find_inherent_entity_id(&vertex, &ctx.ontology)? {
            Some(id) => (id, false),
            None => {
                let value_generator = entity.id_value_generator.ok_or_else(|| {
                    DomainError::DataStoreBadRequest(anyhow!("No id provided and no ID generator"))
                })?;

                (
                    self.generate_entity_id(entity.id_operator_addr, value_generator, ctx)?,
                    true,
                )
            }
        };

        let Value::Struct(mut struct_map, struct_tag) = vertex else {
            return Err(DomainError::EntityMustBeStruct);
        };

        if id_generated {
            struct_map.insert(entity.id_relationship_id, id.clone().into());
        }

        let mut raw_props: FnvHashMap<RelId, Attr> = Default::default();

        let vertex_key = VertexKey {
            type_def_id: def.id,
            dynamic_key: Self::extract_dynamic_key(&id)?,
        };

        debug!("write vertex_key={vertex_key:?}");

        let mut edge_builders: FnvHashMap<
            EdgeId,
            BTreeMap<CardinalIdx, (EdgeWriteMode, EdgeData<VertexKey>)>,
        > = Default::default();

        for (rel_id, attr) in *struct_map {
            let data_relationship = find_data_relationship(def, &rel_id)?;

            match (
                data_relationship.kind,
                attr,
                data_relationship.cardinality.1,
            ) {
                (DataRelationshipKind::Tree | DataRelationshipKind::Id, attr, _) => {
                    raw_props.insert(rel_id, attr);
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Unit(unit),
                    ValueCardinality::Unit,
                ) => {
                    edge_builders
                        .entry(projection.id)
                        .or_insert_with(|| {
                            BTreeMap::from_iter([(
                                projection.subject,
                                (EdgeWriteMode::Insert, EdgeData::Key(vertex_key.clone())),
                            )])
                        })
                        .insert(
                            projection.object,
                            (EdgeWriteMode::Insert, EdgeData::Value(unit.clone())),
                        );
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Tuple(tuple),
                    ValueCardinality::Unit,
                ) => {
                    self.write_edge(
                        projection.id,
                        endo_tuple_to_edge_input(
                            projection,
                            &vertex_key,
                            *tuple,
                            EdgeWriteMode::Insert,
                        ),
                        ctx,
                    )?;
                }
                (
                    DataRelationshipKind::Edge(projection),
                    Attr::Matrix(matrix),
                    ValueCardinality::IndexSet | ValueCardinality::List,
                ) => {
                    for tuple in matrix.into_rows() {
                        self.write_edge(
                            projection.id,
                            endo_tuple_to_edge_input(
                                projection,
                                &vertex_key,
                                tuple,
                                EdgeWriteMode::Insert,
                            ),
                            ctx,
                        )?;
                    }
                }
                (DataRelationshipKind::Edge(proj), a, c) => {
                    todo!("{proj:?} {a:?} {c:?}")
                }
            }
        }

        for (edge_id, data) in edge_builders {
            self.write_edge(edge_id, data, ctx)?;
        }

        let collection = self.vertices.get_mut(&struct_tag.def_id()).unwrap();

        if collection.contains_key(&vertex_key.dynamic_key) {
            if !entity.is_self_identifying {
                return Err(DomainError::EntityAlreadyExists);
            } else {
                // "upsert", not an error
            }
        } else {
            collection.insert(vertex_key.dynamic_key, raw_props);
        }

        Ok(id)
    }

    fn write_edge(
        &mut self,
        edge_id: EdgeId,
        input: BTreeMap<CardinalIdx, (EdgeWriteMode, EdgeData<VertexKey>)>,
        ctx: &DbContext,
    ) -> DomainResult<()> {
        let _entered = debug_span!("wr_edge", id = ?edge_id).entered();

        {
            let edge_store = self.edges.get(&edge_id).expect("No edge collection");
            let edge_info = edge_store
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| {
                    let cardinal_idx = CardinalIdx(idx as u8);
                    (
                        cardinal_idx,
                        column.vertex_union.clone(),
                        format!("unique={}", column.unique),
                    )
                })
                .collect_vec();
            debug!("write_edge data: {input:#?} edge_info: {edge_info:?}");
        }

        fn write_mode_from_value(value: &Value) -> EdgeWriteMode {
            match value {
                Value::Struct(_, tag) => {
                    if tag.is_update() {
                        EdgeWriteMode::UpdateExisting
                    } else {
                        EdgeWriteMode::Insert
                    }
                }
                _ => EdgeWriteMode::Overwrite,
            }
        }

        let mut mapped_input: BTreeMap<CardinalIdx, (EdgeWriteMode, EdgeData<VertexKey>)> =
            Default::default();

        for (cardinal_idx, (write_mode, input_data)) in input {
            let mut mapped_write_mode = write_mode;

            let edge_data = match input_data {
                EdgeData::Key(key) => EdgeData::Key(key),
                EdgeData::Value(value) => {
                    match self.match_edge_column(edge_id, cardinal_idx, value.type_def_id(), ctx) {
                        EdgeColumnMatch::VertexIdOf(vertex_def_id) => {
                            let vertex_key = self.resolve_foreign_key_for_edge(
                                vertex_def_id,
                                ctx.ontology.def(vertex_def_id).entity().unwrap(),
                                value,
                                ctx,
                            )?;

                            EdgeData::Key(vertex_key)
                        }
                        EdgeColumnMatch::VertexValue(vertex_def_id) => {
                            let vertex_id = self.write_new_vertex_inner(value, ctx)?;
                            EdgeData::Key(VertexKey {
                                type_def_id: vertex_def_id,
                                dynamic_key: Self::extract_dynamic_key(&vertex_id)?,
                            })
                        }
                        EdgeColumnMatch::EdgeValue => {
                            mapped_write_mode = write_mode_from_value(&value);
                            EdgeData::Value(value)
                        }
                    }
                }
            };

            mapped_input.insert(cardinal_idx, (mapped_write_mode, edge_data));
        }

        let edge_store = self.edges.get_mut(&edge_id).expect("No edge collection");

        if mapped_input
            .iter()
            .any(|(_, (write_mode, _))| matches!(write_mode, EdgeWriteMode::UpdateExisting))
        {
            let mut match_union: Vec<BTreeSet<usize>> = Default::default();

            for (cardinal_idx, (write_mode, data)) in &mapped_input {
                if !matches!(write_mode, EdgeWriteMode::UpdateExisting) {
                    let mut matching = Default::default();
                    edge_store.collect_column_eq(*cardinal_idx, data, &mut matching);
                    match_union.push(matching);
                }
            }

            let matching: BTreeSet<usize> = if match_union.is_empty() {
                BTreeSet::new()
            } else {
                let mut iter = match_union.into_iter();
                let mut intersection = iter.next().unwrap();

                for next in iter {
                    intersection = intersection.intersection(&next).copied().collect();
                }

                intersection
            };

            if matching.is_empty() {
                return Err(DomainError::EntityNotFound);
            }

            for (cardinal_idx, (write_mode, data)) in mapped_input {
                if matches!(write_mode, EdgeWriteMode::UpdateExisting) {
                    let column = edge_store.columns.get_mut(cardinal_idx.0 as usize).unwrap();

                    match (&mut column.data, data) {
                        (EdgeVectorData::Values(values), EdgeData::Value(value)) => {
                            for edge_index in &matching {
                                values[*edge_index] = value.clone();
                            }
                        }
                        _ => panic!("not updatable"),
                    }
                }
            }
        } else {
            let mut edge_tuple = Vec::with_capacity(edge_store.columns.len());
            let mut overwrite_columns: FnvHashSet<CardinalIdx> = Default::default();

            for (cardinal_idx, (write_mode, data)) in mapped_input {
                if cardinal_idx.0 as usize > edge_tuple.len() {
                    panic!("incomplete tuple");
                }

                if matches!(write_mode, EdgeWriteMode::Overwrite) {
                    overwrite_columns.insert(cardinal_idx);
                }

                edge_tuple.push(data);
            }

            let mut to_delete = BTreeSet::default();
            edge_store.collect_unique_violations(&edge_tuple, &mut to_delete);

            for overwrite_column in overwrite_columns {
                edge_store.collect_column_eq(
                    overwrite_column,
                    &edge_tuple[overwrite_column.0 as usize],
                    &mut to_delete,
                );
            }

            debug!("to_delete: {to_delete:?}");

            edge_store.delete_edges(to_delete);
            edge_store.push_tuple(edge_tuple);
        }

        Ok(())
    }

    fn delete_edge(
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
                    ctx.ontology.def(*entity_def_id).entity().unwrap(),
                    foreign_id,
                    ctx,
                )?,
            DataRelationshipTarget::Union(union_def_id) => {
                let variants = ctx.ontology.union_variants(*union_def_id);
                let (variant_def_id, entity) = variants
                    .iter()
                    .find_map(|variant_def_id| {
                        let entity = ctx.ontology.def(*variant_def_id).entity().unwrap();

                        if entity.id_value_def_id == foreign_id.type_def_id() {
                            Some((*variant_def_id, entity))
                        } else {
                            None
                        }
                    })
                    .expect("Corresponding entity def id not found for the given ID");

                self.resolve_foreign_key_for_edge(variant_def_id, entity, foreign_id, ctx)?
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
        entity: &Entity,
        id_value: Value,
        ctx: &DbContext,
    ) -> DomainResult<VertexKey> {
        let foreign_key = Self::extract_dynamic_key(&id_value)?;
        let entity_data = self.look_up_vertex(foreign_entity_def_id, &foreign_key);

        if entity_data.is_none() && entity.is_self_identifying {
            // This type has UPSERT semantics.
            // Synthesize the entity, write it and move on..

            let entity_data =
                FnvHashMap::from_iter([(entity.id_relationship_id, Attr::from(id_value))]);
            self.write_new_vertex_inner(
                Value::Struct(Box::new(entity_data), foreign_entity_def_id.into()),
                ctx,
            )?;
        } else if entity_data.is_none() {
            let def = ctx.ontology.def(id_value.type_def_id());
            let repr = if let Some(operator_addr) = def.operator_addr {
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

fn endo_tuple_to_edge_input(
    projection: EdgeCardinalProjection,
    subject_key: &VertexKey,
    tuple: EndoTuple<Value>,
    common_write_mode: EdgeWriteMode,
) -> BTreeMap<CardinalIdx, (EdgeWriteMode, EdgeData<VertexKey>)> {
    debug!("endo tuple to edge input: {projection:?}, subject_key = {subject_key:?}, tuple = {tuple:?}");

    let mut edge_input: BTreeMap<CardinalIdx, (EdgeWriteMode, EdgeData<VertexKey>)> =
        Default::default();

    let mut cardinal_idx: u8 = 0;

    for value in tuple.elements {
        if CardinalIdx(cardinal_idx) == projection.subject {
            edge_input.insert(
                CardinalIdx(cardinal_idx),
                (common_write_mode, EdgeData::Key(subject_key.clone())),
            );
            cardinal_idx += 1;
        }

        edge_input.insert(
            CardinalIdx(cardinal_idx),
            (common_write_mode, EdgeData::Value(value)),
        );
        cardinal_idx += 1;
    }

    if CardinalIdx(cardinal_idx) == projection.subject {
        edge_input.insert(
            CardinalIdx(cardinal_idx),
            (common_write_mode, EdgeData::Key(subject_key.clone())),
        );
    }

    edge_input
}
