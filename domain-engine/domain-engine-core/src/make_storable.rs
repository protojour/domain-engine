use fnv::FnvHashMap;
use ontol_runtime::{
    attr::Attr,
    interface::serde::processor::ProcessorMode,
    ontology::{
        aspects::DefsAspect,
        domain::{DataRelationshipKind, DataTreeRepr, Def},
        ontol::ValueGenerator,
    },
    value::{OctetSequence, Value, ValueTag},
    PropId,
};

use crate::{system::SystemAPI, DomainResult};

/// A converter/finalizer for ONTOL values that processeses the values before stored to a data store.
pub struct MakeStorable<'e> {
    pub(crate) defs: &'e DefsAspect,
    pub(crate) system: &'e dyn SystemAPI,
    pub(crate) mode: ProcessorMode,

    /// All generated times get the same value
    pub(crate) current_time: Option<chrono::DateTime<chrono::Utc>>,
}

impl<'e> MakeStorable<'e> {
    pub fn new(
        mode: ProcessorMode,
        ontology: &'e impl AsRef<DefsAspect>,
        system: &'e dyn SystemAPI,
    ) -> Self {
        Self {
            defs: ontology.as_ref(),
            system,
            mode,
            current_time: Some(system.current_time()),
        }
    }

    pub fn without_timestamps(
        mode: ProcessorMode,
        ontology: &'e impl AsRef<DefsAspect>,
        system: &'e dyn SystemAPI,
    ) -> Self {
        Self {
            defs: ontology.as_ref(),
            system,
            mode,
            current_time: None,
        }
    }

    pub fn make_storable(&self, value: &mut Value) -> DomainResult<()> {
        match value {
            Value::Struct(struct_map, type_def_id) => {
                let def = self.defs.def(type_def_id.def_id());

                self.process_struct(struct_map, def);

                // recurse into sub-properties
                for (prop_id, attr) in struct_map.iter_mut() {
                    let rel_kind = def
                        .data_relationships
                        .get(prop_id)
                        .map(|rel_info| &rel_info.kind);

                    match (rel_kind, attr) {
                        (
                            Some(DataRelationshipKind::Tree(DataTreeRepr::Crdt)),
                            Attr::Unit(Value::CrdtStruct(..)),
                        ) => {
                            // already CRDT
                        }
                        (
                            Some(DataRelationshipKind::Tree(DataTreeRepr::Crdt)),
                            Attr::Unit(value),
                        ) => {
                            let automerge_value =
                                self.ontol_to_automerge(std::mem::replace(value, Value::unit()))?;
                            *value = automerge_value;
                        }
                        (_, Attr::Unit(unit)) => {
                            self.make_storable(unit)?;
                        }
                        (_, Attr::Tuple(tuple)) => {
                            for value in tuple.elements.iter_mut() {
                                self.make_storable(value)?;
                            }
                        }
                        (_, Attr::Matrix(matrix)) => {
                            for column in matrix.columns.iter_mut() {
                                for value in column.elements_mut() {
                                    self.make_storable(value)?;
                                }
                            }
                        }
                    }
                }
            }
            Value::Sequence(seq, _) => {
                for value in seq.elements_mut() {
                    self.make_storable(value)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn process_struct(&self, struct_map: &mut FnvHashMap<PropId, Attr>, def: &Def) {
        let id_prop = def.entity().map(|entity| entity.id_prop);

        for (prop_id, rel_info) in &def.data_relationships {
            let Some(generator) = rel_info.generator else {
                continue;
            };

            // Don't mess with IDs here, this is the responsibility of the data store:
            if Some(*prop_id) == id_prop {
                continue;
            }

            let tag: ValueTag = rel_info.target.def_id().into();

            match generator {
                ValueGenerator::DefaultProc(_) => {}
                ValueGenerator::Uuid => {
                    struct_map.insert(
                        *prop_id,
                        Value::OctetSequence(
                            OctetSequence(
                                self.system
                                    .generate_uuid()
                                    .as_bytes()
                                    .iter()
                                    .cloned()
                                    .collect(),
                            ),
                            tag,
                        )
                        .into(),
                    );
                }
                ValueGenerator::Ulid => {
                    struct_map.insert(
                        *prop_id,
                        Value::OctetSequence(
                            OctetSequence(ulid::Ulid::new().to_bytes().into_iter().collect()),
                            tag,
                        )
                        .into(),
                    );
                }
                ValueGenerator::Autoincrement => {
                    panic!("Cannot auto increment value here");
                }
                ValueGenerator::CreatedAtTime => {
                    // FIXME: upsert semantics!
                    if let Some(timestamp) = self.current_time {
                        if matches!(self.mode, ProcessorMode::Create) {
                            struct_map
                                .insert(*prop_id, Value::ChronoDateTime(timestamp, tag).into());
                        }
                    }
                }
                ValueGenerator::UpdatedAtTime => {
                    if let Some(timestamp) = self.current_time {
                        struct_map.insert(*prop_id, Value::ChronoDateTime(timestamp, tag).into());
                    }
                }
            }
        }
    }
}
