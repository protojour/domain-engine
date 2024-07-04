use fnv::FnvHashMap;
use ontol_runtime::{
    attr::Attr,
    interface::serde::{
        operator::{AppliedVariants, SerdeOperator, SerdeOperatorAddr, SerdeProperty},
        processor::{ProcessorLevel, ProcessorMode},
    },
    ontology::{domain::Def, ontol::ValueGenerator, Ontology},
    value::{Value, ValueTag},
    DefId, RelationshipId,
};

use crate::system::SystemAPI;

/// Relationship and object generator based on auto-generator information from ONTOL.
pub struct ObjectGenerator<'e> {
    ontology: &'e Ontology,
    system: &'e dyn SystemAPI,
    mode: ProcessorMode,

    /// All generated times get the same value
    current_time: chrono::DateTime<chrono::Utc>,
}

impl<'e> ObjectGenerator<'e> {
    pub fn new(mode: ProcessorMode, ontology: &'e Ontology, system: &'e dyn SystemAPI) -> Self {
        Self {
            ontology,
            system,
            mode,
            current_time: system.current_time(),
        }
    }

    pub fn generate_objects(&self, value: &mut Value) {
        match value {
            Value::Struct(struct_map, type_def_id) => {
                let def = self.ontology.def(type_def_id.def_id());
                if let Some(addr) = def.operator_addr {
                    self.generate_struct_relationships(struct_map, def, addr);
                }

                // recurse into sub-properties
                for attr in struct_map.values_mut() {
                    self.generate_attr(attr);
                }
            }
            Value::Sequence(seq, _) => {
                for value in seq.elements_mut() {
                    self.generate_objects(value);
                }
            }
            _ => {}
        }
    }

    fn generate_attr(&self, attr: &mut Attr) {
        match attr {
            Attr::Unit(unit) => self.generate_objects(unit),
            Attr::Tuple(tuple) => {
                for value in tuple.elements.iter_mut() {
                    self.generate_objects(value);
                }
            }
            Attr::Matrix(matrix) => {
                for column in matrix.columns.iter_mut() {
                    for value in column.elements_mut() {
                        self.generate_objects(value);
                    }
                }
            }
        }
    }

    fn generate_struct_relationships(
        &self,
        struct_map: &mut FnvHashMap<RelationshipId, Attr>,
        def: &Def,
        addr: SerdeOperatorAddr,
    ) {
        let operator = &self.ontology[addr];
        let id_relationship = def.entity().map(|entity| entity.id_relationship_id);

        match operator {
            SerdeOperator::Struct(struct_op) => {
                for (key, property) in struct_op.properties.iter() {
                    if !property.is_generator() {
                        continue;
                    }

                    // Don't mess with IDs here, this is the responsibility of the data store:
                    if Some(property.rel_id) == id_relationship {
                        continue;
                    }

                    match self.ontology.get_value_generator(property.rel_id) {
                        Some(ValueGenerator::DefaultProc(_)) => {}
                        Some(ValueGenerator::Uuid) => {
                            struct_map.insert(
                                property.rel_id,
                                Value::OctetSequence(
                                    self.system
                                        .generate_uuid()
                                        .as_bytes()
                                        .iter()
                                        .cloned()
                                        .collect(),
                                    self.property_tag(property),
                                )
                                .into(),
                            );
                        }
                        Some(ValueGenerator::Autoincrement) => {
                            panic!("Cannot auto increment value here");
                        }
                        Some(ValueGenerator::CreatedAtTime) => {
                            if matches!(self.mode, ProcessorMode::Create) {
                                struct_map.insert(
                                    property.rel_id,
                                    Value::ChronoDateTime(
                                        self.current_time,
                                        self.property_tag(property),
                                    )
                                    .into(),
                                );
                            }
                        }
                        Some(ValueGenerator::UpdatedAtTime) => {
                            struct_map.insert(
                                property.rel_id,
                                Value::ChronoDateTime(
                                    self.current_time,
                                    self.property_tag(property),
                                )
                                .into(),
                            );
                        }
                        None => {
                            panic!("No generator for {key}", key = key.arc_str())
                        }
                    }
                }
            }
            SerdeOperator::Union(union_op) => {
                match union_op.applied_variants(ProcessorMode::Create, ProcessorLevel::new_root()) {
                    AppliedVariants::Unambiguous(child_addr) => {
                        self.generate_struct_relationships(struct_map, def, child_addr);
                    }
                    AppliedVariants::OneOf(_) => panic!("BUG"),
                }
            }
            _ => {}
        }
    }

    fn property_tag(&self, property: &SerdeProperty) -> ValueTag {
        let operator = &self.ontology[property.value_addr];
        self.operator_def_id(operator).into()
    }

    fn operator_def_id(&self, operator: &SerdeOperator) -> DefId {
        match operator {
            SerdeOperator::AnyPlaceholder | SerdeOperator::Unit => DefId::unit(),
            SerdeOperator::True(def_id)
            | SerdeOperator::False(def_id)
            | SerdeOperator::Boolean(def_id)
            | SerdeOperator::I64(def_id, _)
            | SerdeOperator::I32(def_id, _)
            | SerdeOperator::F64(def_id, _)
            | SerdeOperator::Serial(def_id)
            | SerdeOperator::String(def_id)
            | SerdeOperator::StringConstant(_, def_id)
            | SerdeOperator::TextPattern(def_id)
            | SerdeOperator::CapturingTextPattern(def_id) => *def_id,
            SerdeOperator::DynamicSequence => panic!("DynamicSequence"),
            SerdeOperator::RelationList(_) | SerdeOperator::RelationIndexSet(_) => panic!(),
            SerdeOperator::ConstructorSequence(seq_op) => seq_op.def.def_id,
            SerdeOperator::Alias(alias_op) => alias_op.def.def_id,
            SerdeOperator::Union(union_op) => union_op.union_def().def_id,
            SerdeOperator::Struct(struct_op) => struct_op.def.def_id,
            SerdeOperator::IdSingletonStruct(.., addr) => {
                self.operator_def_id(&self.ontology[*addr])
            }
        }
    }
}
