use fnv::FnvHashMap;
use ontol_runtime::{
    interface::serde::{
        operator::{AppliedVariants, SerdeOperator, SerdeOperatorAddr, SerdeProperty},
        processor::{ProcessorLevel, ProcessorMode},
    },
    ontology::{Ontology, TypeInfo},
    value::{Attribute, PropertyId, Value},
    value_generator::ValueGenerator,
    DefId, Role,
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
            Value::Struct(struct_map, type_def_id)
            | Value::StructUpdate(struct_map, type_def_id) => {
                let type_info = self.ontology.get_type_info(*type_def_id);
                if let Some(addr) = type_info.operator_addr {
                    self.generate_struct_relationships(struct_map, type_info, addr);
                }

                // recurse into sub-properties
                for attribute in struct_map.values_mut() {
                    self.generate_objects(&mut attribute.rel);
                    self.generate_objects(&mut attribute.val);
                }
            }
            Value::Sequence(seq, _) => {
                for attribute in &mut seq.attrs {
                    self.generate_objects(&mut attribute.rel);
                    self.generate_objects(&mut attribute.val);
                }
            }
            _ => {}
        }
    }

    fn generate_struct_relationships(
        &self,
        struct_map: &mut FnvHashMap<PropertyId, Attribute>,
        type_info: &TypeInfo,
        addr: SerdeOperatorAddr,
    ) {
        let operator = self.ontology.get_serde_operator(addr);
        let id_relationship = type_info
            .entity_info
            .as_ref()
            .map(|entity_info| entity_info.id_relationship_id);

        match operator {
            SerdeOperator::Struct(struct_op) => {
                for (key, property) in &struct_op.properties {
                    if !property.is_read_only() || matches!(property.property_id.role, Role::Object)
                    {
                        continue;
                    }
                    let relationship_id = property.property_id.relationship_id;

                    // Don't mess with IDs here, this is the responsibility of the data store:
                    if Some(relationship_id) == id_relationship {
                        continue;
                    }

                    match self.ontology.get_value_generator(relationship_id) {
                        Some(ValueGenerator::DefaultProc(_)) => {}
                        Some(ValueGenerator::Uuid) => {
                            struct_map.insert(
                                property.property_id,
                                Value::OctetSequence(
                                    self.system
                                        .generate_uuid()
                                        .as_bytes()
                                        .iter()
                                        .cloned()
                                        .collect(),
                                    self.property_def_id(property),
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
                                    property.property_id,
                                    Value::ChronoDateTime(
                                        self.current_time,
                                        self.property_def_id(property),
                                    )
                                    .into(),
                                );
                            }
                        }
                        Some(ValueGenerator::UpdatedAtTime) => {
                            struct_map.insert(
                                property.property_id,
                                Value::ChronoDateTime(
                                    self.current_time,
                                    self.property_def_id(property),
                                )
                                .into(),
                            );
                        }
                        None => {
                            panic!("No generator for {key}")
                        }
                    }
                }
            }
            SerdeOperator::Union(union_op) => {
                match union_op.applied_variants(ProcessorMode::Create, ProcessorLevel::new_root()) {
                    AppliedVariants::Unambiguous(child_addr) => {
                        self.generate_struct_relationships(struct_map, type_info, child_addr);
                    }
                    AppliedVariants::OneOf(_) => panic!("BUG"),
                }
            }
            _ => {}
        }
    }

    fn property_def_id(&self, property: &SerdeProperty) -> DefId {
        let operator = self.ontology.get_serde_operator(property.value_addr);
        self.operator_def_id(operator)
    }

    fn operator_def_id(&self, operator: &SerdeOperator) -> DefId {
        match operator {
            SerdeOperator::Unit => DefId::unit(),
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
            SerdeOperator::RelationSequence(_) => panic!(),
            SerdeOperator::ConstructorSequence(seq_op) => seq_op.def.def_id,
            SerdeOperator::Alias(alias_op) => alias_op.def.def_id,
            SerdeOperator::Union(union_op) => union_op.union_def().def_id,
            SerdeOperator::Struct(struct_op) => struct_op.def.def_id,
            SerdeOperator::IdSingletonStruct(.., addr) => {
                self.operator_def_id(self.ontology.get_serde_operator(*addr))
            }
        }
    }
}
