use fnv::FnvHashMap;
use ontol_runtime::{
    interface::serde::operator::{AliasOperator, SerdeOperator, SerdeOperatorAddr},
    ontology::{
        ontol::{
            text_pattern::{TextPattern, TextPatternConstantPart, TextPatternProperty},
            TextLikeType, ValueGenerator,
        },
        Ontology,
    },
    value::{OctetSequence, Value},
    DefId, PropId,
};

use crate::{domain_error::DomainErrorKind, system::SystemAPI, DomainError, DomainResult};

pub fn find_inherent_entity_id(
    entity_val: &Value,
    ontology: &Ontology,
) -> Result<Option<Value>, DomainError> {
    let def_id = entity_val.type_def_id();
    let def = ontology.def(def_id);
    let entity = def
        .entity()
        .ok_or(DomainErrorKind::NotAnEntity(def_id).into_error())?;

    let struct_map = match entity_val {
        Value::Struct(struct_map, _) => struct_map,
        _ => return Err(DomainErrorKind::EntityMustBeStruct.into_error()),
    };

    match struct_map.get(&entity.id_prop) {
        Some(attr) => Ok(attr.as_unit().cloned()),
        None => Ok(None),
    }
}

pub enum GeneratedId {
    Generated(Value),
    AutoIncrementSerial(DefId),
}

pub enum GeneratedIdContainer {
    Raw,
    SingletonStruct(DefId, PropId),
}

impl GeneratedIdContainer {
    pub fn wrap(self, id: Value) -> Value {
        match self {
            Self::Raw => id,
            Self::SingletonStruct(def_id, prop_id) => Value::Struct(
                Box::new(FnvHashMap::from_iter([(prop_id, id.into())])),
                def_id.into(),
            ),
        }
    }
}

pub fn try_generate_entity_id(
    id_operator_addr: SerdeOperatorAddr,
    value_generator: ValueGenerator,
    ontology: &Ontology,
    system: &dyn SystemAPI,
) -> DomainResult<(GeneratedId, GeneratedIdContainer)> {
    match (&ontology[id_operator_addr], value_generator) {
        (SerdeOperator::String(def_id), ValueGenerator::Uuid) => Ok((
            GeneratedId::Generated(Value::Text(
                format!("{}", system.generate_uuid()).into(),
                (*def_id).into(),
            )),
            GeneratedIdContainer::Raw,
        )),
        (SerdeOperator::TextPattern(def_id), _) => {
            match (ontology.get_text_like_type(*def_id), value_generator) {
                (Some(TextLikeType::Uuid), ValueGenerator::Uuid) => Ok((
                    GeneratedId::Generated(Value::OctetSequence(
                        OctetSequence(system.generate_uuid().as_bytes().iter().cloned().collect()),
                        (*def_id).into(),
                    )),
                    GeneratedIdContainer::Raw,
                )),
                _ => Err(DomainErrorKind::TypeCannotBeUsedForIdGeneration.into_error()),
            }
        }
        (SerdeOperator::CapturingTextPattern(def_id), _) => {
            if let Some(property) =
                analyze_text_pattern(ontology.get_text_pattern(*def_id).unwrap())
            {
                let def = ontology.def(property.type_def_id);
                let (generated_id, _) = try_generate_entity_id(
                    def.operator_addr.unwrap(),
                    value_generator,
                    ontology,
                    system,
                )?;

                Ok((
                    generated_id,
                    GeneratedIdContainer::SingletonStruct(*def_id, property.prop_id),
                ))
            } else {
                Err(DomainErrorKind::TypeCannotBeUsedForIdGeneration.into_error())
            }
        }
        (SerdeOperator::Serial(def_id), ValueGenerator::Autoincrement) => Ok((
            GeneratedId::AutoIncrementSerial(*def_id),
            GeneratedIdContainer::Raw,
        )),
        (
            SerdeOperator::Alias(AliasOperator {
                def, inner_addr, ..
            }),
            _,
        ) => match try_generate_entity_id(*inner_addr, value_generator, ontology, system)? {
            (GeneratedId::Generated(mut value), container) => {
                value.tag_mut().set_def_id(def.def_id);
                Ok((GeneratedId::Generated(value), container))
            }
            auto => Ok(auto),
        },
        _ => Err(DomainErrorKind::TypeCannotBeUsedForIdGeneration.into_error()),
    }
}

fn analyze_text_pattern(pattern: &TextPattern) -> Option<&TextPatternProperty> {
    let mut out_property = None;

    for part in &pattern.constant_parts {
        if let TextPatternConstantPart::Property(property) = part {
            if out_property.is_some() {
                return None;
            }

            out_property = Some(property);
        }
    }

    out_property
}
