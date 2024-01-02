use fnv::FnvHashMap;
use ontol_runtime::{
    interface::serde::operator::{AliasOperator, SerdeOperator, SerdeOperatorAddr},
    ontology::Ontology,
    smart_format,
    text_like_types::TextLikeType,
    text_pattern::{TextPattern, TextPatternConstantPart, TextPatternProperty},
    value::{PropertyId, Value},
    value_generator::ValueGenerator,
    DefId,
};

use crate::{system::SystemAPI, DomainError, DomainResult};

pub fn find_inherent_entity_id(
    entity: &Value,
    ontology: &Ontology,
) -> Result<Option<Value>, DomainError> {
    let def_id = entity.type_def_id();
    let type_info = ontology.get_type_info(def_id);
    let entity_info = type_info
        .entity_info
        .as_ref()
        .ok_or(DomainError::NotAnEntity(def_id))?;

    let struct_map = match entity {
        Value::Struct(struct_map, _) | Value::StructUpdate(struct_map, _) => struct_map,
        _ => return Err(DomainError::EntityMustBeStruct),
    };

    match struct_map.get(&PropertyId::subject(entity_info.id_relationship_id)) {
        Some(attribute) => Ok(Some(attribute.value.clone())),
        None => Ok(None),
    }
}

pub enum GeneratedId {
    Generated(Value),
    AutoIncrementI64(DefId),
}

pub fn try_generate_entity_id(
    id_operator_addr: SerdeOperatorAddr,
    value_generator: ValueGenerator,
    ontology: &Ontology,
    system: &dyn SystemAPI,
) -> DomainResult<GeneratedId> {
    match (
        ontology.get_serde_operator(id_operator_addr),
        value_generator,
    ) {
        (SerdeOperator::String(def_id), ValueGenerator::Uuid) => Ok(GeneratedId::Generated(
            Value::Text(smart_format!("{}", system.generate_uuid()), *def_id),
        )),
        (SerdeOperator::TextPattern(def_id), _) => {
            match (ontology.get_text_like_type(*def_id), value_generator) {
                (Some(TextLikeType::Uuid), ValueGenerator::Uuid) => {
                    Ok(GeneratedId::Generated(Value::OctetSequence(
                        system.generate_uuid().as_bytes().iter().cloned().collect(),
                        *def_id,
                    )))
                }
                _ => Err(DomainError::TypeCannotBeUsedForIdGeneration),
            }
        }
        (SerdeOperator::CapturingTextPattern(def_id), _) => {
            if let Some(property) =
                analyze_text_pattern(ontology.get_text_pattern(*def_id).unwrap())
            {
                let type_info = ontology.get_type_info(property.type_def_id);
                match try_generate_entity_id(
                    type_info.operator_addr.unwrap(),
                    value_generator,
                    ontology,
                    system,
                )? {
                    GeneratedId::Generated(id) => Ok(GeneratedId::Generated(Value::Struct(
                        Box::new(FnvHashMap::from_iter([(property.property_id, id.into())])),
                        *def_id,
                    ))),
                    GeneratedId::AutoIncrementI64(_) => {
                        Err(DomainError::TypeCannotBeUsedForIdGeneration)
                    }
                }
            } else {
                Err(DomainError::TypeCannotBeUsedForIdGeneration)
            }
        }
        (SerdeOperator::I64(def_id, _), ValueGenerator::Autoincrement) => {
            Ok(GeneratedId::AutoIncrementI64(*def_id))
        }
        (
            SerdeOperator::Alias(AliasOperator {
                def, inner_addr, ..
            }),
            _,
        ) => match try_generate_entity_id(*inner_addr, value_generator, ontology, system)? {
            GeneratedId::Generated(mut value) => {
                *value.type_def_id_mut() = def.def_id;
                Ok(GeneratedId::Generated(value))
            }
            auto => Ok(auto),
        },
        _ => Err(DomainError::TypeCannotBeUsedForIdGeneration),
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
