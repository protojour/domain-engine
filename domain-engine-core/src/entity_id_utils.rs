use std::collections::BTreeMap;

use ontol_runtime::{
    interface::serde::operator::{AliasOperator, SerdeOperator, SerdeOperatorAddr},
    ontology::Ontology,
    smart_format,
    text_like_types::TextLikeType,
    text_pattern::{TextPattern, TextPatternConstantPart, TextPatternProperty},
    value::{Data, PropertyId, Value},
    value_generator::ValueGenerator,
    DefId,
};
use uuid::Uuid;

use crate::{DomainError, DomainResult};

pub fn find_inherent_entity_id(
    ontology: &Ontology,
    entity: &Value,
) -> Result<Option<Value>, DomainError> {
    let def_id = entity.type_def_id;
    let type_info = ontology.get_type_info(def_id);
    let entity_info = type_info
        .entity_info
        .as_ref()
        .ok_or(DomainError::NotAnEntity(def_id))?;

    let struct_map = match &entity.data {
        Data::Struct(struct_map) => struct_map,
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
    ontology: &Ontology,
    id_operator_addr: SerdeOperatorAddr,
    value_generator: ValueGenerator,
) -> DomainResult<GeneratedId> {
    match (
        ontology.get_serde_operator(id_operator_addr),
        value_generator,
    ) {
        (SerdeOperator::String(def_id), ValueGenerator::UuidV4) => {
            let string = smart_format!("{}", Uuid::new_v4());
            Ok(GeneratedId::Generated(Value::new(
                Data::Text(string),
                *def_id,
            )))
        }
        (SerdeOperator::TextPattern(def_id), _) => {
            match (ontology.get_text_like_type(*def_id), value_generator) {
                (Some(TextLikeType::Uuid), ValueGenerator::UuidV4) => {
                    Ok(GeneratedId::Generated(Value::new(
                        Data::OctetSequence(Uuid::new_v4().as_bytes().iter().cloned().collect()),
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
                    ontology,
                    type_info.operator_addr.unwrap(),
                    value_generator,
                )? {
                    GeneratedId::Generated(id) => Ok(GeneratedId::Generated(Value::new(
                        Data::Struct(BTreeMap::from([(property.property_id, id.into())])),
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
        ) => match try_generate_entity_id(ontology, *inner_addr, value_generator)? {
            GeneratedId::Generated(mut value) => {
                value.type_def_id = def.def_id;
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
