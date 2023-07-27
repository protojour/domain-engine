use ontol_runtime::{
    ontology::Ontology,
    string_pattern::{StringPattern, StringPatternConstantPart, StringPatternProperty},
    value::{Data, PropertyId, Value},
};

use crate::DomainError;

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

pub fn analyze_string_pattern(pattern: &StringPattern) -> Option<&StringPatternProperty> {
    let mut out_property = None;

    for part in &pattern.constant_parts {
        if let StringPatternConstantPart::Property(property) = part {
            if out_property.is_some() {
                return None;
            }

            out_property = Some(property);
        }
    }

    out_property
}
