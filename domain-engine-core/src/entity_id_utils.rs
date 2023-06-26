use ontol_runtime::{
    env::Env,
    value::{Data, PropertyId, Value},
};

use crate::DomainError;

pub fn find_inherent_entity_id(env: &Env, entity: &Value) -> Result<Option<Value>, DomainError> {
    let def_id = entity.type_def_id;
    let type_info = env.get_type_info(def_id);
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
