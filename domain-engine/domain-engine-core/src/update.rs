use fnv::FnvHashMap;
use ontol_runtime::value::{Attribute, PropertyId, Value};

/// Sanitize something that is known to be a data store update
/// after it has gone through domain translation.
pub fn sanitize_update(update: &mut Value) {
    let value = match update.take() {
        Value::Struct(mut attrs, def_id) | Value::StructUpdate(mut attrs, def_id) => {
            remove_none_attrs(&mut attrs);
            Value::StructUpdate(attrs, def_id)
        }
        other => other,
    };

    *update = value;
}

fn remove_none_attrs(attrs: &mut FnvHashMap<PropertyId, Attribute>) {
    attrs.retain(|_, attr| is_some(&attr.rel_params) && is_some(&attr.value))
}

fn is_some(value: &Value) -> bool {
    match value {
        Value::None(_) => false,
        Value::Struct(attrs, _) | Value::StructUpdate(attrs, _) => attrs
            .iter()
            .all(|(_, attr)| is_some(&attr.rel_params) && is_some(&attr.value)),
        Value::Sequence(seq, _) => seq
            .attrs
            .iter()
            .all(|attr| is_some(&attr.rel_params) && is_some(&attr.value)),
        _other => true,
    }
}
