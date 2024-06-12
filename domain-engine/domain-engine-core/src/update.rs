use fnv::FnvHashMap;
use ontol_runtime::{attr::Attr, value::Value, RelationshipId};

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

fn remove_none_attrs(attrs: &mut FnvHashMap<RelationshipId, Attr>) {
    attrs.retain(|_, attr| attr_exists(attr))
}

fn attr_exists(attr: &Attr) -> bool {
    match attr {
        Attr::Unit(unit) => exists(unit),
        Attr::Tuple(tuple) => tuple.elements.iter().all(exists),
        Attr::Matrix(matrix) => matrix
            .columns
            .iter()
            .all(|column| column.elements().iter().all(exists)),
    }
}

fn exists(value: &Value) -> bool {
    match value {
        Value::Void(_) => false,
        Value::Struct(attrs, _) | Value::StructUpdate(attrs, _) => {
            attrs.iter().all(|(_, attr)| attr_exists(attr))
        }
        Value::Sequence(seq, _) => seq.elements().iter().all(exists),
        _other => true,
    }
}
