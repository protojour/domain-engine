use serde_json::Value;

pub fn json_map(array: Value, f: impl Fn(Value) -> Value) -> Value {
    let vec = to_array(array).into_iter().map(f).collect();
    Value::Array(vec)
}

pub fn json_prop(value: Value, property: &str) -> Value {
    to_object(value).remove(property).unwrap()
}

fn to_array(value: Value) -> Vec<Value> {
    let Value::Array(array) = value else {
        panic!("Not an array");
    };
    array
}

fn to_object(value: Value) -> serde_json::Map<String, Value> {
    let Value::Object(object) = value else {
        panic!("Not an object");
    };
    object
}
