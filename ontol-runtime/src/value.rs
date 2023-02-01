use std::collections::HashMap;

use smartstring::alias::String;

use crate::PropertyId;

#[derive(Clone, Debug)]
pub enum Value {
    // TODO: Big rational numbers
    Number(i64),
    String(String),
    Map(HashMap<PropertyId, Value>),
    // Represents both dynamic lists and static tuples at runtime:
    Vec(Vec<Value>),
}
