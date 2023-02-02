use std::collections::HashMap;

use smartstring::alias::String;

use crate::{DefId, PropertyId};

#[derive(Clone, Debug)]
pub struct Value {
    pub data: Data,
    pub type_def_id: DefId,
}

impl Value {
    pub fn new(data: Data, type_def_id: DefId) -> Self {
        Self { data, type_def_id }
    }
}

#[derive(Clone, Debug)]
pub enum Data {
    // TODO: Big rational numbers
    Number(i64),
    String(String),
    Map(HashMap<PropertyId, Value>),
    // Represents both dynamic lists and static tuples at runtime:
    Vec(Vec<Value>),
}
