use std::collections::HashMap;

use smartstring::alias::String;

use crate::relation::PropertyId;

pub enum Value {
    String(String),
    Compound(Compound),
}

pub struct Compound {
    pub attributes: HashMap<PropertyId, Value>,
}
