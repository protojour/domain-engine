use std::collections::HashMap;

use smartstring::alias::String;

use crate::relation::PropertyId;

#[derive(Debug)]
pub enum Value {
    // TODO: Big rational numbers
    Number(i64),
    String(String),
    Compound(HashMap<PropertyId, Value>),
}
