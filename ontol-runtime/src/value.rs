use std::collections::HashMap;

use smartstring::alias::String;

use crate::{DefId, RelationId};

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
    Unit,
    Int(i64),
    Float(f64),
    Rational(num::rational::BigRational),
    String(String),
    Map(HashMap<RelationId, Value>),
    // Represents both dynamic lists and static tuples at runtime:
    Vec(Vec<Value>),
}

#[cfg(test)]
mod tests {
    use num::rational::BigRational;

    #[test]
    fn rational_arithmetic() {
        let a = BigRational::new(9.into(), 1.into());
        let b = BigRational::new(5.into(), 1.into());

        let c = a / b;

        assert_eq!(BigRational::new(9.into(), 5.into()), c);
    }
}
