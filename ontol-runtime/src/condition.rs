use crate::{
    value::{PropertyId, Value},
    var::Var,
    DefId,
};

#[derive(Clone, Debug)]
pub struct Condition {
    pub clauses: Vec<Clause>,
}

#[derive(Clone, Debug)]
pub enum Clause {
    Root(Var),
    IsEntity(CondTerm, DefId),
    Attr(Var, PropertyId, (CondTerm, CondTerm)),
    Eq(Var, CondTerm),
    Or(Vec<Clause>),
}

#[derive(Clone, Debug)]
pub enum CondTerm {
    Wildcard,
    Var(Var),
    Value(Value),
}

impl From<Value> for CondTerm {
    fn from(value: Value) -> Self {
        Self::Value(value)
    }
}
