use crate::{value::PropertyId, var::Var, DefId};

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
    Text(Box<str>),
    I64(i64),
    F64(f64),
}
