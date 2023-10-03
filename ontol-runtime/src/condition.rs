use crate::{value::PropertyId, DefId};

pub struct Condition {
    pub clauses: Vec<Clause>,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Hash)]
pub struct UniVar(pub u32);

impl From<UniVar> for u32 {
    fn from(value: UniVar) -> Self {
        value.0
    }
}

impl From<UniVar> for usize {
    fn from(value: UniVar) -> Self {
        value.0 as usize
    }
}

impl From<usize> for UniVar {
    fn from(value: usize) -> Self {
        UniVar(value as u32)
    }
}

pub enum Clause {
    Root(UniVar),
    IsEntity(CondTerm, DefId),
    Attr(UniVar, PropertyId, (CondTerm, CondTerm)),
    Eq(UniVar, CondTerm),
    Or(Vec<Clause>),
}

pub enum CondTerm {
    Wildcard,
    Var(UniVar),
    Text(Box<str>),
    I64(i64),
    F64(f64),
}
