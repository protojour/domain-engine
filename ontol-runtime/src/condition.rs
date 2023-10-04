use std::fmt::{Debug, Display};

use crate::{
    value::{PropertyId, Value},
    var::Var,
    DefId,
};

#[derive(Clone, Default)]
pub struct Condition {
    pub clauses: Vec<Clause>,
}

/// The PartialEq implementation is only meant for debugging purposes
impl PartialEq<str> for Condition {
    fn eq(&self, other: &str) -> bool {
        let str = format!("{self}");
        str == other
    }
}

#[derive(Clone)]
pub enum Clause {
    Root(Var),
    IsEntity(CondTerm, DefId),
    Attr(Var, PropertyId, (CondTerm, CondTerm)),
    Eq(Var, CondTerm),
    Or(Vec<Clause>),
}

#[derive(Clone)]
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

impl Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for clause in &self.clauses {
            writeln!(f, "{clause}")?;
        }
        Ok(())
    }
}

impl Display for Clause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Clause::Root(var) => write!(f, "(root {var})"),
            Clause::IsEntity(term, def_id) => write!(f, "(is-entity {term} {def_id:?})"),
            Clause::Attr(var, prop_id, (rel, val)) => {
                write!(f, "(attr {var} {prop_id} ({rel} {val}))")
            }
            Clause::Eq(var, term) => write!(f, "(eq {var} {term})"),
            Clause::Or(_) => write!(f, "(or ..)"),
        }
    }
}

impl Display for CondTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wildcard => write!(f, "_"),
            Self::Var(var) => write!(f, "{var}"),
            Self::Value(value) => write!(f, "{:?}", value.data),
        }
    }
}

impl Debug for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Debug for Clause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Debug for CondTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}
