use std::fmt::{Debug, Display};

use crate::{
    value::{PropertyId, Value},
    var::Var,
    DefId,
};

#[derive(Clone)]
pub struct Condition<Term> {
    pub clauses: Vec<Clause<Term>>,
}

impl<Term> Default for Condition<Term> {
    fn default() -> Self {
        Self { clauses: vec![] }
    }
}

/// The PartialEq implementation is only meant for debugging purposes
impl<Term> PartialEq<str> for Condition<Term>
where
    Term: Display,
{
    fn eq(&self, other: &str) -> bool {
        let str = format!("{self}");
        str == other
    }
}

#[derive(Clone)]
pub enum Clause<Term> {
    Root(Var),
    IsEntity(Term, DefId),
    Attr(Var, PropertyId, (Term, Term)),
    Eq(Var, Term),
    Or(Vec<Term>),
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

impl<Term> Display for Condition<Term>
where
    Term: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for clause in &self.clauses {
            writeln!(f, "{clause}")?;
        }
        Ok(())
    }
}

impl<Term> Display for Clause<Term>
where
    Term: Display,
{
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

impl<Term> Debug for Condition<Term>
where
    Term: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl<Term> Debug for Clause<Term>
where
    Term: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Debug for CondTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}
