use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::{
    format_utils::Literal,
    value::{PropertyId, Value, ValueDebug},
    var::Var,
    DefId,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Condition<Term> {
    pub clauses: Vec<Clause<Term>>,
}

impl<Term> Default for Condition<Term> {
    fn default() -> Self {
        Self { clauses: vec![] }
    }
}

/// The PartialEq implementation is only meant for debugging purposes
impl<'a, Term> PartialEq<Literal<'a>> for Condition<Term>
where
    Term: Display,
{
    fn eq(&self, other: &Literal<'a>) -> bool {
        let a = format!("{self}");
        let b = format!("{other}");
        a == b
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Clause<Term> {
    Root(Var),
    IsEntity(Term, DefId),
    Attr(Var, PropertyId, (Term, Term)),
    Eq(Var, Term),
    Or(Vec<Term>),
}

#[derive(Clone, Serialize, Deserialize)]
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
            Self::Value(value) => write!(f, "{}", ValueDebug(value)),
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
