use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};
use thin_vec::{thin_vec, ThinVec};

use crate::{
    format_utils::Literal,
    value::{PropertyId, Value, ValueDebug},
    var::Var,
    DefId,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Condition {
    pub clauses: ThinVec<Clause<Var, CondTerm>>,
    next_cond_var: Var,
}

impl Condition {
    pub fn root_def_id(&self) -> Option<DefId> {
        self.clauses.iter().find_map(|clause| match clause {
            Clause::IsEntity(_, def_id) => Some(*def_id),
            _ => None,
        })
    }

    pub fn mk_cond_var(&mut self) -> Var {
        let var = self.next_cond_var;
        self.next_cond_var.0 += 1;
        var
    }
}

impl From<ThinVec<Clause<Var, CondTerm>>> for Condition {
    fn from(value: ThinVec<Clause<Var, CondTerm>>) -> Self {
        Self {
            clauses: value,
            next_cond_var: Var(0),
        }
    }
}

impl Default for Condition {
    fn default() -> Self {
        Self {
            clauses: thin_vec![],
            next_cond_var: Var(0),
        }
    }
}

/// The PartialEq implementation is only meant for debugging purposes
impl<'a> PartialEq<Literal<'a>> for Condition {
    fn eq(&self, other: &Literal<'a>) -> bool {
        let a = format!("{self}");
        let b = format!("{other}");
        a == b
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Clause<V, Term> {
    Root(V),
    IsEntity(Term, DefId),
    /// The left variable is connected via a property to the right variable.
    /// The right variable represents a set of values for the property.
    MatchProp(V, PropertyId, SetOperator, V),
    /// An attribute having a specific value, bound to a Term
    Attr(V, PropertyId, (Term, Term)),
    /// An element in the set defined by the variable
    Element(V, (Term, Term)),
    Eq(Var, Term),
    Or(ThinVec<Term>),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum CondTerm {
    Wildcard,
    Var(Var),
    Value(Value),
}

/// An binary operator that takes a set as its right hand operand
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum SetOperator {
    /// The left operand is an element of the right operand
    ElementIn,
    AllInSet,
    SetContainsAll,
    SetIntersects,
    SetEquals,
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

impl<V, Term> Display for Clause<V, Term>
where
    V: Display,
    Term: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Clause::Root(var) => write!(f, "(root {var})"),
            Clause::IsEntity(term, def_id) => write!(f, "(is-entity {term} {def_id:?})"),
            Clause::Attr(var, prop_id, (rel, val)) => {
                write!(f, "(attr {var} {prop_id} ({rel} {val}))")
            }
            Clause::MatchProp(var, prop_id, operator, term) => {
                write!(
                    f,
                    "(attr-set-predicate {var} {prop_id} ({operator} {term}))"
                )
            }
            Clause::Element(var, (rel, val)) => {
                write!(f, "(element {var} ({rel} {val}))")
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

impl Display for SetOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOperator::ElementIn => write!(f, "element-in"),
            SetOperator::AllInSet => write!(f, "all-in-set"),
            SetOperator::SetContainsAll => write!(f, "set-contains-all"),
            SetOperator::SetIntersects => write!(f, "set-intersects"),
            SetOperator::SetEquals => write!(f, "set-equals"),
        }
    }
}

impl Debug for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl<V, Term> Debug for Clause<V, Term>
where
    V: Display,
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
