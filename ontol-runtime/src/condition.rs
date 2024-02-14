use std::fmt::{Debug, Display};

use fnv::FnvHashMap;
use serde::{Deserialize, Serialize};
use thin_vec::ThinVec;

use crate::{
    format_utils::Literal,
    value::{PropertyId, Value, ValueDebug},
    var::Var,
    DefId,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Condition {
    expansions: FnvHashMap<Var, Expansion>,
    next_cond_var: Var,
}

impl Condition {
    pub fn root_def_id(&self) -> Option<DefId> {
        let expansion = self.expansions.get(&Var(0))?;
        expansion.clauses.iter().find_map(|term| match term {
            Clause::IsEntity(def_id) => Some(*def_id),
            _ => None,
        })
    }

    pub fn mk_cond_var(&mut self) -> Var {
        let var = self.next_cond_var;
        self.next_cond_var.0 += 1;
        var
    }

    pub fn expansions(&self) -> &FnvHashMap<Var, Expansion> {
        &self.expansions
    }

    pub fn add_clause(&mut self, cond_var: Var, clause: Clause<Var, CondTerm>) {
        match &clause {
            Clause::IsEntity(_) => {}
            Clause::Member(rel, val) => {
                self.register_term(rel);
                self.register_term(val);
            }
            Clause::MatchProp(.., set_var) => {
                self.expansions.entry(*set_var).or_default().incoming_count += 1;
            }
            Clause::Root => {}
        }

        self.expansions
            .entry(cond_var)
            .or_default()
            .clauses
            .push(clause);
    }

    fn register_term(&mut self, term: &CondTerm) {
        match term {
            CondTerm::Wildcard => {}
            CondTerm::Variable(var) => {
                self.expansions.entry(*var).or_default().incoming_count += 1;
            }
            CondTerm::Value(_) => {}
        }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Expansion {
    incoming_count: usize,
    clauses: Vec<Clause<Var, CondTerm>>,
}

impl Expansion {
    pub fn incoming_count(&self) -> usize {
        self.incoming_count
    }

    pub fn clauses(&self) -> &[Clause<Var, CondTerm>] {
        &self.clauses
    }
}

impl From<ThinVec<ClausePair<Var, CondTerm>>> for Condition {
    fn from(value: ThinVec<ClausePair<Var, CondTerm>>) -> Self {
        let mut condition = Condition {
            expansions: Default::default(),
            next_cond_var: Var(0),
        };

        for ClausePair(var, clause) in value {
            condition.add_clause(var, clause);
        }

        condition
    }
}

impl Default for Condition {
    fn default() -> Self {
        Self {
            expansions: FnvHashMap::default(),
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
pub struct ClausePair<V, Term>(pub V, pub Clause<V, Term>);

#[derive(Clone, Serialize, Deserialize)]
pub enum Clause<V, Term> {
    Root,
    IsEntity(DefId),
    /// The left variable is connected via a property to the right variable.
    /// The right variable represents a set of values for the property.
    MatchProp(PropertyId, SetOperator, V),
    /// An element in the set defined by the variable
    Member(Term, Term),
}

#[derive(Clone, Serialize, Deserialize)]
pub enum CondTerm {
    Wildcard,
    Variable(Var),
    Value(Value),
}

/// An binary operator that takes a set as its right hand operand
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub enum SetOperator {
    /// The left operand is an element of the right operand
    ElementIn,
    /// The left operand is a subset of the right operand
    SubsetOf,
    /// The left operand is a superset of the right operand
    SupersetOf,
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
        for v in 0..self.next_cond_var.0 {
            let var = Var(v);

            if let Some(expansion) = self.expansions.get(&var) {
                for clause in &expansion.clauses {
                    writeln!(f, "{}", ClausePair(var, clause.clone()))?;
                }
            }
        }

        Ok(())
    }
}

impl<V, Term> Display for ClausePair<V, Term>
where
    V: Display,
    Term: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let var = &self.0;
        match &self.1 {
            Clause::Root => write!(f, "(root {var})"),
            Clause::IsEntity(def_id) => write!(f, "(is-entity {var} {def_id:?})"),
            Clause::MatchProp(prop_id, operator, term) => {
                write!(f, "(match-prop {var} {prop_id} ({operator} {term}))")
            }
            Clause::Member(rel, val) => {
                write!(f, "(member {var} ({rel} {val}))")
            }
        }
    }
}

impl Display for CondTerm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Wildcard => write!(f, "_"),
            Self::Variable(var) => write!(f, "{var}"),
            Self::Value(value) => write!(f, "{}", ValueDebug(value)),
        }
    }
}

impl Display for SetOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetOperator::ElementIn => write!(f, "element-in"),
            SetOperator::SubsetOf => write!(f, "subset-of"),
            SetOperator::SupersetOf => write!(f, "superset-of"),
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

impl<V, Term> Debug for ClausePair<V, Term>
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
