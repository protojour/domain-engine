use ontol_runtime::{
    condition::{Clause, CondTerm, Condition},
    var::Var,
};

#[derive(Clone, Copy)]
pub struct ConditionWalker<'a> {
    condition: &'a Condition,
}

impl<'a> ConditionWalker<'a> {
    pub fn new(condition: &'a Condition) -> Self {
        Self { condition }
    }

    pub fn start(&self) -> impl Iterator<Item = &Clause<Var, CondTerm>> {
        self.clauses(Var(0))
    }

    pub fn clauses(&self, var: Var) -> impl Iterator<Item = &Clause<Var, CondTerm>> {
        self.condition
            .expansions()
            .get(&var)
            .into_iter()
            .flat_map(|expansion| expansion.clauses())
    }

    pub fn set_members(&self, set_var: Var) -> Members {
        match self.condition.expansions().get(&set_var) {
            Some(expansion) => {
                if expansion.incoming_count() > 1 {
                    Members::Join(set_var)
                } else {
                    Members::Members(SetMembers(expansion.clauses()))
                }
            }
            None => Members::Empty,
        }
    }
}

pub enum Members<'a> {
    Members(SetMembers<'a>),
    Empty,
    Join(Var),
}

pub struct SetMembers<'a>(&'a [Clause<Var, CondTerm>]);

impl<'a> SetMembers<'a> {
    pub fn size(&self) -> usize {
        self.iter().count()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&CondTerm, &CondTerm)> {
        self.0.iter().filter_map(|clause| match clause {
            Clause::Member(rel, val) => Some((rel, val)),
            _ => None,
        })
    }
}
