use crate::{value::PropertyId, DefId};

pub struct Condition {
    pub binder: UniVar,
    pub clauses: Vec<Clause>,
}

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug, Hash)]
pub struct UniVar(u32);

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
    IsEntity(CondTerm, DefId),
    Prop(UniVar, PropertyId, (CondTerm, CondTerm)),
    ElementIn(UniVar, CondTerm),
    Or(Vec<Clause>),
}

pub enum CondTerm {
    Wildcard,
    Var(UniVar),
    Text(Box<str>),
    I64(i64),
    F64(f64),
}

#[cfg(test)]
mod tests {
    use crate::value::{Data, Value};

    #[test]
    fn struct_as_filter() {
        let lol = Data::Struct(
            [(
                "O:0:0".parse().unwrap(),
                Value::new(Data::I64(42), crate::DefId::unit()).into(),
            )]
            .into(),
        );
    }
}
