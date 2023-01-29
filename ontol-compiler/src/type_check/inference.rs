use std::{collections::HashMap, fmt::Debug, marker::PhantomData};

use crate::{error::CompileError, expr::ExprId, types::TypeRef};

use super::TypeError;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct TypeVar<'m>(pub u32, PhantomData<&'m ()>);

impl<'m> Debug for TypeVar<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TypeVar").field(&self.0).finish()
    }
}

impl<'m> ena::unify::UnifyKey for TypeVar<'m> {
    type Value = UnifyValue<'m>;

    fn index(&self) -> u32 {
        self.0
    }

    fn from_index(i: u32) -> Self {
        TypeVar(i, PhantomData)
    }

    fn tag() -> &'static str {
        "TypeVar"
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum UnifyValue<'m> {
    Known(TypeRef<'m>),
    Unknown,
}

impl<'m> ena::unify::UnifyValue for UnifyValue<'m> {
    type Error = TypeError<'m>;

    fn unify_values(value1: &Self, value2: &Self) -> Result<Self, Self::Error> {
        match (value1, value2) {
            (Self::Known(a), Self::Known(b)) => {
                if a == b {
                    Ok(value1.clone())
                } else {
                    Err(TypeError::Mismatch {
                        actual: a,
                        expected: b,
                    })
                }
            }
            (Self::Known(_), Self::Unknown) => Ok(value1.clone()),
            (Self::Unknown, Self::Known(_)) => Ok(value2.clone()),
            (Self::Unknown, Self::Unknown) => Ok(Self::Unknown),
        }
    }
}

pub struct Inference<'m> {
    variables: HashMap<ExprId, Vec<TypeVar<'m>>>,
    pub(super) eq_relations: ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
}

impl<'m> Inference<'m> {
    pub fn new() -> Self {
        Self {
            variables: Default::default(),
            eq_relations: Default::default(),
        }
    }

    pub fn new_type_variable(&mut self, variable_id: ExprId) -> TypeVar<'m> {
        let type_var = self.eq_relations.new_key(UnifyValue::Unknown);
        let var_equations = self.variables.entry(variable_id).or_default();

        for var in var_equations.iter() {
            self.eq_relations.unify_var_var(*var, type_var).unwrap();
        }
        var_equations.push(type_var);

        type_var
    }
}
