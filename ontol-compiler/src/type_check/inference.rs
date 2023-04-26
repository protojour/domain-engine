use std::{fmt::Debug, marker::PhantomData};

use fnv::FnvHashMap;

use crate::{
    expr::ExprId,
    mem::Intern,
    types::{Type, TypeRef, Types},
};

use super::{TypeEquation, TypeError};

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
                    Err(TypeError::Mismatch(TypeEquation {
                        actual: a,
                        expected: b,
                    }))
                }
            }
            (Self::Known(ty), Self::Unknown) => {
                accept_unification(ty)?;
                Ok(value1.clone())
            }
            (Self::Unknown, Self::Known(ty)) => {
                accept_unification(ty)?;
                Ok(value2.clone())
            }
            (Self::Unknown, Self::Unknown) => Ok(Self::Unknown),
        }
    }
}

fn accept_unification<'m>(ty: &Type<'m>) -> Result<(), TypeError<'m>> {
    match ty {
        // FIXME: A variable should not be inferred to array, that must be explicit
        // Type::Array(_) => Err(TypeError::MustBeSequence),
        _ => Ok(()),
    }
}

pub struct Inference<'m> {
    variables: FnvHashMap<ExprId, Vec<TypeVar<'m>>>,
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

pub struct Infer<'c, 'm> {
    pub types: &'c mut Types<'m>,
    pub eq_relations: &'c mut ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
}

impl<'c, 'm> Infer<'c, 'm> {
    pub fn infer_recursive(&mut self, ty: TypeRef<'m>) -> Result<TypeRef<'m>, TypeError<'m>> {
        match ty {
            Type::Error => Err(TypeError::Propagated),
            Type::Infer(var) => match self.eq_relations.probe_value(*var) {
                UnifyValue::Known(ty) => Ok(ty),
                UnifyValue::Unknown => Err(TypeError::NotEnoughInformation),
            },
            Type::Array(elem) => self
                .infer_recursive(elem)
                .map(|elem| self.types.intern(Type::Array(elem))),
            Type::Option(item) => self
                .infer_recursive(item)
                .map(|item| self.types.intern(Type::Option(item))),
            ty => Ok(ty),
        }
    }
}
