use std::{fmt::Debug, marker::PhantomData};

use fnv::FnvHashMap;
use tracing::debug;

use crate::{
    mem::Intern,
    pattern::PatId,
    types::{Type, TypeCtx, TypeRef},
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
    type Value = InferValue<'m>;

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

pub type KnownType<'m> = (TypeRef<'m>, Strength);

/// If a weak type is unified with a strong type and they are
/// Repr-compatible, choose the strong type
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub enum Strength {
    Weak,
    Strong,
}

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum InferValue<'m> {
    Known(KnownType<'m>),
    Unknown,
}

impl<'m> ena::unify::UnifyValue for InferValue<'m> {
    type Error = TypeError<'m>;

    fn unify_values(value_a: &Self, value_b: &Self) -> Result<Self, Self::Error> {
        match (value_a, value_b) {
            (Self::Known((type_a, a_strength)), Self::Known((type_b, b_strength))) => {
                if type_a == type_b {
                    let strongest = [*a_strength, *b_strength].into_iter().max().unwrap();
                    Ok(Self::Known((type_a, strongest)))
                } else {
                    Err(TypeError::Mismatch(TypeEquation {
                        actual: (type_a, *a_strength),
                        expected: (type_b, *b_strength),
                    }))
                }
            }
            (Self::Known(..), Self::Unknown) => Ok(*value_a),
            (Self::Unknown, Self::Known(..)) => Ok(*value_b),
            (Self::Unknown, Self::Unknown) => Ok(Self::Unknown),
        }
    }
}

pub struct Inference<'m> {
    variables: FnvHashMap<PatId, Vec<TypeVar<'m>>>,
    pub(super) eq_relations: ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
}

impl<'m> Inference<'m> {
    pub fn new() -> Self {
        Self {
            variables: Default::default(),
            eq_relations: Default::default(),
        }
    }

    pub fn new_type_variable(&mut self, variable_id: PatId) -> TypeVar<'m> {
        let type_var = self.eq_relations.new_key(InferValue::Unknown);
        let var_equations = self.variables.entry(variable_id).or_default();

        for var in var_equations.iter() {
            self.eq_relations.unify_var_var(*var, type_var).unwrap();
        }
        var_equations.push(type_var);

        type_var
    }
}

pub struct Infer<'c, 'm> {
    pub types: &'c mut TypeCtx<'m>,
    pub eq_relations: &'c mut ena::unify::InPlaceUnificationTable<TypeVar<'m>>,
}

impl<'c, 'm> Infer<'c, 'm> {
    pub fn infer_recursive(&mut self, ty: TypeRef<'m>) -> Result<TypeRef<'m>, TypeError<'m>> {
        match ty {
            Type::Error => Err(TypeError::Propagated),
            Type::Infer(var) => match self.eq_relations.probe_value(*var) {
                InferValue::Known((ty, _)) => {
                    debug!("{var:?} inferred to: {ty:?}");
                    Ok(ty)
                }
                InferValue::Unknown => Err(TypeError::NotEnoughInformation),
            },
            Type::Seq(rel, val) => {
                let rel = self.infer_recursive(rel)?;
                let val = self.infer_recursive(val)?;

                Ok(self.types.intern(Type::Seq(rel, val)))
            }
            Type::Option(item) => self
                .infer_recursive(item)
                .map(|item| self.types.intern(Type::Option(item))),
            ty => Ok(ty),
        }
    }
}

#[test]
fn strength_cmp() {
    let strongest = [Strength::Strong, Strength::Weak]
        .into_iter()
        .max()
        .unwrap();
    assert_eq!(strongest, Strength::Strong);
}
