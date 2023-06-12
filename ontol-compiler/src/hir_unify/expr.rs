use ontol_hir::kind::{Attribute, Optional};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};

use crate::{hir_unify::VarSet, typed_hir};

#[derive(Debug)]
pub struct Expr<'m>(pub Kind<'m>, pub Meta<'m>);

impl<'m> Expr<'m> {
    #[inline]
    pub fn meta(&self) -> &Meta<'m> {
        &self.1
    }

    #[inline]
    pub fn hir_meta(&self) -> &typed_hir::Meta<'m> {
        &self.1.hir_meta
    }
}

#[derive(Clone, Debug)]
pub struct Meta<'m> {
    pub free_vars: VarSet,
    pub hir_meta: typed_hir::Meta<'m>,
}

#[derive(Debug)]
pub enum Kind<'m> {
    Var(ontol_hir::Var),
    Unit,
    Struct(Struct<'m>),
    Prop(Box<Prop<'m>>),
    Map(Box<Expr<'m>>),
    Call(Call<'m>),
    Int(i64),
    Seq(ontol_hir::Label, Box<Attribute<Expr<'m>>>),
    Push(ontol_hir::Var, Box<Attribute<Expr<'m>>>),
}

impl<'m> Kind<'m> {
    #[allow(unused)]
    pub fn debug_short(&self) -> String {
        match self {
            Self::Var(var) => format!("Var({var})"),
            Self::Unit => "Unit".to_string(),
            Self::Struct(_) => "Struct".to_string(),
            Self::Prop(_) => "Prop".to_string(),
            Self::Map(_) => "Map".to_string(),
            Self::Call(_) => "Call".to_string(),
            Self::Int(int) => format!("Int({int})"),
            Self::Seq(label, _) => format!("Seq({label})"),
            Self::Push(var, _) => format!("Push({var})"),
        }
    }
}

#[derive(Debug)]
pub struct Struct<'m>(pub ontol_hir::Binder, pub Vec<Prop<'m>>);

#[derive(Debug)]
pub struct Prop<'m> {
    pub optional: Optional,
    pub struct_var: ontol_hir::Var,
    pub prop_id: PropertyId,
    pub seq: Option<ontol_hir::Label>,
    pub attr: Attribute<Expr<'m>>,
    pub free_vars: VarSet,
}

#[derive(Debug)]
pub struct Call<'m>(pub BuiltinProc, pub Vec<Expr<'m>>);

impl<'m> super::dep_tree::Expression for Expr<'m> {
    fn free_vars(&self) -> &VarSet {
        &self.1.free_vars
    }

    fn optional(&self) -> bool {
        false
    }
}

impl<'m> super::dep_tree::Expression for Prop<'m> {
    fn free_vars(&self) -> &VarSet {
        &self.free_vars
    }

    fn optional(&self) -> bool {
        self.optional.0
    }
}
