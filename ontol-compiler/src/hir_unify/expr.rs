use ontol_hir::kind::{Attribute, Optional};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};

use crate::{hir_unify::VarSet, typed_hir::Meta};

#[derive(Debug)]
pub struct Expr<'m> {
    pub kind: Kind<'m>,
    pub meta: Meta<'m>,
    pub free_vars: VarSet,
}

impl<'m> Expr<'m> {
    #[allow(unused)]
    pub fn debug_short(&self) -> String {
        match &self.kind {
            Kind::Var(var) => format!("Var({var})"),
            Kind::Unit => "Unit".to_string(),
            Kind::Struct(_) => "Struct".to_string(),
            Kind::Prop(_) => "Prop".to_string(),
            Kind::Map(_) => "Map".to_string(),
            Kind::Call(_) => "Call".to_string(),
            Kind::Int(int) => format!("Int({int})"),
            Kind::Seq(label, _) => format!("Seq({label})"),
            Kind::Push(var, _) => format!("Push({var})"),
        }
    }
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

impl<'m> super::hierarchy::Expression for Expr<'m> {
    fn free_vars(&self) -> &VarSet {
        &self.free_vars
    }

    fn optional(&self) -> bool {
        false
    }
}

impl<'m> super::hierarchy::Expression for Prop<'m> {
    fn free_vars(&self) -> &VarSet {
        &self.free_vars
    }

    fn optional(&self) -> bool {
        self.optional.0
    }
}
