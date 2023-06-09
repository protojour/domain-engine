use ontol_hir::kind::{Attribute, Optional};
use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};

use crate::{hir_unify::VarSet, typed_hir::Meta};

#[derive(Debug)]
pub struct Expr<'m> {
    pub kind: Kind<'m>,
    pub meta: Meta<'m>,
    pub free_vars: VarSet,
}

#[derive(Debug)]
pub enum Kind<'m> {
    Var(ontol_hir::Var),
    Unit,
    Struct(Struct<'m>),
    Prop(Box<Prop<'m>>),
    Call(Call<'m>),
    Int(i64),
}

#[derive(Debug)]
pub struct Struct<'m>(pub ontol_hir::Binder, pub Vec<Prop<'m>>);

#[derive(Debug)]
pub struct Prop<'m> {
    pub optional: Optional,
    pub struct_var: ontol_hir::Var,
    pub prop_id: PropertyId,
    pub attr: Attribute<Expr<'m>>,
    pub free_vars: VarSet,
}

#[derive(Debug)]
pub struct Call<'m>(pub BuiltinProc, pub Vec<Expr<'m>>);
