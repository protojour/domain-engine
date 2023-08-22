use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc};
use smartstring::alias::String;

use crate::{
    hir_unify::VarSet,
    typed_hir::{self, TypedBinder},
};

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
    I64(i64),
    F64(f64),
    String(String),
    Seq(ontol_hir::Label, Box<ontol_hir::Attribute<Expr<'m>>>),
    Push(ontol_hir::Var, Box<ontol_hir::Attribute<Expr<'m>>>),
}

impl<'m> Kind<'m> {
    #[allow(unused)]
    pub fn debug_short(&self) -> std::string::String {
        match self {
            Self::Var(var) => format!("Var({var})"),
            Self::Unit => "Unit".to_string(),
            Self::Struct(struct_) => format!("Struct({})", struct_.0.var),
            Self::Prop(prop) => format!(
                "Prop({}{})",
                if prop.seq.is_some() { "seq " } else { "" },
                prop.prop_id
            ),
            Self::Map(_) => "Map".to_string(),
            Self::Call(_) => "Call".to_string(),
            Self::I64(int) => format!("i64({int})"),
            Self::F64(float) => format!("f64({float})"),
            Self::String(string) => format!("String({string})"),
            Self::Seq(label, _) => format!("Seq({label})"),
            Self::Push(var, _) => format!("Push({var})"),
        }
    }
}

#[derive(Debug)]
pub struct Struct<'m>(pub TypedBinder<'m>, pub Vec<Prop<'m>>);

#[derive(Debug)]
pub struct Prop<'m> {
    pub optional: ontol_hir::Optional,
    pub struct_var: ontol_hir::Var,
    pub prop_id: PropertyId,
    pub seq: Option<ontol_hir::Label>,
    pub attr: ontol_hir::Attribute<Expr<'m>>,
    pub free_vars: VarSet,
}

#[derive(Debug)]
pub struct Call<'m>(pub BuiltinProc, pub Vec<Expr<'m>>);

impl<'m> super::dep_tree::Expression for Expr<'m> {
    fn free_vars(&self) -> &VarSet {
        &self.1.free_vars
    }

    fn is_optional(&self) -> bool {
        false
    }

    fn is_seq(&self) -> bool {
        matches!(&self.0, Kind::Seq(..))
    }
}

impl<'m> super::dep_tree::Expression for Prop<'m> {
    fn free_vars(&self) -> &VarSet {
        &self.free_vars
    }

    fn is_optional(&self) -> bool {
        self.optional.0
    }

    fn is_seq(&self) -> bool {
        self.seq.is_some()
    }
}
