use ontol_runtime::{value::PropertyId, vm::proc::BuiltinProc, DefId};
use smartstring::alias::String;

use crate::{
    hir_unify::VarSet,
    typed_hir::{self, TypedBinder},
    SourceSpan,
};

#[derive(Debug)]
pub struct Expr<'m>(pub Kind<'m>, pub Meta<'m>);

impl<'m> Expr<'m> {
    #[inline]
    pub fn kind(&self) -> &Kind<'m> {
        &self.0
    }

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
    Struct {
        binder: TypedBinder<'m>,
        flags: ontol_hir::StructFlags,
        props: Vec<Prop<'m>>,
    },
    Prop(Box<Prop<'m>>),
    Map(Box<Expr<'m>>),
    Call(Call<'m>),
    I64(i64),
    F64(f64),
    String(String),
    Const(DefId),
    Seq(ontol_hir::Label, Box<ontol_hir::Attribute<Expr<'m>>>),
    SeqItem(
        ontol_hir::Label,
        usize,
        Iter,
        Box<ontol_hir::Attribute<Expr<'m>>>,
    ),
    Push(ontol_hir::Var, Box<ontol_hir::Attribute<Expr<'m>>>),
    StringInterpolation(TypedBinder<'m>, Vec<StringInterpolationComponent>),
}

impl<'m> Kind<'m> {
    #[allow(unused)]
    pub fn debug_short(&self) -> std::string::String {
        match self {
            Self::Var(var) => format!("Var({var})"),
            Self::Unit => "Unit".to_string(),
            Self::Struct { binder, .. } => format!("Struct({})", binder.var),
            Self::Prop(prop) => format!(
                "Prop{}({}{}[{}])",
                if prop.optional.0 { "?" } else { "" },
                if prop.seq.is_some() { "seq " } else { "" },
                prop.struct_var,
                prop.prop_id
            ),
            Self::Map(_) => "Map".to_string(),
            Self::Call(_) => "Call".to_string(),
            Self::I64(int) => format!("i64({int})"),
            Self::F64(float) => format!("f64({float})"),
            Self::String(string) => format!("String({string})"),
            Self::Const(const_def_id) => format!("Const({const_def_id:?})"),
            Self::Seq(label, _) => format!("Seq({label})"),
            Self::SeqItem(label, index, _iter, attr) => format!(
                "SeqItem({label}, {index}, ({}, {}))",
                attr.rel.kind().debug_short(),
                attr.val.kind().debug_short()
            ),
            Self::Push(var, _) => format!("Push({var})"),
            Self::StringInterpolation(binder, _) => format!("StringInterpolation({})", binder.var),
        }
    }
}

#[derive(Debug)]
pub struct Struct<'m>(
    pub TypedBinder<'m>,
    pub ontol_hir::StructFlags,
    pub Vec<Prop<'m>>,
);

#[derive(Debug)]
pub struct Prop<'m> {
    pub optional: ontol_hir::Optional,
    pub struct_var: ontol_hir::Var,
    pub prop_id: PropertyId,
    pub seq: Option<ontol_hir::Label>,
    // pub attr: ontol_hir::Attribute<Expr<'m>>,
    pub variant: PropVariant<'m>,
    pub free_vars: VarSet,
}

#[derive(Debug)]
pub enum PropVariant<'m> {
    Singleton(ontol_hir::Attribute<Expr<'m>>),
    Seq {
        label: ontol_hir::Label,
        elements: Vec<SeqPropElement<'m>>,
    },
}

#[derive(Debug)]
pub struct SeqPropElement<'m> {
    pub iter: bool,
    pub attribute: ontol_hir::Attribute<Expr<'m>>,
}

#[derive(Debug)]
pub struct Iter(pub bool);

#[derive(Debug)]
pub enum StringInterpolationComponent {
    Const(String),
    Var(ontol_hir::Var, SourceSpan),
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
