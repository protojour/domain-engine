use ontol_hir::{Node, StructFlags};
use ontol_runtime::{
    smart_format,
    value::PropertyId,
    var::{Var, VarSet},
    vm::proc::BuiltinProc,
    DefId,
};
use smartstring::alias::String;

use crate::{
    typed_hir::{self, TypedHirData},
    SourceSpan,
};

use super::flat_scope::OutputVar;

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
    Var(Var),
    Unit,
    Struct {
        binder: TypedHirData<'m, ontol_hir::Binder>,
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
    DestructuredSeq(ontol_hir::Label, OutputVar),
    SeqItem(
        ontol_hir::Label,
        usize,
        ontol_hir::Iter,
        Box<ontol_hir::Attribute<Expr<'m>>>,
    ),
    Push(Var, Box<ontol_hir::Attribute<Expr<'m>>>),
    StringInterpolation(
        TypedHirData<'m, ontol_hir::Binder>,
        Vec<StringInterpolationComponent>,
    ),
    /// Temporarily wrap an ontol_hir::Node
    /// FIXME: This is a temporary hack for flat_unifier for dividing scoping into outside-loop and inside-loop
    HirNode(Node),
}

impl<'m> Kind<'m> {
    #[allow(unused)]
    pub fn debug_short(&self) -> std::string::String {
        match self {
            Self::Var(var) => format!("Var({var})"),
            Self::Unit => "Unit".to_string(),
            Self::Struct { binder, flags, .. } => {
                if flags.contains(StructFlags::MATCH) {
                    format!("MatchStruct({})", binder.hir().var)
                } else {
                    format!("Struct({})", binder.hir().var)
                }
            }
            Self::Prop(prop) => format!(
                "Prop{}({}{}[{}])",
                if prop.optional.0 { "?" } else { "" },
                if let Some(label) = prop.seq {
                    smart_format!("seq({label}) ")
                } else {
                    smart_format!("")
                },
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
            Self::DestructuredSeq(label, output_var) => {
                format!("DestructuredSeq({label}, {output_var:?})")
            }
            Self::SeqItem(label, index, iter, attr) => format!(
                "{}SeqItem({label}, {index}, ({}, {}))",
                if iter.0 { "Iter" } else { "" },
                attr.rel.kind().debug_short(),
                attr.val.kind().debug_short()
            ),
            Self::Push(var, _) => format!("Push({var})"),
            Self::StringInterpolation(binder, _) => {
                format!("StringInterpolation({})", binder.hir().var)
            }
            Self::HirNode(_) => "Node".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct Struct<'m>(
    pub TypedHirData<'m, ontol_hir::Binder>,
    pub ontol_hir::StructFlags,
    pub Vec<Prop<'m>>,
);

#[derive(Debug)]
pub struct Prop<'m> {
    pub optional: ontol_hir::Optional,
    pub struct_var: Var,
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
        elements: Vec<(ontol_hir::Iter, ontol_hir::Attribute<Expr<'m>>)>,
    },
}

#[derive(Debug)]
pub enum StringInterpolationComponent {
    Const(String),
    Var(Var, SourceSpan),
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

pub fn collect_free_vars(expr: &Expr) -> VarSet {
    let mut visitor = FreeVarVisitor::default();
    visitor.visit(expr);
    visitor.free_vars
}

#[derive(Default)]
struct FreeVarVisitor {
    pub free_vars: VarSet,
}

impl FreeVarVisitor {
    pub fn visit(&mut self, expr: &Expr) {
        match expr.kind() {
            Kind::Var(var) => {
                self.free_vars.insert(*var);
            }
            Kind::Unit | Kind::I64(_) | Kind::F64(_) | Kind::String(_) | Kind::Const(_) => {}
            Kind::Struct { props, .. } => {
                for prop in props {
                    self.visit_prop_variant(&prop.variant);
                }
            }
            Kind::Prop(prop) => {
                self.visit_prop_variant(&prop.variant);
            }
            Kind::Map(inner) => {
                self.visit(inner);
            }
            Kind::Call(call) => {
                for arg in &call.1 {
                    self.visit(arg);
                }
            }
            Kind::Seq(_, attr) => {
                self.visit_attr(attr);
            }
            Kind::DestructuredSeq(..) => {}
            Kind::SeqItem(_, _, _, attr) => self.visit_attr(attr),
            Kind::Push(_, attr) => {
                self.visit_attr(attr);
            }
            Kind::StringInterpolation(_, components) => {
                for component in components {
                    match component {
                        StringInterpolationComponent::Var(var, _) => {
                            self.free_vars.insert(*var);
                        }
                        StringInterpolationComponent::Const(_) => {}
                    }
                }
            }
            Kind::HirNode(_) => {}
        }
    }

    fn visit_prop_variant(&mut self, variant: &PropVariant) {
        match variant {
            PropVariant::Singleton(attr) => {
                self.visit_attr(attr);
            }
            PropVariant::Seq { elements, .. } => {
                for (_iter, attr) in elements {
                    self.visit_attr(attr);
                }
            }
        }
    }

    fn visit_attr(&mut self, attr: &ontol_hir::Attribute<Expr>) {
        self.visit(&attr.rel);
        self.visit(&attr.val);
    }
}
