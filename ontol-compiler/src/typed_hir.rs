use std::fmt::{Debug, Display};

use ontol_hir::{CaptureMatchArm, PropVariant, SeqPropertyVariant};
use ontol_runtime::DefId;

use crate::{
    primitive::PrimitiveKind,
    types::{Type, TypeRef},
    SourceSpan, NO_SPAN,
};

/// An ontol_hir language "dialect" with type information and source spans.
#[derive(Clone, Copy)]
pub struct TypedHir;

impl ontol_hir::Lang for TypedHir {
    type Meta<'m, T> = TypedHirValue<'m, T> where T: Clone;

    fn default_meta<'a, T: Clone>(&self, value: T) -> Self::Meta<'a, T> {
        TypedHirValue(
            value,
            Meta {
                ty: &Type::Error,
                span: NO_SPAN,
            },
        )
    }

    fn inner<'m, 'a, T: Clone>(meta: &'m Self::Meta<'a, T>) -> &'m T {
        meta.value()
    }
}

pub type TypedHirKind<'m> = ontol_hir::Kind<'m, TypedHir>;

#[derive(Clone, Copy, Debug)]
pub struct TypedHirValue<'m, T>(pub T, pub Meta<'m>);

impl<'m, T> TypedHirValue<'m, T> {
    pub fn split(self) -> (T, Meta<'m>) {
        (self.0, self.1)
    }

    pub fn value(&self) -> &T {
        &self.0
    }

    pub fn value_mut(&mut self) -> &mut T {
        &mut self.0
    }

    pub fn into_value(self) -> T {
        self.0
    }

    pub fn meta(&self) -> &Meta<'m> {
        &self.1
    }

    pub fn meta_mut(&mut self) -> &mut Meta<'m> {
        &mut self.1
    }

    pub fn ty(&self) -> TypeRef<'m> {
        self.1.ty
    }

    pub fn span(&self) -> SourceSpan {
        self.1.span
    }
}

pub trait IntoTypedHirValue<'m>: Sized {
    fn with_meta(self, meta: Meta<'m>) -> TypedHirValue<'m, Self>;
    fn with_ty(self, ty: TypeRef<'m>) -> TypedHirValue<'m, Self>;
}

impl<'m, T> IntoTypedHirValue<'m> for T {
    fn with_meta(self, meta: Meta<'m>) -> TypedHirValue<'m, Self> {
        TypedHirValue(self, meta)
    }

    fn with_ty(self, ty: TypeRef<'m>) -> TypedHirValue<'m, Self> {
        TypedHirValue(self, Meta { ty, span: NO_SPAN })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Meta<'m> {
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

static UNIT_TY: Type = Type::Primitive(PrimitiveKind::Unit, DefId::unit());

pub static UNIT_META: Meta<'static> = Meta {
    ty: &UNIT_TY,
    span: NO_SPAN,
};

pub struct HirFunc<'m> {
    pub arg: TypedHirValue<'m, ontol_hir::Binder>,
    pub body: ontol_hir::RootNode<'m, TypedHir>,
}

impl<'m> Display for HirFunc<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|{}| {}", self.arg.0.var, self.body)
    }
}

pub fn arena_import_root<'h, 'm>(
    source: ontol_hir::arena::NodeRef<'h, 'm, TypedHir>,
) -> ontol_hir::RootNode<'m, TypedHir> {
    let mut target: ontol_hir::arena::Arena<'m, TypedHir> = Default::default();
    let node = arena_import(&mut target, source);
    ontol_hir::RootNode::new(node, target)
}

// TODO: Generalize this in ontol-hir
pub fn arena_import<'h, 'm>(
    target: &mut ontol_hir::arena::Arena<'m, TypedHir>,
    source: ontol_hir::arena::NodeRef<'h, 'm, TypedHir>,
) -> ontol_hir::Node {
    let value = &source.arena()[source.node()];
    let (kind, meta) = (&value.0, &value.1);

    fn import_nodes<'h, 'm>(
        target: &mut ontol_hir::arena::Arena<'m, TypedHir>,
        source: &ontol_hir::arena::Arena<'m, TypedHir>,
        nodes: &[ontol_hir::Node],
    ) -> ontol_hir::Nodes {
        let mut imported_nodes = ontol_hir::Nodes::default();
        for node in nodes {
            imported_nodes.push(arena_import(target, source.node_ref(*node)));
        }
        imported_nodes
    }

    fn import_attr<'h, 'm>(
        target: &mut ontol_hir::arena::Arena<'m, TypedHir>,
        source: &ontol_hir::arena::Arena<'m, TypedHir>,
        attr: ontol_hir::Attribute<ontol_hir::Node>,
    ) -> ontol_hir::Attribute<ontol_hir::Node> {
        let rel = arena_import(target, source.node_ref(attr.rel));
        let val = arena_import(target, source.node_ref(attr.val));
        ontol_hir::Attribute { rel, val }
    }

    use ontol_hir::Kind::*;

    match kind {
        Var(_) | Unit | I64(_) | F64(_) | Text(_) | Const(_) => {
            // No subnodes
            target.add(TypedHirValue(kind.clone(), *meta))
        }
        Let(binder, def, body) => {
            let def = arena_import(target, source.arena().node_ref(*def));
            let body = import_nodes(target, source.arena(), body);

            target.add(TypedHirValue(Let(*binder, def, body), *meta))
        }
        Call(proc, args) => {
            let args = import_nodes(target, source.arena(), args);
            target.add(TypedHirValue(Call(*proc, args), *meta))
        }
        Map(arg) => {
            let arg = arena_import(target, source.arena().node_ref(*arg));
            target.add(TypedHirValue(Map(arg), *meta))
        }
        DeclSeq(label, attr) => {
            let attr = import_attr(target, source.arena(), *attr);
            target.add(TypedHirValue(DeclSeq(*label, attr), *meta))
        }
        Struct(binder, flags, body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirValue(Struct(*binder, *flags, body), *meta))
        }
        Prop(optional, struct_var, prop_id, variants) => {
            let variants = variants
                .iter()
                .map(|variant| match variant {
                    PropVariant::Singleton(attr) => {
                        PropVariant::Singleton(import_attr(target, source.arena(), *attr))
                    }
                    PropVariant::Seq(seq_variant) => {
                        let elements = seq_variant
                            .elements
                            .iter()
                            .map(|(iter, attr)| (*iter, import_attr(target, source.arena(), *attr)))
                            .collect();
                        PropVariant::Seq(SeqPropertyVariant {
                            label: seq_variant.label,
                            has_default: seq_variant.has_default,
                            elements,
                        })
                    }
                })
                .collect();
            target.add(TypedHirValue(
                Prop(*optional, *struct_var, *prop_id, variants),
                *meta,
            ))
        }
        MatchProp(struct_var, prop_id, arms) => {
            let arms = arms
                .iter()
                .map(|(pattern, body)| {
                    let body = import_nodes(target, source.arena(), body);
                    (pattern.clone(), body)
                })
                .collect();
            target.add(TypedHirValue(MatchProp(*struct_var, *prop_id, arms), *meta))
        }
        Sequence(binder, body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirValue(Sequence(*binder, body), *meta))
        }
        ForEach(var, (rel, val), body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirValue(ForEach(*var, (*rel, *val), body), *meta))
        }
        SeqPush(var, attr) => {
            let attr = import_attr(target, source.arena(), *attr);
            target.add(TypedHirValue(SeqPush(*var, attr), *meta))
        }
        StringPush(var, node) => {
            let node = arena_import(target, source.arena().node_ref(*node));
            target.add(TypedHirValue(StringPush(*var, node), *meta))
        }
        Regex(label, def_id, groups_list) => target.add(TypedHirValue(
            Regex(*label, *def_id, groups_list.clone()),
            *meta,
        )),
        MatchRegex(iter, var, def_id, arms) => {
            let arms = arms
                .iter()
                .map(|arm| {
                    let nodes = import_nodes(target, source.arena(), &arm.nodes);
                    CaptureMatchArm {
                        capture_groups: arm.capture_groups.clone(),
                        nodes,
                    }
                })
                .collect();
            target.add(TypedHirValue(MatchRegex(*iter, *var, *def_id, arms), *meta))
        }
    }
}
