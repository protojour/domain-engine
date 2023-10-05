use std::fmt::{Debug, Display};

use ontol_hir::{CaptureMatchArm, PropVariant, SeqPropertyVariant};

use crate::{
    types::{TypeRef, ERROR_TYPE, UNIT_TYPE},
    SourceSpan, NO_SPAN,
};

/// An ontol_hir language "dialect" with type information and source spans.
#[derive(Clone, Copy)]
pub struct TypedHir;

impl ontol_hir::Lang for TypedHir {
    type Data<'m, H> = TypedHirData<'m, H> where H: Clone;

    fn default_data<'m, H: Clone>(&self, hir: H) -> Self::Data<'m, H> {
        TypedHirData(hir, ERROR_META)
    }

    fn as_hir<'m, T: Clone>(data: &'m Self::Data<'_, T>) -> &'m T {
        data.hir()
    }
}

pub type TypedHirKind<'m> = ontol_hir::Kind<'m, TypedHir>;

pub type TypedArena<'m> = ontol_hir::arena::Arena<'m, TypedHir>;
pub type TypedNodeRef<'h, 'm> = ontol_hir::arena::NodeRef<'h, 'm, TypedHir>;
pub type TypedRootNode<'m> = ontol_hir::RootNode<'m, TypedHir>;

/// Data structure for associating ontol-hir data with the compiler's metadata attached to each hir node.
#[derive(Clone, Copy)]
pub struct TypedHirData<'m, H>(pub H, pub Meta<'m>);

impl<'m, H> TypedHirData<'m, H> {
    pub fn split(self) -> (H, Meta<'m>) {
        (self.0, self.1)
    }

    /// Access the ontol-hir part of data
    pub fn hir(&self) -> &H {
        &self.0
    }

    pub fn hir_mut(&mut self) -> &mut H {
        &mut self.0
    }

    pub fn into_hir(self) -> H {
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

impl<'m, T: Debug> std::fmt::Debug for TypedHirData<'m, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut tup = f.debug_tuple("Data");
        tup.field(&self.0);
        tup.field(&self.meta().ty);
        tup.finish()
    }
}

pub trait IntoTypedHirData<'m>: Sized {
    fn with_meta(self, meta: Meta<'m>) -> TypedHirData<'m, Self>;
    fn with_ty(self, ty: TypeRef<'m>) -> TypedHirData<'m, Self>;
}

impl<'m, T> IntoTypedHirData<'m> for T {
    fn with_meta(self, meta: Meta<'m>) -> TypedHirData<'m, Self> {
        TypedHirData(self, meta)
    }

    fn with_ty(self, ty: TypeRef<'m>) -> TypedHirData<'m, Self> {
        TypedHirData(self, Meta { ty, span: NO_SPAN })
    }
}

/// The compiler's metadata for each interesting bit in ontol-hir
#[derive(Clone, Copy, Debug)]
pub struct Meta<'m> {
    pub ty: TypeRef<'m>,
    pub span: SourceSpan,
}

pub static UNIT_META: Meta<'static> = Meta {
    ty: &UNIT_TYPE,
    span: NO_SPAN,
};
pub static ERROR_META: Meta<'static> = Meta {
    ty: &ERROR_TYPE,
    span: NO_SPAN,
};

pub struct HirFunc<'m> {
    pub arg: TypedHirData<'m, ontol_hir::Binder>,
    pub body: ontol_hir::RootNode<'m, TypedHir>,
}

impl<'m> Display for HirFunc<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|{}| {}", self.arg.0.var, self.body)
    }
}

pub fn arena_import_root<'h, 'm>(
    source: TypedNodeRef<'h, 'm>,
) -> ontol_hir::RootNode<'m, TypedHir> {
    let mut target: TypedArena<'m> = Default::default();
    let node = arena_import(&mut target, source);
    ontol_hir::RootNode::new(node, target)
}

// TODO: Generalize this in ontol-hir
pub fn arena_import<'m>(
    target: &mut TypedArena<'m>,
    source: TypedNodeRef<'_, 'm>,
) -> ontol_hir::Node {
    let value = &source.arena()[source.node()];
    let (kind, meta) = (&value.0, &value.1);

    fn import_nodes<'m>(
        target: &mut TypedArena<'m>,
        source: &TypedArena<'m>,
        nodes: &[ontol_hir::Node],
    ) -> ontol_hir::Nodes {
        let mut imported_nodes = ontol_hir::Nodes::default();
        for node in nodes {
            imported_nodes.push(arena_import(target, source.node_ref(*node)));
        }
        imported_nodes
    }

    fn import_attr<'m>(
        target: &mut TypedArena<'m>,
        source: &TypedArena<'m>,
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
            target.add(TypedHirData(kind.clone(), *meta))
        }
        Let(binder, def, body) => {
            let def = arena_import(target, source.arena().node_ref(*def));
            let body = import_nodes(target, source.arena(), body);

            target.add(TypedHirData(Let(*binder, def, body), *meta))
        }
        Call(proc, args) => {
            let args = import_nodes(target, source.arena(), args);
            target.add(TypedHirData(Call(*proc, args), *meta))
        }
        Map(arg) => {
            let arg = arena_import(target, source.arena().node_ref(*arg));
            target.add(TypedHirData(Map(arg), *meta))
        }
        DeclSeq(label, attr) => {
            let attr = import_attr(target, source.arena(), *attr);
            target.add(TypedHirData(DeclSeq(*label, attr), *meta))
        }
        Struct(binder, flags, body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirData(Struct(*binder, *flags, body), *meta))
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
            target.add(TypedHirData(
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
            target.add(TypedHirData(MatchProp(*struct_var, *prop_id, arms), *meta))
        }
        Sequence(binder, body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirData(Sequence(*binder, body), *meta))
        }
        ForEach(var, (rel, val), body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirData(ForEach(*var, (*rel, *val), body), *meta))
        }
        SeqPush(var, attr) => {
            let attr = import_attr(target, source.arena(), *attr);
            target.add(TypedHirData(SeqPush(*var, attr), *meta))
        }
        StringPush(var, node) => {
            let node = arena_import(target, source.arena().node_ref(*node));
            target.add(TypedHirData(StringPush(*var, node), *meta))
        }
        Regex(label, def_id, groups_list) => target.add(TypedHirData(
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
            target.add(TypedHirData(MatchRegex(*iter, *var, *def_id, arms), *meta))
        }
        PushCondClause(var, clause) => {
            target.add(TypedHirData(PushCondClause(*var, clause.clone()), *meta))
        }
    }
}
