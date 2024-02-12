use std::fmt::{Debug, Display};

use ontol_hir::{PropVariant, SetEntry};
use ontol_runtime::value::Attribute;
use smallvec::SmallVec;

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

impl<'m> Meta<'m> {
    pub const fn new(ty: TypeRef<'m>, span: SourceSpan) -> Self {
        Self { ty, span }
    }

    pub fn unit(span: SourceSpan) -> Self {
        Self {
            ty: &UNIT_TYPE,
            span,
        }
    }

    pub fn error(span: SourceSpan) -> Self {
        Self {
            ty: &ERROR_TYPE,
            span,
        }
    }
}

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
        attr: Attribute<ontol_hir::Node>,
    ) -> Attribute<ontol_hir::Node> {
        let rel = arena_import(target, source.node_ref(attr.rel));
        let val = arena_import(target, source.node_ref(attr.val));
        Attribute { rel, val }
    }

    fn import_entries<'m>(
        target: &mut TypedArena<'m>,
        source: &TypedArena<'m>,
        entries: &[ontol_hir::SetEntry<'m, TypedHir>],
    ) -> SmallVec<[ontol_hir::SetEntry<'m, TypedHir>; 1]> {
        entries
            .iter()
            .map(|SetEntry(iter, attr)| SetEntry(*iter, import_attr(target, source, *attr)))
            .collect()
    }

    use ontol_hir::Kind::*;

    match kind {
        NoOp => target.add(TypedHirData(kind.clone(), *meta)),
        Var(_) | Unit | I64(_) | F64(_) | Text(_) | Const(_) | CopySubSeq(..)
        | MoveRestAttrs(..) => {
            // No subnodes
            target.add(TypedHirData(kind.clone(), *meta))
        }
        Block(body) => {
            let imported_body = import_nodes(target, source.arena(), body);
            target.add(TypedHirData(Block(imported_body), *meta))
        }
        Catch(label, body) => {
            let imported_body = import_nodes(target, source.arena(), body);
            target.add(TypedHirData(Catch(*label, imported_body), *meta))
        }
        Try(label, var) => target.add(TypedHirData(Try(*label, *var), *meta)),
        Let(binder, node) => {
            let node = arena_import(target, source.arena().node_ref(*node));
            target.add(TypedHirData(Let(*binder, node), *meta))
        }
        TryLet(label, binder, node) => {
            let node = arena_import(target, source.arena().node_ref(*node));
            target.add(TypedHirData(TryLet(*label, *binder, node), *meta))
        }
        LetProp(binding, (var, prop_id)) => {
            target.add(TypedHirData(LetProp(*binding, (*var, *prop_id)), *meta))
        }
        LetPropDefault(binding, (var, prop_id), default) => {
            let rel = arena_import(target, source.arena().node_ref(default.rel));
            let val = arena_import(target, source.arena().node_ref(default.val));
            target.add(TypedHirData(
                LetPropDefault(*binding, (*var, *prop_id), Attribute { rel, val }),
                *meta,
            ))
        }
        TryLetProp(catch, attr, (var, prop_id)) => target.add(TypedHirData(
            TryLetProp(*catch, *attr, (*var, *prop_id)),
            *meta,
        )),
        TryLetTup(catch, bindings, source) => target.add(TypedHirData(
            TryLetTup(*catch, bindings.clone(), *source),
            *meta,
        )),
        LetRegex(groups_list, regex_def_id, var) => target.add(TypedHirData(
            LetRegex(groups_list.clone(), *regex_def_id, *var),
            *meta,
        )),
        LetRegexIter(binding, groups_list, regex_def_id, var) => target.add(TypedHirData(
            LetRegexIter(*binding, groups_list.clone(), *regex_def_id, *var),
            *meta,
        )),
        With(binder, def, body) => {
            let def = arena_import(target, source.arena().node_ref(*def));
            let body = import_nodes(target, source.arena(), body);

            target.add(TypedHirData(With(*binder, def, body), *meta))
        }
        Call(proc, args) => {
            let args = import_nodes(target, source.arena(), args);
            target.add(TypedHirData(Call(*proc, args), *meta))
        }
        Map(arg) => {
            let arg = arena_import(target, source.arena().node_ref(*arg));
            target.add(TypedHirData(Map(arg), *meta))
        }
        // DeclSet(label, attr) => {
        //     let attr = import_attr(target, source.arena(), *attr);
        //     target.add(TypedHirData(DeclSet(*label, attr), *meta))
        // }
        Set(entries) => {
            let entries = import_entries(target, source.arena(), entries);
            target.add(TypedHirData(Set(entries), *meta))
        }
        Struct(binder, flags, body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirData(Struct(*binder, *flags, body), *meta))
        }
        Prop(optional, struct_var, prop_id, variant) => {
            let variant = match variant {
                PropVariant::Value(attr) => {
                    PropVariant::Value(import_attr(target, source.arena(), *attr))
                }
                PropVariant::Predicate(operator, param) => PropVariant::Predicate(
                    *operator,
                    arena_import(target, source.arena().node_ref(*param)),
                ),
            };
            target.add(TypedHirData(
                Prop(*optional, *struct_var, *prop_id, variant),
                *meta,
            ))
        }
        MakeSeq(binder, body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirData(MakeSeq(*binder, body), *meta))
        }
        ForEach(var, (rel, val), body) => {
            let body = import_nodes(target, source.arena(), body);
            target.add(TypedHirData(ForEach(*var, (*rel, *val), body), *meta))
        }
        Insert(var, attr) => {
            let attr = import_attr(target, source.arena(), *attr);
            target.add(TypedHirData(Insert(*var, attr), *meta))
        }
        StringPush(var, node) => {
            let node = arena_import(target, source.arena().node_ref(*node));
            target.add(TypedHirData(StringPush(*var, node), *meta))
        }
        Regex(label, def_id, groups_list) => target.add(TypedHirData(
            Regex(*label, *def_id, groups_list.clone()),
            *meta,
        )),
        LetCondVar(bind_var, cond) => target.add(TypedHirData(LetCondVar(*bind_var, *cond), *meta)),
        PushCondClause(var, clause) => {
            target.add(TypedHirData(PushCondClause(*var, clause.clone()), *meta))
        }
    }
}
