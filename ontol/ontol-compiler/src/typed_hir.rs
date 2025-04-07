use std::fmt::{Debug, Display};

use ontol_parser::source::{NO_SPAN, SourceSpan};

use crate::types::{ERROR_TYPE, TypeRef, UNIT_TYPE};

/// An ontol_hir language "dialect" with type information and source spans.
#[derive(Clone, Copy)]
pub struct TypedHir;

impl ontol_hir::Lang for TypedHir {
    type Data<'m, H>
        = TypedHirData<'m, H>
    where
        H: Clone;

    fn default_data<'m, H: Clone>(&self, hir: H) -> Self::Data<'m, H> {
        TypedHirData(hir, ERROR_META)
    }

    fn wrap<'a, H: Clone>(data: &Self::Data<'a, H>, hir: H) -> Self::Data<'a, H> {
        TypedHirData(hir, data.1)
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
    /// Access the ontol-hir part of data
    pub fn hir(&self) -> &H {
        &self.0
    }

    pub fn hir_mut(&mut self) -> &mut H {
        &mut self.0
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

impl<T: Debug> std::fmt::Debug for TypedHirData<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut tup = f.debug_tuple("Data");
        tup.field(&self.0);
        tup.field(&self.meta().ty);
        tup.finish()
    }
}

pub trait IntoTypedHirData<'m>: Sized {
    fn with_meta(self, meta: Meta<'m>) -> TypedHirData<'m, Self>;
}

impl<'m, T> IntoTypedHirData<'m> for T {
    fn with_meta(self, meta: Meta<'m>) -> TypedHirData<'m, Self> {
        TypedHirData(self, meta)
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
}

pub static ERROR_META: Meta<'static> = Meta {
    ty: &ERROR_TYPE,
    span: NO_SPAN,
};

pub struct HirFunc<'m> {
    pub arg: TypedHirData<'m, ontol_hir::Binder>,
    pub body: ontol_hir::RootNode<'m, TypedHir>,
}

impl Display for HirFunc<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "|{}| {}", self.arg.0.var, self.body)
    }
}
