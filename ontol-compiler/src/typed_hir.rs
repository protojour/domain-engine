use std::fmt::{Debug, Display};

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

    fn with_meta<'a, T: Clone>(&self, value: T) -> Self::Meta<'a, T> {
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
