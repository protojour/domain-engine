use fnv::FnvHashMap;
use ontol_runtime::DefId;

use crate::{
    codegen::task::CodeCtx,
    def::Defs,
    entity::entity_ctx::EntityCtx,
    error::CompileError,
    pattern::Patterns,
    primitive::Primitives,
    relation::RelCtx,
    repr::{repr_check::ReprCheck, repr_ctx::ReprCtx},
    strings::StringCtx,
    thesaurus::Thesaurus,
    types::{DefTypeCtx, FormatType, TypeCtx, TypeRef, ERROR_TYPE},
    CompileErrors, Compiler, SourceSpan, SpannedCompileError,
};

use self::ena_inference::KnownType;
use self::seal::SealCtx;

pub mod check_def;
pub mod check_domain_types;
pub mod check_union;
pub mod ena_inference;
pub mod seal;

mod check_extern;
mod check_map;
mod check_relationship;
mod hir_build;
mod hir_build_ctx;
mod hir_build_props;
mod hir_type_inference;
mod map_arm_analyze;

// Experimental setting
// pub const LABEL_PER_ITER_ELEMENT: bool = false;

#[derive(Clone, Copy, Debug)]
pub struct TypeEquation<'m> {
    actual: KnownType<'m>,
    expected: KnownType<'m>,
}

#[derive(Clone)]
pub enum MapArmsKind {
    Concrete,
    Abstract,
    /// Instantiation of an abstract map
    Template([DefId; 2]),
}

/// Type checking is a stage in compilation.
/// It is not only an immutable "check" of typing,
/// but also actually produces more output for later compile stages.
pub struct TypeCheck<'c, 'm> {
    /// This map stores the expected type of constants
    pub expected_constant_types: FnvHashMap<DefId, TypeRef<'m>>,
    pub types: &'c mut TypeCtx<'m>,
    pub def_ty_ctx: &'c mut DefTypeCtx<'m>,
    pub rel_ctx: &'c mut RelCtx,
    pub thesaurus: &'c mut Thesaurus,
    pub errors: &'c mut CompileErrors,
    pub code_ctx: &'c mut CodeCtx<'m>,
    pub patterns: &'c mut Patterns,
    pub repr_ctx: &'c mut ReprCtx,
    pub seal_ctx: &'c mut SealCtx,
    pub str_ctx: &'c mut StringCtx<'m>,
    pub defs: &'c Defs<'m>,
    pub entity_ctx: &'c EntityCtx,
    pub primitives: &'c Primitives,
}

#[derive(Clone, Copy, Debug)]
pub enum TypeError<'m> {
    // Another error is the cause of this error
    Propagated,
    Mismatch(TypeEquation<'m>),
    MustBeSequence(TypeRef<'m>),
    VariableMustBeSequenceEnclosed(TypeRef<'m>),
    NotEnoughInformation,
    #[allow(unused)]
    NotConvertibleFromNumber(TypeRef<'m>),
    #[allow(unused)]
    NoRelationParametersExpected,
    StructTypeNotInferrable,
}

impl<'m> TypeError<'m> {
    fn report<'c>(self, span: SourceSpan, tc: &mut TypeCheck<'c, 'm>) -> TypeRef<'m> {
        let error = match self {
            TypeError::Propagated => return &ERROR_TYPE,
            TypeError::Mismatch(equation) => CompileError::TypeMismatch {
                actual: format!(
                    "{}",
                    FormatType::new(equation.actual.0, tc.defs, tc.primitives)
                ),
                expected: format!(
                    "{}",
                    FormatType::new(equation.expected.0, tc.defs, tc.primitives)
                ),
            },
            TypeError::MustBeSequence(ty) => CompileError::TypeMismatch {
                actual: format!("{}", FormatType::new(ty, tc.defs, tc.primitives)),
                expected: format!("{{{}}}", FormatType::new(ty, tc.defs, tc.primitives)),
            },
            TypeError::VariableMustBeSequenceEnclosed(ty) => {
                CompileError::VariableMustBeSequenceEnclosed(format!(
                    "{}",
                    FormatType::new(ty, tc.defs, tc.primitives)
                ))
            }
            TypeError::NotEnoughInformation => {
                CompileError::TODO("Not enough information to infer type")
            }
            TypeError::NotConvertibleFromNumber(ty) => CompileError::TODO(format!(
                "Type {} cannot be represented as a number",
                FormatType::new(ty, tc.defs, tc.primitives)
            )),
            TypeError::NoRelationParametersExpected => CompileError::NoRelationParametersExpected,
            TypeError::StructTypeNotInferrable => CompileError::ExpectedExplicitStructPath,
        };

        error.span(span).report(tc);

        &ERROR_TYPE
    }
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    pub(crate) fn repr_check<'tc>(&'tc mut self, root_def_id: DefId) -> ReprCheck<'tc, 'm> {
        ReprCheck {
            root_def_id,
            defs: self.defs,
            def_types: self.def_ty_ctx,
            relations: self.rel_ctx,
            thesaurus: self.thesaurus,
            primitives: self.primitives,
            repr_ctx: self.repr_ctx,
            errors: self.errors,
            state: Default::default(),
        }
    }
}

impl<'c, 'm> AsRef<Defs<'m>> for TypeCheck<'c, 'm> {
    fn as_ref(&self) -> &Defs<'m> {
        self.defs
    }
}

impl<'c, 'm> AsRef<RelCtx> for TypeCheck<'c, 'm> {
    fn as_ref(&self) -> &RelCtx {
        self.rel_ctx
    }
}

impl<'c, 'm> AsMut<CompileErrors> for TypeCheck<'c, 'm> {
    fn as_mut(&mut self) -> &mut CompileErrors {
        self.errors
    }
}

impl<'m> Compiler<'m> {
    pub fn type_check(&mut self) -> TypeCheck<'_, 'm> {
        TypeCheck {
            expected_constant_types: Default::default(),
            types: &mut self.ty_ctx,
            errors: &mut self.errors,
            def_ty_ctx: &mut self.def_ty_ctx,
            rel_ctx: &mut self.rel_ctx,
            thesaurus: &mut self.thesaurus,
            code_ctx: &mut self.code_ctx,
            patterns: &mut self.patterns,
            repr_ctx: &mut self.repr_ctx,
            seal_ctx: &mut self.seal_ctx,
            str_ctx: &mut self.str_ctx,
            defs: &self.defs,
            primitives: &self.primitives,
            entity_ctx: &self.entity_ctx,
        }
    }
}

impl SpannedCompileError {
    /// Report the error and return the [ERROR_TYPE]
    fn report_ty(self, ctx: &mut TypeCheck) -> TypeRef<'static> {
        ctx.as_mut().errors.push(self);
        &ERROR_TYPE
    }
}
