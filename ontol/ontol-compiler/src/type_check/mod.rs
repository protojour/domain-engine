use fnv::FnvHashMap;
use ontol_parser::source::SourceSpan;
use ontol_runtime::DefId;

use crate::{
    CompileErrors, Compiler, SpannedCompileError,
    codegen::task::CodeCtx,
    def::Defs,
    edge::EdgeCtx,
    entity::entity_ctx::EntityCtx,
    error::CompileError,
    misc::MiscCtx,
    pattern::Patterns,
    properties::PropCtx,
    relation::RelCtx,
    repr::{repr_check::ReprCheck, repr_ctx::ReprCtx},
    strings::StringCtx,
    thesaurus::Thesaurus,
    types::{DefTypeCtx, ERROR_TYPE, FormatType, TypeCtx, TypeRef},
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
    pub type_ctx: &'c mut TypeCtx<'m>,
    pub def_ty_ctx: &'c mut DefTypeCtx<'m>,
    pub prop_ctx: &'c mut PropCtx,
    pub misc_ctx: &'c mut MiscCtx,
    pub edge_ctx: &'c mut EdgeCtx,
    pub thesaurus: &'c mut Thesaurus,
    pub errors: &'c mut CompileErrors,
    pub code_ctx: &'c mut CodeCtx<'m>,
    pub patterns: &'c mut Patterns,
    pub repr_ctx: &'c mut ReprCtx,
    pub seal_ctx: &'c mut SealCtx,
    pub str_ctx: &'c mut StringCtx<'m>,
    pub defs: &'c Defs<'m>,
    pub rel_ctx: &'c RelCtx,
    pub entity_ctx: &'c EntityCtx,
}

#[derive(Clone, Copy, Debug)]
pub enum TypeError<'m> {
    // Another error is the cause of this error
    Propagated,
    Mismatch(TypeEquation<'m>),
    MustBeSequence(TypeRef<'m>),
    VariableMustBeSequenceEnclosed(TypeRef<'m>),
    NotEnoughInformation,
    #[expect(unused)]
    NotConvertibleFromNumber(TypeRef<'m>),
    #[expect(unused)]
    NoRelationParametersExpected,
    StructTypeNotInferrable,
}

impl<'m> TypeError<'m> {
    fn report<'c>(self, span: SourceSpan, tc: &mut TypeCheck<'c, 'm>) -> TypeRef<'m> {
        let error = match self {
            TypeError::Propagated => return &ERROR_TYPE,
            TypeError::Mismatch(equation) => CompileError::TypeMismatch {
                actual: format!("{}", FormatType::new(equation.actual.0, tc.defs)),
                expected: format!("{}", FormatType::new(equation.expected.0, tc.defs)),
            },
            TypeError::MustBeSequence(ty) => CompileError::TypeMismatch {
                actual: format!("{}", FormatType::new(ty, tc.defs)),
                expected: format!("{{{}}}", FormatType::new(ty, tc.defs)),
            },
            TypeError::VariableMustBeSequenceEnclosed(ty) => {
                CompileError::VariableMustBeSequenceEnclosed(format!(
                    "{}",
                    FormatType::new(ty, tc.defs)
                ))
            }
            TypeError::NotEnoughInformation => {
                CompileError::TODO("Not enough information to infer type")
            }
            TypeError::NotConvertibleFromNumber(ty) => CompileError::TODO(format!(
                "Type {} cannot be represented as a number",
                FormatType::new(ty, tc.defs)
            )),
            TypeError::NoRelationParametersExpected => CompileError::NoRelationParametersExpected,
            TypeError::StructTypeNotInferrable => CompileError::ExpectedExplicitStructPath,
        };

        error.span(span).report(tc);

        &ERROR_TYPE
    }
}

impl<'m> TypeCheck<'_, 'm> {
    pub(crate) fn repr_check<'tc>(&'tc mut self, root_def_id: DefId) -> ReprCheck<'tc, 'm> {
        ReprCheck {
            root_def_id,
            defs: self.defs,
            def_types: self.def_ty_ctx,
            rel_ctx: self.rel_ctx,
            misc_ctx: self.misc_ctx,
            thesaurus: self.thesaurus,
            repr_ctx: self.repr_ctx,
            prop_ctx: self.prop_ctx,
            errors: self.errors,
            state: Default::default(),
        }
    }
}

impl<'m> AsRef<Defs<'m>> for TypeCheck<'_, 'm> {
    fn as_ref(&self) -> &Defs<'m> {
        self.defs
    }
}

impl AsRef<RelCtx> for TypeCheck<'_, '_> {
    fn as_ref(&self) -> &RelCtx {
        self.rel_ctx
    }
}

impl AsMut<CompileErrors> for TypeCheck<'_, '_> {
    fn as_mut(&mut self) -> &mut CompileErrors {
        self.errors
    }
}

impl<'m> Compiler<'m> {
    pub fn type_check(&mut self) -> TypeCheck<'_, 'm> {
        TypeCheck {
            expected_constant_types: Default::default(),
            type_ctx: &mut self.ty_ctx,
            errors: &mut self.errors,
            def_ty_ctx: &mut self.def_ty_ctx,
            rel_ctx: &mut self.rel_ctx,
            prop_ctx: &mut self.prop_ctx,
            edge_ctx: &mut self.edge_ctx,
            misc_ctx: &mut self.misc_ctx,
            thesaurus: &mut self.thesaurus,
            code_ctx: &mut self.code_ctx,
            patterns: &mut self.patterns,
            repr_ctx: &mut self.repr_ctx,
            seal_ctx: &mut self.seal_ctx,
            str_ctx: &mut self.str_ctx,
            defs: &self.defs,
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
