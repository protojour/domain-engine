use fnv::FnvHashMap;
use ontol_runtime::{smart_format, DefId};

use crate::{
    codegen::task::CodegenTasks,
    def::Defs,
    error::CompileError,
    mem::Intern,
    pattern::Patterns,
    primitive::Primitives,
    relation::Relations,
    repr::repr_check::ReprCheck,
    strings::Strings,
    thesaurus::Thesaurus,
    types::{DefTypes, FormatType, Type, TypeRef, Types, ERROR_TYPE},
    CompileErrors, Compiler, SourceSpan, SpannedNote,
};

use self::ena_inference::KnownType;
use self::seal::SealCtx;

pub mod check_def;
pub mod check_domain_types;
pub mod check_union;
pub mod ena_inference;
pub mod seal;

mod check_entity;
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
pub enum TypeError<'m> {
    // Another error is the cause of this error
    Propagated,
    Mismatch(TypeEquation<'m>),
    MustBeSequence(TypeRef<'m>),
    VariableMustBeSequenceEnclosed(TypeRef<'m>),
    NotEnoughInformation,
    NotConvertibleFromNumber(TypeRef<'m>),
    NoRelationParametersExpected,
    StructTypeNotInferrable,
}

#[derive(Clone, Copy, Debug)]
pub struct TypeEquation<'m> {
    actual: KnownType<'m>,
    expected: KnownType<'m>,
}

/// Type checking is a stage in compilation.
/// It is not only an immutable "check" of typing,
/// but also actually produces more output for later compile stages.
pub struct TypeCheck<'c, 'm> {
    /// This map stores the expected type of constants
    pub expected_constant_types: FnvHashMap<DefId, TypeRef<'m>>,
    pub types: &'c mut Types<'m>,
    pub def_types: &'c mut DefTypes<'m>,
    pub relations: &'c mut Relations,
    pub thesaurus: &'c mut Thesaurus,
    pub errors: &'c mut CompileErrors,
    pub codegen_tasks: &'c mut CodegenTasks<'m>,
    pub patterns: &'c mut Patterns,
    pub seal_ctx: &'c mut SealCtx,
    pub strings: &'c mut Strings<'m>,
    pub defs: &'c Defs<'m>,
    pub primitives: &'c Primitives,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        self.error_with_notes(error, span, vec![])
    }

    fn error_with_notes(
        &mut self,
        error: CompileError,
        span: &SourceSpan,
        notes: Vec<SpannedNote>,
    ) -> TypeRef<'m> {
        self.errors.error_with_notes(error, span, notes);
        &ERROR_TYPE
    }

    fn type_error(&mut self, error: TypeError<'m>, span: &SourceSpan) -> TypeRef<'m> {
        match error {
            TypeError::Propagated => self.types.intern(Type::Error),
            TypeError::Mismatch(equation) => self.error(
                CompileError::TypeMismatch {
                    actual: smart_format!(
                        "{}",
                        FormatType(equation.actual.0, self.defs, self.primitives)
                    ),
                    expected: smart_format!(
                        "{}",
                        FormatType(equation.expected.0, self.defs, self.primitives)
                    ),
                },
                span,
            ),
            TypeError::MustBeSequence(ty) => self.error(
                CompileError::TypeMismatch {
                    actual: smart_format!("{}", FormatType(ty, self.defs, self.primitives)),
                    expected: smart_format!("{{{}}}", FormatType(ty, self.defs, self.primitives)),
                },
                span,
            ),
            TypeError::VariableMustBeSequenceEnclosed(ty) => self.error(
                CompileError::VariableMustBeSequenceEnclosed(smart_format!(
                    "{}",
                    FormatType(ty, self.defs, self.primitives)
                )),
                span,
            ),
            TypeError::NotEnoughInformation => self.error(
                CompileError::TODO(smart_format!("Not enough information to infer type")),
                span,
            ),
            TypeError::NotConvertibleFromNumber(ty) => self.error(
                CompileError::TODO(smart_format!(
                    "Type {} cannot be represented as a number",
                    FormatType(ty, self.defs, self.primitives)
                )),
                span,
            ),
            TypeError::NoRelationParametersExpected => {
                self.error(CompileError::NoRelationParametersExpected, span)
            }
            TypeError::StructTypeNotInferrable => {
                self.error(CompileError::ExpectedExplicitStructPath, span)
            }
        }
    }

    pub(crate) fn repr_check<'tc>(&'tc mut self, root_def_id: DefId) -> ReprCheck<'tc, 'm> {
        ReprCheck {
            root_def_id,
            defs: self.defs,
            def_types: self.def_types,
            relations: self.relations,
            thesaurus: self.thesaurus,
            primitives: self.primitives,
            seal_ctx: self.seal_ctx,
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

impl<'c, 'm> AsRef<Relations> for TypeCheck<'c, 'm> {
    fn as_ref(&self) -> &Relations {
        self.relations
    }
}

impl<'m> Compiler<'m> {
    pub fn type_check(&mut self) -> TypeCheck<'_, 'm> {
        TypeCheck {
            expected_constant_types: Default::default(),
            types: &mut self.types,
            errors: &mut self.errors,
            def_types: &mut self.def_types,
            relations: &mut self.relations,
            thesaurus: &mut self.thesaurus,
            codegen_tasks: &mut self.codegen_tasks,
            patterns: &mut self.patterns,
            seal_ctx: &mut self.seal_ctx,
            strings: &mut self.strings,
            defs: &self.defs,
            primitives: &self.primitives,
        }
    }
}
