use fnv::FnvHashMap;
use ontol_runtime::{smart_format, DefId};

use crate::{
    codegen::CodegenTasks,
    def::Defs,
    error::CompileError,
    expr::Expressions,
    mem::Intern,
    primitive::Primitives,
    relation::Relations,
    types::{DefTypes, FormatType, Type, TypeRef, Types},
    CompileErrors, Compiler, SourceSpan,
};

pub mod check_def;
pub mod check_domain_types;
pub mod check_union;
pub mod inference;

mod check_expr;
mod check_expr2;
mod check_map;
mod check_relationship;
mod unify_ctx;

#[derive(Clone, Copy, Debug)]
pub enum TypeError<'m> {
    Mismatch(TypeEquation<'m>),
    MustBeSequence(TypeRef<'m>),
    VariableMustBeSequenceEnclosed(TypeRef<'m>),
    NotEnoughInformation,
    // Another error is the cause of this error
    Propagated,
    NotConvertibleFromNumber(TypeRef<'m>),
}

#[derive(Clone, Copy, Debug)]
pub struct TypeEquation<'m> {
    actual: TypeRef<'m>,
    expected: TypeRef<'m>,
}

/// Type checking is a stage in compilation.
/// It is not only an immutable "check" of typing,
/// but also actually produces more output for later compile stages.
pub struct TypeCheck<'c, 'm> {
    /// This map stores the expected type of constants
    expected_constant_types: FnvHashMap<DefId, TypeRef<'m>>,
    types: &'c mut Types<'m>,
    def_types: &'c mut DefTypes<'m>,
    relations: &'c mut Relations,
    errors: &'c mut CompileErrors,
    codegen_tasks: &'c mut CodegenTasks<'m>,
    expressions: &'c mut Expressions,
    defs: &'c Defs<'m>,
    primitives: &'c Primitives,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        self.errors.push(error.spanned(span));
        self.types.intern(Type::Error)
    }

    fn type_error(&mut self, error: TypeError<'m>, span: &SourceSpan) -> TypeRef<'m> {
        match error {
            TypeError::Mismatch(equation) => self.error(
                CompileError::TypeMismatch {
                    actual: smart_format!(
                        "{}",
                        FormatType(equation.actual, self.defs, self.primitives)
                    ),
                    expected: smart_format!(
                        "{}",
                        FormatType(equation.expected, self.defs, self.primitives)
                    ),
                },
                span,
            ),
            TypeError::MustBeSequence(ty) => self.error(
                CompileError::TypeMismatch {
                    actual: smart_format!("{}", FormatType(ty, self.defs, self.primitives)),
                    expected: smart_format!("[{}]", FormatType(ty, self.defs, self.primitives)),
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
            TypeError::Propagated => self.types.intern(Type::Error),
            TypeError::NotConvertibleFromNumber(ty) => self.error(
                CompileError::TODO(smart_format!(
                    "Type {} cannot be represented as a number",
                    FormatType(ty, self.defs, self.primitives)
                )),
                span,
            ),
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
            codegen_tasks: &mut self.codegen_tasks,
            expressions: &mut self.expressions,
            defs: &self.defs,
            primitives: &self.primitives,
        }
    }
}
