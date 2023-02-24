use fnv::FnvHashMap;
use ontol_runtime::smart_format;

use crate::{
    codegen::CodegenTasks,
    compiler::Compiler,
    def::Defs,
    error::CompileError,
    expr::{Expr, ExprId},
    mem::Intern,
    relation::Relations,
    types::{DefTypes, FormatType, Type, TypeRef, Types},
    CompileErrors, SourceSpan,
};

pub mod check_def;
pub mod check_domain_types;
pub mod check_union;
pub mod inference;

mod check_expr;
mod check_relationship;

#[derive(Debug)]
pub enum TypeError<'m> {
    Mismatch {
        actual: TypeRef<'m>,
        expected: TypeRef<'m>,
    },
}

/// Type checking is a stage in compilation.
/// It is not only an immutable "check" of typing,
/// but also actually produces more output for later compile stages.
pub struct TypeCheck<'c, 'm> {
    types: &'c mut Types<'m>,
    def_types: &'c mut DefTypes<'m>,
    relations: &'c mut Relations,
    errors: &'c mut CompileErrors,
    codegen_tasks: &'c mut CodegenTasks<'m>,
    expressions: &'c FnvHashMap<ExprId, Expr>,
    defs: &'c Defs<'m>,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        self.errors.push(error.spanned(span));
        self.types.intern(Type::Error)
    }

    fn type_error(&mut self, error: TypeError<'m>, span: &SourceSpan) -> TypeRef<'m> {
        let compile_error = match error {
            TypeError::Mismatch { actual, expected } => CompileError::TypeMismatch {
                actual: smart_format!("{}", FormatType(actual, self.defs)),
                expected: smart_format!("{}", FormatType(expected, self.defs)),
            },
        };
        self.error(compile_error, span)
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
            types: &mut self.types,
            errors: &mut self.errors,
            def_types: &mut self.def_types,
            relations: &mut self.relations,
            codegen_tasks: &mut self.codegen_tasks,
            expressions: &self.expressions,
            defs: &self.defs,
        }
    }
}
