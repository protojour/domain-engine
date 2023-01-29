use std::collections::HashMap;

use crate::{
    compiler::Compiler,
    def::Defs,
    error::CompileError,
    expr::{Expr, ExprId},
    mem::Intern,
    relation::Relations,
    types::{format_type, DefTypes, Type, TypeRef, Types},
    CompileErrors, SourceSpan, Sources,
};

pub mod check_def;
pub mod inference;

mod check_expr;

pub struct TypeCheck<'c, 'm> {
    types: &'c mut Types<'m>,
    def_types: &'c mut DefTypes<'m>,
    relations: &'c mut Relations,
    errors: &'c mut CompileErrors,
    expressions: &'c HashMap<ExprId, Expr>,
    defs: &'c Defs,
    sources: &'c Sources,
}

impl<'c, 'm> TypeCheck<'c, 'm> {
    fn error(&mut self, error: CompileError, span: &SourceSpan) -> TypeRef<'m> {
        self.errors.push(error.spanned(&self.sources, span));
        self.types.intern(Type::Error)
    }

    fn type_error(&mut self, error: TypeError<'m>, span: &SourceSpan) -> TypeRef<'m> {
        let compile_error = match error {
            TypeError::Mismatch { actual, expected } => CompileError::TypeMismatch {
                actual: format_type(actual),
                expected: format_type(expected),
            },
        };
        self.error(compile_error, span)
    }
}

#[derive(Debug)]
pub enum TypeError<'m> {
    Mismatch {
        actual: TypeRef<'m>,
        expected: TypeRef<'m>,
    },
}

impl<'c, 'm> AsRef<Defs> for TypeCheck<'c, 'm> {
    fn as_ref(&self) -> &Defs {
        &self.defs
    }
}

impl<'c, 'm> AsRef<Relations> for TypeCheck<'c, 'm> {
    fn as_ref(&self) -> &Relations {
        &self.relations
    }
}

impl<'m> Compiler<'m> {
    pub fn type_check(&mut self) -> TypeCheck<'_, 'm> {
        TypeCheck {
            types: &mut self.types,
            errors: &mut self.errors,
            def_types: &mut self.def_types,
            relations: &mut self.relations,
            expressions: &self.expressions,
            defs: &self.defs,
            sources: &self.sources,
        }
    }
}
