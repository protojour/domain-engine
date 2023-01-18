use std::ops::Range;

use crate::{
    compile_error::CompileError,
    def::{Def, DefId, DefKind},
    env::Env,
    misc::{PackageId, SourceId, CORE_PKG},
    parse::ast,
    Span,
};

pub fn lower_ast<'m>(
    env: &mut Env<'m>,
    package: PackageId,
    source: SourceId,
    ast: (ast::Ast, Range<usize>),
) -> Result<(), CompileError> {
    ast_to_def(env, package, ast)?;
    Ok(())
}

fn ast_to_def<'m>(
    env: &mut Env<'m>,
    package: PackageId,
    (ast, span): (ast::Ast, Span),
) -> Result<DefId, CompileError> {
    match ast {
        ast::Ast::Data(ast::Data { ident, ty: ast_ty }) => {
            let def_id = env.alloc_def_id();

            env.namespaces
                .get_mut(package)
                .insert(ident.0.clone(), def_id);

            let type_def = ast_type_to_def(env, package, ast_ty)?;

            env.defs.insert(
                def_id,
                Def {
                    package,
                    kind: DefKind::Constructor(ident.0, type_def),
                },
            );

            Ok(def_id)
        }
        _ => panic!(),
    }
}

fn ast_type_to_def<'m>(
    env: &mut Env<'m>,
    package: PackageId,
    (ast_ty, span): (ast::Type, Span),
) -> Result<DefId, CompileError> {
    let def_id = env.alloc_def_id();
    match ast_ty {
        ast::Type::Sym(ident) => {
            let Some(type_def_id) = env.namespaces.lookup(&[package, CORE_PKG], &ident) else {
                return Err(CompileError::TypeNotFound);
            };
            env.defs.insert(
                def_id,
                Def {
                    package,
                    kind: DefKind::Field { type_def_id },
                },
            );
        }
        ast::Type::Record(_) => {}
    };
    Ok(def_id)
}
