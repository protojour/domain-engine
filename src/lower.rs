use crate::{
    compile_error::{CompileError, SpannedCompileError},
    def::{Def, DefId, DefKind},
    env::Env,
    misc::{CompileSrc, CORE_PKG},
    parse::{ast, Span},
};

pub fn lower_ast<'m>(
    env: &mut Env<'m>,
    src: &CompileSrc,
    ast: (ast::Ast, Span),
) -> Result<(), SpannedCompileError> {
    ast_to_def(env, src, ast)?;
    Ok(())
}

fn ast_to_def<'m>(
    env: &mut Env<'m>,
    src: &CompileSrc,
    (ast, span): (ast::Ast, Span),
) -> Result<DefId, SpannedCompileError> {
    match ast {
        ast::Ast::Data(ast::Data { ident, ty: ast_ty }) => {
            let def_id = env.alloc_def_id();

            env.namespaces
                .get_mut(src.package)
                .insert(ident.0.clone(), def_id);

            let type_def = ast_type_to_def(env, src, ast_ty)?;

            env.defs.insert(
                def_id,
                Def {
                    package: src.package,
                    kind: DefKind::Constructor(ident.0, type_def),
                    span: src.span(span),
                },
            );

            Ok(def_id)
        }
        _ => panic!(),
    }
}

fn ast_type_to_def<'m>(
    env: &mut Env<'m>,
    src: &CompileSrc,
    (ast_ty, span): (ast::Type, Span),
) -> Result<DefId, SpannedCompileError> {
    let def_id = env.alloc_def_id();
    match ast_ty {
        ast::Type::Sym(ident) => {
            let Some(type_def_id) = env.namespaces.lookup(&[src.package, CORE_PKG], &ident) else {
                return Err(SpannedCompileError::new(CompileError::TypeNotFound, src));
            };
            env.defs.insert(
                def_id,
                Def {
                    package: src.package,
                    kind: DefKind::Field { type_def_id },
                    span: src.span(span),
                },
            );
        }
        ast::Type::Record(_) => {}
    };
    Ok(def_id)
}
