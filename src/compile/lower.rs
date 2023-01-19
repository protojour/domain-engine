use crate::{
    compile::error::{CompileError, SpannedCompileError},
    def::{Def, DefId, DefKind},
    env::Env,
    parse::{ast, Span},
    source::{CompileSrc, CORE_PKG},
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
        ast::Ast::Data(ast::Data {
            ident,
            ty: (ast_ty, span),
        }) => {
            let def_id = env.alloc_def_id();

            env.namespaces
                .get_mut(src.package)
                .insert(ident.0.clone(), def_id);

            let type_def_id = ast_type_to_def(env, src, (ast_ty, span.clone()))?;
            let field_def_id = env.add_def(
                DefKind::AnonField { type_def_id },
                src.package,
                src.span(&span),
            );

            env.defs.insert(
                def_id,
                Def {
                    package: src.package,
                    kind: DefKind::Constructor(ident.0, field_def_id),
                    span: src.span(&span),
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
    match ast_ty {
        ast::Type::Literal(_) => {
            Err(CompileError::InvalidType.spanned(&env.sources, &src.span(&span)))
        }
        ast::Type::Sym(ident) => {
            let Some(type_def_id) = env.namespaces.lookup(&[src.package, CORE_PKG], &ident) else {
                return Err(CompileError::TypeNotFound.spanned(&env.sources, &src.span(&span)));
            };
            Ok(type_def_id)
        }
        ast::Type::Record(ast_record) => {
            let mut record_defs = vec![];
            for (ast_field, span) in ast_record.fields {
                let type_def_id = ast_type_to_def(env, src, ast_field.ty)?;
                record_defs.push(env.add_def(
                    DefKind::NamedField {
                        ident: ast_field.ident.0,
                        type_def_id,
                    },
                    src.package,
                    src.span(&span),
                ));
            }

            Ok(env.add_def(
                DefKind::Record {
                    field_defs: record_defs,
                },
                src.package,
                src.span(&span),
            ))
        }
    }
}
