use smartstring::alias::String;

use crate::{
    compile::error::{CompileError, SpannedCompileError},
    def::{Def, DefId, DefKind},
    env::Env,
    parse::{ast, Span},
    source::{CompileSrc, CORE_PKG},
};

pub struct Lowering<'s, 'm> {
    env: &'s mut Env<'m>,
    src: &'s CompileSrc,
    root_defs: Vec<DefId>,
}

impl<'s, 'm> Lowering<'s, 'm> {
    pub fn new(env: &'s mut Env<'m>, src: &'s CompileSrc) -> Self {
        Self {
            env,
            src,
            root_defs: Default::default(),
        }
    }

    pub fn finish(self) -> Vec<DefId> {
        self.root_defs
    }

    pub fn lower_ast(&mut self, ast: (ast::Ast, Span)) -> Result<(), SpannedCompileError> {
        let root_def = self.ast_to_def(ast)?;
        self.root_defs.push(root_def);
        Ok(())
    }

    fn ast_to_def(&mut self, (ast, span): (ast::Ast, Span)) -> Result<DefId, SpannedCompileError> {
        match ast {
            ast::Ast::Data(ast::Data {
                ident,
                ty: (ast_ty, ty_span),
            }) => {
                let def_id = self.named_def_id(&ident.0);
                let type_def_id = self.ast_type_to_def((ast_ty, ty_span.clone()))?;
                let field_def_id = self.def(DefKind::AnonField { type_def_id }, &ty_span);

                self.set_def(def_id, DefKind::Constructor(ident.0, field_def_id), &span);

                Ok(def_id)
            }
            _ => panic!(),
        }
    }

    fn ast_type_to_def(
        &mut self,
        (ast_ty, span): (ast::Type, Span),
    ) -> Result<DefId, SpannedCompileError> {
        match ast_ty {
            ast::Type::Literal(_) => Err(self.error(CompileError::InvalidType, &span)),
            ast::Type::Sym(ident) => {
                let Some(type_def_id) = self.env.namespaces.lookup(&[self.src.package, CORE_PKG], &ident) else {
                return Err(self.error(CompileError::TypeNotFound, &span));
            };
                Ok(type_def_id)
            }
            ast::Type::Record(ast_record) => {
                let mut record_defs = vec![];
                for (ast_field, span) in ast_record.fields {
                    let type_def_id = self.ast_type_to_def(ast_field.ty)?;
                    record_defs.push(self.def(
                        DefKind::NamedField {
                            ident: ast_field.ident.0,
                            type_def_id,
                        },
                        &span,
                    ));
                }

                Ok(self.def(
                    DefKind::Record {
                        field_defs: record_defs,
                    },
                    &span,
                ))
            }
        }
    }

    fn named_def_id(&mut self, ident: &String) -> DefId {
        let def_id = self.env.alloc_def_id();
        self.env
            .namespaces
            .get_mut(self.src.package)
            .insert(ident.clone(), def_id);
        def_id
    }

    fn set_def(&mut self, def_id: DefId, kind: DefKind, span: &Span) {
        self.env.defs.insert(
            def_id,
            Def {
                package: self.src.package,
                kind,
                span: self.src.span(&span),
            },
        );
    }

    fn def(&mut self, kind: DefKind, span: &Span) -> DefId {
        let def_id = self.env.alloc_def_id();
        self.set_def(def_id, kind, span);
        def_id
    }

    fn error(&self, compile_error: CompileError, span: &Span) -> SpannedCompileError {
        compile_error.spanned(&self.env.sources, &self.src.span(span))
    }
}
