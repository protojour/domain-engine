use std::collections::HashMap;

use crate::{
    compile_error::{CompileError, CompileErrors},
    def::{Def, DefId, DefKind, Primitive},
    env::Env,
    expr::{Expr, ExprKind},
    mem::Intern,
    types::{Type, TypeRef, Types},
};

struct DefTck<'e, 'm> {
    types: &'e mut Types<'m>,
    def_types: &'e mut HashMap<DefId, TypeRef<'m>>,
    errors: &'e mut CompileErrors,
    defs: &'e HashMap<DefId, Def>,
}

impl<'e, 'm> DefTck<'e, 'm> {
    fn check(&mut self, def_id: DefId) -> TypeRef<'m> {
        if let Some(type_ref) = self.def_types.get(&def_id) {
            return type_ref;
        }

        match &self.defs.get(&def_id).unwrap().kind {
            DefKind::Constructor(_, field_def) => {
                let ty = self.types.intern(Type::Data(def_id, *field_def));
                self.def_types.insert(def_id, ty);

                self.type_check_field(*field_def);

                ty
            }
            DefKind::Primitive(Primitive::Number) => self.types.intern(Type::Number),
            DefKind::Equivalence(_, _) => self.types.intern(Type::Tautology),
            other => {
                panic!("failed def typecheck: {other:?}");
            }
        }
    }

    fn type_check_field(&mut self, field_id: DefId) {
        let field = self
            .defs
            .get(&field_id)
            .expect("No field definition for {field_id:?}");
        let DefKind::Field { type_def_id } = &field.kind else {
            panic!("Field definition is not a field: {:?}", field.kind);
        };

        let type_ref = self.check(*type_def_id);
        self.def_types.insert(field_id, type_ref);
    }
}

struct ExprTck<'e, 'm> {
    types: &'e mut Types<'m>,
    errors: &'e mut CompileErrors,
    def_types: &'e HashMap<DefId, TypeRef<'m>>,
    defs: &'e HashMap<DefId, Def>,
}

impl<'e, 'm> ExprTck<'e, 'm> {
    fn check(&mut self, expr: &Expr) -> TypeRef<'m> {
        match &expr.kind {
            ExprKind::Constant(_) => self.types.intern(Type::Number),
            ExprKind::Call(def_id, args) => match self.def_types.get(&def_id) {
                Some(Type::Function { params, output }) => {
                    if args.len() != params.len() {
                        self.errors.push(CompileError::WrongNumberOfArguments);
                        return self.types.intern(Type::Error);
                    }
                    for (arg, param_ty) in args.iter().zip(*params) {
                        self.check(arg);
                    }
                    *output
                }
                Some(Type::Data(data_def_id, field_id)) => {
                    if args.len() != 1 {
                        self.errors.push(CompileError::WrongNumberOfArguments);
                    }

                    match self.def_types.get(data_def_id) {
                        Some(ty) => ty,
                        None => self.types.intern(Type::Error),
                    }
                }
                _ => {
                    self.errors.push(CompileError::NotCallable);
                    self.types.intern(Type::Error)
                }
            },
            ExprKind::Variable(id) => {
                panic!()
            }
        }
    }
}

impl<'m> Env<'m> {
    fn def_tck(&mut self) -> DefTck<'_, 'm> {
        DefTck {
            types: &mut self.types,
            defs: &self.defs,
            errors: &mut self.errors,
            def_types: &mut self.def_types,
        }
    }

    fn expr_tck(&mut self) -> ExprTck<'_, 'm> {
        ExprTck {
            types: &mut self.types,
            errors: &mut self.errors,
            defs: &self.defs,
            def_types: &self.def_types,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        compile_error::CompileError,
        env::Env,
        expr::{ExprId, ExprKind},
        mem::Mem,
        misc::{SourceSpan, TEST_PKG},
        Compile,
    };

    #[test]
    fn type_check_data_call() -> Result<(), CompileError> {
        let mem = Mem::default();
        let mut env = Env::new(&mem).with_core();

        "(data m (number))".compile(&mut env, TEST_PKG)?;

        let m = env
            .namespaces
            .lookup(&[TEST_PKG], "m")
            .expect("m not found");
        let type_of_m = env.def_tck().check(m);

        let args = vec![env.expr(ExprKind::Variable(ExprId(100)), SourceSpan::none())];
        let expr = env.expr(ExprKind::Call(m, args), SourceSpan::none());
        let expr_type = env.expr_tck().check(&expr);

        assert_eq!(expr_type, type_of_m);

        Ok(())
    }
}
