use std::collections::HashMap;

use crate::{
    compile_error::{CompileError, CompileErrors},
    def::{Def, DefId, DefKind, Primitive},
    env::Env,
    expr::{Expr, ExprKind},
    mem::Intern,
    types::{Type, TypeRef, Types},
};

struct DefCtx<'e, 'm> {
    types: &'e mut Types<'m>,
    def_types: &'e mut HashMap<DefId, TypeRef<'m>>,
    errors: &'e mut CompileErrors,
    defs: &'e HashMap<DefId, Def>,
}

struct ExprCtx<'e, 'm> {
    types: &'e mut Types<'m>,
    errors: &'e mut CompileErrors,
    def_types: &'e HashMap<DefId, TypeRef<'m>>,
    defs: &'e HashMap<DefId, Def>,
}

impl<'m> Env<'m> {
    fn def_type_check_ctx(&mut self) -> DefCtx<'_, 'm> {
        DefCtx {
            types: &mut self.types,
            defs: &self.defs,
            errors: &mut self.errors,
            def_types: &mut self.def_types,
        }
    }

    fn expr_type_check_ctx(&mut self) -> ExprCtx<'_, 'm> {
        ExprCtx {
            types: &mut self.types,
            errors: &mut self.errors,
            defs: &self.defs,
            def_types: &self.def_types,
        }
    }
}

fn type_check_def<'e, 'm>(ctx: &mut DefCtx<'e, 'm>, def_id: DefId) -> TypeRef<'m> {
    if let Some(type_ref) = ctx.def_types.get(&def_id) {
        return type_ref;
    }

    match &ctx.defs.get(&def_id).unwrap().kind {
        DefKind::Constructor(_, field_def) => {
            let ty = ctx.types.intern(Type::Data(def_id, *field_def));
            ctx.def_types.insert(def_id, ty);

            type_check_field(ctx, *field_def);

            ty
        }
        DefKind::Primitive(Primitive::Number) => ctx.types.intern(Type::Number),
        DefKind::Equivalence(_, _) => ctx.types.intern(Type::Tautology),
        other => {
            panic!("failed def typecheck: {other:?}");
        }
    }
}

fn type_check_field<'e, 'm>(ctx: &mut DefCtx<'e, 'm>, field_id: DefId) {
    let field = ctx
        .defs
        .get(&field_id)
        .expect("No field definition for {field_id:?}");
    let DefKind::Field { type_def_id } = &field.kind else {
        panic!("Field definition is not a field: {:?}", field.kind);
    };

    let type_ref = type_check_def(ctx, *type_def_id);
    ctx.def_types.insert(field_id, type_ref);
}

fn type_check_expr<'e, 'm>(ctx: &mut ExprCtx<'e, 'm>, expr: &Expr) -> TypeRef<'m> {
    match &expr.kind {
        ExprKind::Constant(_) => ctx.types.intern(Type::Number),
        ExprKind::Call(def_id, args) => match ctx.def_types.get(&def_id) {
            Some(Type::Function { params, output }) => {
                if args.len() != params.len() {
                    ctx.errors.push(CompileError::WrongNumberOfArguments);
                    return ctx.types.intern(Type::Error);
                }
                for (arg, param_ty) in args.iter().zip(*params) {
                    type_check_expr(ctx, arg);
                }
                *output
            }
            Some(Type::Data(data_def_id, field_id)) => {
                if args.len() != 1 {
                    ctx.errors.push(CompileError::WrongNumberOfArguments);
                }

                match ctx.def_types.get(data_def_id) {
                    Some(ty) => ty,
                    None => ctx.types.intern(Type::Error),
                }
            }
            _ => {
                ctx.errors.push(CompileError::NotCallable);
                ctx.types.intern(Type::Error)
            }
        },
        ExprKind::Variable(id) => {
            panic!()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        def::DefKind,
        env::Env,
        expr::{ExprId, ExprKind},
        mem::Mem,
        misc::{PackageId, SourceSpan},
    };

    use super::{type_check_def, type_check_expr};

    #[test]
    fn test_type_check_data_call() {
        let mem = Mem::default();
        let mut env = Env::new(&mem);
        env.register_builtins();

        let package = PackageId(1);
        let span = SourceSpan::none();
        let field = env.add_def(
            package,
            DefKind::Field {
                type_def_id: env.core_def_by_name("Number").unwrap(),
            },
        );
        let m = env.add_named_def(package, "m", DefKind::Constructor("m".into(), field));
        let m_type = type_check_def(&mut env.def_type_check_ctx(), m);

        let args = vec![env.expr(ExprKind::Variable(ExprId(100)), span)];
        let expr = env.expr(ExprKind::Call(m, args), SourceSpan::none());
        let expr_type = type_check_expr(&mut env.expr_type_check_ctx(), &expr);

        assert_eq!(m_type, expr_type);
    }
}
