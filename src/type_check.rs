use std::collections::HashMap;

use crate::{
    def::{DefId, DefKind, Primitive, Def},
    env::{Env, InternMut},
    expr::{Expr, ExprKind},
    types::{Type, TypeKind, Types},
};

struct DefCtx<'e, 'm> {
    types: &'e mut Types<'m>,
    def_types: &'e mut HashMap<DefId, Type<'m>>,
    defs: &'e HashMap<DefId, Def>,
}

struct ExprCtx<'e, 'm> {
    types: &'e mut Types<'m>,
    def_types: &'e HashMap<DefId, Type<'m>>,
    defs: &'e HashMap<DefId, Def>,
}

impl<'m> Env<'m> {
    fn def_type_check_ctx(&mut self) -> DefCtx<'_, 'm> {
        DefCtx {
            types: &mut self.types,
            defs: &self.defs,
            def_types: &mut self.def_types
        }
    }

    fn expr_type_check_ctx(&mut self) -> ExprCtx<'_, 'm> {
        ExprCtx {
            types: &mut self.types,
            defs: &self.defs,
            def_types: &self.def_types
        }
    }
}

fn type_check_def<'e, 'm>(ctx: &mut DefCtx<'e, 'm>, def_id: DefId) -> Type<'m> {
    match &ctx.defs.get(&def_id).unwrap().kind {
        DefKind::Constructor(_, arg_def) => {
            let arg_type = type_check_def(ctx, *arg_def);

            let params = ctx.types.intern_mut([arg_type]);
            let output = ctx.types.intern_mut(TypeKind::New(def_id, arg_type));
            let fn_ty = ctx.types.intern_mut(TypeKind::Function {
                params,
                output,
            });

            ctx.def_types.insert(def_id, fn_ty);
            fn_ty
        }
        DefKind::Primitive(Primitive::Number) => ctx.types.intern_mut(TypeKind::Number),
        other => {
            panic!("failed def typecheck: {other:?}");
        }
    }
}

fn type_check_expr<'e, 'm>(ctx: &mut ExprCtx<'e, 'm>, expr: &Expr) -> Type<'m> {
    match &expr.kind {
        ExprKind::Constant(_) => ctx.types.intern_mut(TypeKind::Number),
        ExprKind::Call(def_id, args) => {
            let def_type = ctx.def_types.get(&def_id).unwrap();
            match def_type.kind() {
            TypeKind::Function { params, output } => {
                for arg in args {
                    type_check_expr(ctx, arg);
                }
                *output
            },
            _ => panic!("Not a function")
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
        env::{Env, Mem},
        expr::{ExprId, ExprKind},
        misc::{PackageId, SourceSpan}, types::TypeKind,
    };

    use super::{type_check_def, type_check_expr};

    #[test]
    fn test_type_check() {
        let mem = Mem::default();
        let mut env = Env::new(&mem);
        env.register_builtins();

        let span = SourceSpan::none();
        let m = env.add_def(
            PackageId(1),
            "m",
            DefKind::Constructor("m".into(), env.core_def_by_name("Number").unwrap()),
        );
        let m_ty = type_check_def(&mut env.def_type_check_ctx(), m);

        let args = vec![env.expr(ExprKind::Variable(ExprId(100)), span)];
        let expr = env.expr(
            ExprKind::Call(m, args),
            SourceSpan::none(),
        );

        let TypeKind::Function { params: _, output: m_output } = m_ty.kind() else {
            panic!();    
        };

        let ty = type_check_expr(&mut env.expr_type_check_ctx(), &expr);
        assert_eq!(*m_output, ty);
    }
}
