use std::collections::HashMap;

use crate::{
    def::{DefId, DefKind, Primitive, Def},
    env::{Env, InternMut},
    expr::{Expr, ExprKind},
    types::{Type, TypeKind, Types},
};

struct Ctx<'e, 'm> {
    defs: &'e HashMap<DefId, Def>,
    types: &'e mut Types<'m>,
    def_types: &'e mut HashMap<DefId, Type<'m>>,
}

impl<'m> Env<'m> {
    fn type_check_ctx(&mut self) -> Ctx<'_, 'm> {
        Ctx {
            defs: &self.defs2,
            types: &mut self.types,
            def_types: &mut self.def_types
        }
    }
}

fn type_check_def<'e, 'm>(ctx: &mut Ctx<'e, 'm>, def_id: DefId) -> Type<'m> {
    match &ctx.defs.get(&def_id).unwrap().kind {
        DefKind::Constructor(_, arg_def) => {
            let arg_type = type_check_def(ctx, *arg_def);

            let args = ctx.types.intern_mut([arg_type]);
            let output = ctx.types.intern_mut(TypeKind::New(def_id, arg_type));
            let fn_ty = ctx.types.intern_mut(TypeKind::Function {
                args,
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

fn type_check_expr<'e, 'm>(ctx: &mut Ctx<'e, 'm>, expr: &Expr) -> Type<'m> {
    match &expr.kind {
        ExprKind::Constant(_) => ctx.types.intern_mut(TypeKind::Number),
        ExprKind::Call(def_id, args) => match ctx.def_types.get(&def_id).unwrap().kind() {
            TypeKind::Function { args, output } => *output,
            _ => panic!("Not a function"),
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
        let m_ty = type_check_def(&mut env.type_check_ctx(), m);
        let expr = env.expr(
            ExprKind::Call(m, vec![env.expr(ExprKind::Variable(ExprId(100)), span)]),
            SourceSpan::none(),
        );

        let TypeKind::Function { args: _, output: m_output } = m_ty.kind() else {
            panic!();    
        };

        let ty = type_check_expr(&mut env.type_check_ctx(), &expr);
        assert_eq!(*m_output, ty);
    }
}
