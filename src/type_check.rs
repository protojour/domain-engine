use crate::{
    def::{DefId, DefKind, Primitive},
    env::{Env, Intern},
    expr::{Expr, ExprKind},
    types::{Type, TypeKind},
};

struct Checker {}

fn type_check_def<'m>(env: &Env<'m>, def_id: DefId) -> Type<'m> {
    let defs = env.defs.borrow();
    match &defs.get(&def_id).unwrap().kind {
        DefKind::Constructor(_, arg_def) => {
            let arg_type = type_check_def(env, *arg_def);

            let fn_ty = env.intern(TypeKind::Function {
                args: env.intern([arg_type]),
                output: env.intern(TypeKind::New(def_id, arg_type)),
            });

            let mut def_types = env.def_types.borrow_mut();
            def_types.insert(def_id, fn_ty);

            fn_ty
        }
        DefKind::Primitive(Primitive::Number) => env.intern(TypeKind::Number),
        other => {
            panic!("failed def typecheck: {other:?}");
        }
    }
}

fn type_check_expr<'m>(env: &Env<'m>, expr: &Expr) -> Type<'m> {
    match &expr.kind {
        ExprKind::Constant(_) => env.intern(TypeKind::Number),
        ExprKind::Call(def_id, args) => match env.type_of(*def_id).kind() {
            TypeKind::Function { args, output } => *output,
            _ => panic!("Not a function"),
        },
        ExprKind::Variable(id) => {
            panic!()
        }
    }
}

impl<'m> Env<'m> {
    /// Type of a DefId of an already type checked package
    fn type_of(&self, def_id: DefId) -> Type<'m> {
        let def_types = self.def_types.borrow();
        *def_types.get(&def_id).unwrap()
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
        let m_ty = type_check_def(&env, m);
        let expr = env.expr(
            ExprKind::Call(m, vec![env.expr(ExprKind::Variable(ExprId(100)), span)]),
            SourceSpan::none(),
        );

        let TypeKind::Function { args: _, output: m_output } = m_ty.kind() else {
            panic!();    
        };

        let ty = type_check_expr(&env, &expr);
        assert_eq!(*m_output, ty);
    }
}
