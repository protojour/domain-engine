use crate::typed_expr::{NodeId, TypedExpr, TypedExprKind, TypedExprTable};

#[derive(Default)]
pub struct RewriteTable(Vec<NodeId>);

impl RewriteTable {
    pub fn push_node(&mut self, node: NodeId) {
        self.0.push(node);
    }

    pub fn rewrite(&mut self, source: NodeId, target: NodeId) {
        self.0[source.0 as usize] = target;
    }

    pub fn find_root(&self, mut node_id: NodeId) -> NodeId {
        loop {
            let entry = self.0[node_id.0 as usize];
            if entry == node_id {
                return node_id;
            }

            node_id = entry;
        }
    }
}

fn rewrite(table: &mut TypedExprTable, node: NodeId) -> bool {
    let expr = table.expr_norewrite(node);
    let mut expr_params: Vec<(NodeId, &TypedExpr)> = vec![];
    match &expr.kind {
        TypedExprKind::Call(proc, params) => {
            table.fetch_exprs(&table.source_rewrites, params, &mut expr_params);

            let mut var_param = None;

            for (index, (var_node, param)) in expr_params.iter().enumerate() {
                if let TypedExprKind::Variable(_) = &param.kind {
                    if var_param.is_some() {
                        return false;
                    }
                    var_param = Some((*var_node, index));
                }
            }

            let (var_node, var_index) = match var_param {
                Some(var_param) => var_param,
                None => {
                    return false;
                }
            };

            for rule in &rules::RULES {
                if rule.0.proc() != *proc || rule.0.params().len() != params.len() {
                    continue;
                }

                for (index, ((node_id, expr_param), rule_param)) in
                    expr_params.iter().zip(rule.0.params()).enumerate()
                {
                    // TODO: Check constraints
                }

                let target_expr = TypedExpr {
                    ty: expr.ty,
                    kind: TypedExprKind::Call(
                        rule.1.proc(),
                        rule.1
                            .params()
                            .iter()
                            .map(|index| params[*index as usize])
                            .collect(),
                    ),
                };
                let target_expr_id = table.add_expr(target_expr);

                // rewrites
                table.target_rewrites.rewrite(node, target_expr_id);
                table.source_rewrites.rewrite(node, var_node);

                return true;
            }

            false
        }
        _ => false,
    }
}

mod rules {
    use ontol_runtime::vm::BuiltinProc;

    pub struct Pattern(BuiltinProc, &'static [Match]);
    pub struct Rewrite(BuiltinProc, &'static [u8]);

    pub enum Match {
        Any,
        NonZero,
    }

    impl Pattern {
        pub fn proc(&self) -> BuiltinProc {
            self.0
        }

        pub fn params(&self) -> &[Match] {
            self.1
        }
    }

    impl Rewrite {
        pub fn proc(&self) -> BuiltinProc {
            self.0
        }

        pub fn params(&self) -> &[u8] {
            self.1
        }
    }

    pub static RULES: [(Pattern, Rewrite); 3] = [
        (
            Pattern(BuiltinProc::Add, &[Match::Any, Match::Any]),
            Rewrite(BuiltinProc::Sub, &[0, 1]),
        ),
        (
            Pattern(BuiltinProc::Mul, &[Match::Any, Match::NonZero]),
            Rewrite(BuiltinProc::Div, &[0, 1]),
        ),
        (
            Pattern(BuiltinProc::Mul, &[Match::NonZero, Match::Any]),
            Rewrite(BuiltinProc::Div, &[1, 0]),
        ),
    ];
}

#[cfg(test)]
mod tests {
    use ontol_runtime::vm::BuiltinProc;

    use super::{rewrite, RewriteTable};
    use crate::{
        compiler::Compiler,
        mem::{Intern, Mem},
        typed_expr::{NodeId, SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable},
        types::Type,
    };

    #[test]
    fn eh() {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem).with_core();
        let number = compiler.types.intern(Type::Number);

        let mut table = TypedExprTable::default();
        let var = table.add_expr(TypedExpr {
            ty: number,
            kind: TypedExprKind::Variable(SyntaxVar(42)),
        });
        let constant = table.add_expr(TypedExpr {
            ty: number,
            kind: TypedExprKind::Constant(1000),
        });
        let call = table.add_expr(TypedExpr {
            ty: number,
            kind: TypedExprKind::Call(BuiltinProc::Mul, [var, constant].into()),
        });

        rewrite(&mut table, call);

        println!(
            "source: {}",
            print_tree(&table, &table.source_rewrites, call)
        );
        println!(
            "target: {}",
            print_tree(&table, &table.target_rewrites, call)
        );
    }

    fn print_tree(table: &TypedExprTable, rewrites: &RewriteTable, id: NodeId) -> String {
        let (_, expr) = table.fetch_expr(rewrites, id);
        match &expr.kind {
            TypedExprKind::Call(proc, params) => {
                let param_strings = params
                    .iter()
                    .map(|id| print_tree(table, rewrites, *id))
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("({proc:?} {param_strings})")
            }
            TypedExprKind::Constant(c) => format!("{c}"),
            TypedExprKind::Variable(SyntaxVar(v)) => format!(":{v}"),
        }
    }
}
