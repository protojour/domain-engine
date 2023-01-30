use smartstring::alias::String;

use crate::typed_expr::{NodeId, TypedExpr, TypedExprKind, TypedExprTable};

#[derive(Default, Debug)]
pub struct RewriteTable(Vec<NodeId>);

impl RewriteTable {
    pub fn push_node(&mut self, node: NodeId) {
        self.0.push(node);
    }

    /// Reset all rewrites
    pub fn reset(&mut self, size: usize) {
        self.0.truncate(size);
        for (idx, node) in self.0.iter_mut().enumerate() {
            node.0 = idx as u32;
        }
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

#[derive(Debug)]
pub enum RewriteError {
    UnhandledExpr(String),
    NoRulesMatchedCall,
    MultipleVariablesInCall,
    NoVariablesInCall,
}

pub fn rewrite(table: &mut TypedExprTable, node: NodeId) -> Result<(), RewriteError> {
    let expr = table.get_expr_no_rewrite(node);
    let mut expr_params: Vec<(NodeId, &TypedExpr)> = vec![];

    match &expr.kind {
        TypedExprKind::Call(proc, params) => {
            table.get_exprs(&table.source_rewrites, params, &mut expr_params);

            let mut var_param = None;

            for (index, (var_node, param)) in expr_params.iter().enumerate() {
                if let TypedExprKind::Variable(_) = &param.kind {
                    if var_param.is_some() {
                        return Err(RewriteError::MultipleVariablesInCall);
                    }
                    var_param = Some((*var_node, index));
                }
            }

            let (var_node, var_index) = match var_param {
                Some(var_param) => var_param,
                None => {
                    return Err(RewriteError::NoVariablesInCall);
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

                let expr_ty = expr.ty;

                let mut cloned_params: Vec<_> = rule
                    .1
                    .params()
                    .iter()
                    .map(|index| params[*index as usize])
                    .collect();

                // Make sure the expr node representing the variable
                // doesn't get recursively rewritten.
                // e.g. if the node `:v` gets rewritten to `(* :v 2)`,
                // we only want to do this once.
                // The left `:v` cannot be the same node as the right `:v`.
                let var_expr = table.get_expr_no_rewrite(var_node);
                let cloned_var_node = table.add_expr(var_expr.clone());
                cloned_params[var_index] = cloned_var_node;

                let target_expr = TypedExpr {
                    ty: expr_ty,
                    kind: TypedExprKind::Call(rule.1.proc(), cloned_params.into()),
                };
                let target_expr_id = table.add_expr(target_expr);

                // rewrites
                table.target_rewrites.rewrite(var_node, target_expr_id);
                table.source_rewrites.rewrite(node, var_node);

                println!("target rewrite: {node:?}->{target_expr_id:?}");
                println!("source rewrite: {node:?}->{var_node:?}");

                return Ok(());
            }

            Err(RewriteError::NoRulesMatchedCall)
        }
        TypedExprKind::ValueObj(value_node) => rewrite(table, *value_node),
        TypedExprKind::MapObj(property_map) => {
            let nodes: Vec<_> = property_map.iter().map(|(_, node)| *node).collect();
            for node in nodes {
                rewrite(table, node)?;
            }
            Ok(())
        }
        TypedExprKind::Variable(_) => Ok(()),
        kind => Err(RewriteError::UnhandledExpr(format!("{kind:?}").into())),
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

    use super::rewrite;
    use crate::{
        compiler::Compiler,
        mem::{Intern, Mem},
        typed_expr::{SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable},
        types::Type,
    };

    #[test]
    fn rewrite_test() {
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

        rewrite(&mut table, call).unwrap();

        println!("source: {}", table.debug_tree(&table.source_rewrites, call));
        println!("target: {}", table.debug_tree(&table.target_rewrites, call));
    }
}
