use ontol_runtime::{format_utils::Indent, proc::BuiltinProc, smart_format};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use super::typed_expr::{NodeId, TypedExpr, TypedExprKind, TypedExprTable, TypedExpressions};

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

pub enum RewriteResult {
    Variable(NodeId),
    Constant,
}

pub struct Rewriter<'t, 'm> {
    expressions: &'t mut TypedExpressions<'m>,
    source_rewrites: &'t mut RewriteTable,
    target_rewrites: &'t mut RewriteTable,
}

impl<'t, 'm> Rewriter<'t, 'm> {
    pub fn new(table: &'t mut TypedExprTable<'m>) -> Self {
        Self {
            expressions: &mut table.expressions,
            source_rewrites: &mut table.source_rewrites,
            target_rewrites: &mut table.target_rewrites,
        }
    }

    pub fn rewrite_expr(&mut self, node: NodeId) -> Result<RewriteResult, RewriteError> {
        self.rewrite_expr_inner(node, Indent::default())
    }

    fn rewrite_expr_inner(
        &mut self,
        node_id: NodeId,
        indent: Indent,
    ) -> Result<RewriteResult, RewriteError> {
        match &self.expressions[node_id].kind {
            TypedExprKind::Call(proc, params) => {
                let params_ids = params
                    .into_iter()
                    .map(|param_id| self.source_rewrites.find_root(*param_id))
                    .collect();

                self.rewrite_call(node_id, *proc, params_ids, indent)
            }
            TypedExprKind::ValueObj(value_node) => {
                self.rewrite_expr_inner(*value_node, indent.inc())
            }
            TypedExprKind::MapObj(property_map) => {
                let nodes: Vec<_> = property_map.iter().map(|(_, node)| *node).collect();
                for node in nodes {
                    self.rewrite_expr_inner(node, indent.inc())?;
                }
                Ok(RewriteResult::Constant)
            }
            TypedExprKind::Variable(_) => Ok(RewriteResult::Variable(node_id)),
            TypedExprKind::Constant(_) => Ok(RewriteResult::Constant),
            TypedExprKind::Translate(param_id, param_ty) => {
                let param_ty = *param_ty;
                let param_id = match self.rewrite_expr_inner(*param_id, indent.inc())? {
                    RewriteResult::Variable(var_id) => var_id,
                    RewriteResult::Constant => return Ok(RewriteResult::Constant),
                };

                let cloned_param_id = self.clone_expr(param_id);

                // invert translation:
                let inverted_translation = TypedExpr {
                    ty: param_ty,
                    kind: TypedExprKind::Translate(cloned_param_id, self.expressions[node_id].ty),
                };
                let inverted_translation_id = self.add_expr(inverted_translation);

                // remove the translation call from the source
                self.source_rewrites.rewrite(node_id, param_id);
                // add inverted translation call to the target
                self.target_rewrites
                    .rewrite(param_id, inverted_translation_id);

                Ok(RewriteResult::Variable(cloned_param_id))
            }
            kind => Err(RewriteError::UnhandledExpr(smart_format!("{kind:?}"))),
        }
    }

    fn rewrite_call(
        &mut self,
        node_id: NodeId,
        proc: BuiltinProc,
        params: SmallVec<[NodeId; 4]>,
        indent: Indent,
    ) -> Result<RewriteResult, RewriteError> {
        let params_len = params.len();
        let mut param_var_id = None;

        for (index, param_id) in params.iter().enumerate() {
            match self.rewrite_expr_inner(*param_id, indent.inc())? {
                RewriteResult::Variable(rewritten_node) => {
                    if param_var_id.is_some() {
                        return Err(RewriteError::MultipleVariablesInCall);
                    }
                    param_var_id = Some((rewritten_node, index));
                }
                RewriteResult::Constant => {}
            }
        }

        debug!("{indent}rewrite proc {proc:?}");

        let (param_var_id, var_index) = match param_var_id {
            Some(rewritten) => rewritten,
            None => {
                debug!("{indent}proc {proc:?} was constant");
                return Ok(RewriteResult::Constant);
                // return Err(RewriteError::NoVariablesInCall);
            }
        };

        let expr = &self.expressions[node_id];

        for rule in &rules::RULES {
            if rule.0.proc() != proc || rule.0.params().len() != params_len {
                continue;
            }

            for (_index, (param_id, _rule_param)) in params.iter().zip(rule.0.params()).enumerate()
            {
                let _param_expr = &self.expressions[*param_id];
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
            let cloned_var_id = self.clone_expr(param_var_id);
            cloned_params[var_index] = cloned_var_id;

            let target_expr = TypedExpr {
                ty: expr_ty,
                kind: TypedExprKind::Call(rule.1.proc(), cloned_params.into()),
            };
            let target_expr_id = self.add_expr(target_expr);

            // rewrites
            self.source_rewrites.rewrite(node_id, param_var_id);
            self.target_rewrites.rewrite(param_var_id, target_expr_id);

            debug!("{indent}source rewrite: {node_id:?}->{param_var_id:?}");
            debug!("{indent}target rewrite: {param_var_id:?}->{target_expr_id:?} (new!)",);

            return Ok(RewriteResult::Variable(cloned_var_id));
        }

        Err(RewriteError::NoRulesMatchedCall)
    }

    fn add_expr(&mut self, expr: TypedExpr<'m>) -> NodeId {
        let id = NodeId(self.expressions.0.len() as u32);
        self.expressions.0.push(expr);
        self.source_rewrites.push_node(id);
        self.target_rewrites.push_node(id);
        id
    }

    fn clone_expr(&mut self, node_id: NodeId) -> NodeId {
        let expr = &self.expressions[node_id];
        self.add_expr(expr.clone())
    }
}

mod rules {
    use ontol_runtime::proc::BuiltinProc;

    pub struct Pattern(BuiltinProc, &'static [Match]);
    pub struct Rewrite(BuiltinProc, &'static [u8]);

    pub enum Match {
        Number,
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

    pub static RULES: [(Pattern, Rewrite); 5] = [
        (
            Pattern(BuiltinProc::Add, &[Match::Number, Match::Number]),
            Rewrite(BuiltinProc::Sub, &[0, 1]),
        ),
        (
            Pattern(BuiltinProc::Sub, &[Match::Number, Match::Number]),
            Rewrite(BuiltinProc::Add, &[0, 1]),
        ),
        (
            Pattern(BuiltinProc::Mul, &[Match::Number, Match::NonZero]),
            Rewrite(BuiltinProc::Div, &[0, 1]),
        ),
        (
            Pattern(BuiltinProc::Mul, &[Match::NonZero, Match::Number]),
            Rewrite(BuiltinProc::Div, &[1, 0]),
        ),
        (
            Pattern(BuiltinProc::Div, &[Match::Number, Match::NonZero]),
            Rewrite(BuiltinProc::Mul, &[0, 1]),
        ),
    ];
}

#[cfg(test)]
mod tests {
    use ontol_runtime::{proc::BuiltinProc, DefId};
    use tracing::info;

    use crate::{
        codegen::typed_expr::{SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable},
        compiler::Compiler,
        mem::{Intern, Mem},
        types::Type,
    };

    #[test]
    fn rewrite_test() {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem).with_core();
        let int = compiler.types.intern(Type::Int(DefId(42)));

        let mut table = TypedExprTable::default();
        let var = table.add_expr(TypedExpr {
            ty: int,
            kind: TypedExprKind::Variable(SyntaxVar(42)),
        });
        let constant = table.add_expr(TypedExpr {
            ty: int,
            kind: TypedExprKind::Constant(1000),
        });
        let call = table.add_expr(TypedExpr {
            ty: int,
            kind: TypedExprKind::Call(BuiltinProc::Mul, [var, constant].into()),
        });

        table.rewriter().rewrite_expr(call).unwrap();

        info!("source: {}", table.debug_tree(&table.source_rewrites, call));
        info!("target: {}", table.debug_tree(&table.target_rewrites, call));
    }
}
