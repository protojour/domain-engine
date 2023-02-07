use ontol_runtime::{format_utils::Indent, proc::BuiltinProc, smart_format};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::typed_expr::{ExprRef, TypedExpr, TypedExprKind, TypedExprTable, TypedExpressions};

/// A rewrite table tracks rewrites of node indices.
///
/// Each node id in the vector is a pointer to some node.
/// If a node points to itself, it is not rewritten.
/// Rewrites are resolved by following pointers until
/// a "root" is found that is not rewritten.
///
/// Be careful to not introduce circular rewrites.
#[derive(Default, Debug)]
pub struct RewriteTable(SmallVec<[ExprRef; 32]>);

impl RewriteTable {
    pub fn push(&mut self) {
        let len = self.0.len();
        self.0.push(ExprRef(len as u32));
    }

    /// Register a rewrite.
    /// All occurences of `source` should be rewritten to `target`.
    pub fn rewrite(&mut self, source: ExprRef, target: ExprRef) {
        self.0[source.0 as usize] = target;
    }

    /// Resolve any expression to its rewritten form.
    pub fn resolve(&self, mut expr_ref: ExprRef) -> ExprRef {
        loop {
            let entry = self.0[expr_ref.0 as usize];
            if entry == expr_ref {
                return expr_ref;
            }

            expr_ref = entry;
        }
    }

    /// Reset all rewrites, so that nothing is rewritten.
    pub fn reset(&mut self, size: usize) {
        self.0.truncate(size);
        for (index, node) in self.0.iter_mut().enumerate() {
            node.0 = index as u32;
        }
    }
}

#[derive(Debug)]
#[allow(unused)]
pub enum RewriteError {
    UnhandledExpr(String),
    NoRulesMatchedCall,
    MultipleVariablesInCall,
    NoVariablesInCall,
}

pub enum RewriteResult {
    Variable(ExprRef),
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

    pub fn rewrite_expr(&mut self, expr_ref: ExprRef) -> Result<RewriteResult, RewriteError> {
        self.rewrite_expr_inner(expr_ref, Indent::default())
    }

    fn rewrite_expr_inner(
        &mut self,
        expr_ref: ExprRef,
        indent: Indent,
    ) -> Result<RewriteResult, RewriteError> {
        match &self.expressions[expr_ref].kind {
            TypedExprKind::Call(proc, params) => {
                let param_refs = params
                    .into_iter()
                    .map(|param_id| self.source_rewrites.resolve(*param_id))
                    .collect();

                self.rewrite_call(expr_ref, *proc, param_refs, indent)
            }
            TypedExprKind::ValueObjPattern(value_node) => {
                self.rewrite_expr_inner(*value_node, indent.inc())
            }
            TypedExprKind::MapObjPattern(property_map) => {
                let expr_refs: Vec<_> = property_map.iter().map(|(_, node)| *node).collect();
                for expr_ref in expr_refs {
                    self.rewrite_expr_inner(expr_ref, indent.inc())?;
                }
                Ok(RewriteResult::Constant)
            }
            TypedExprKind::Variable(_) => Ok(RewriteResult::Variable(expr_ref)),
            TypedExprKind::VariableRef(var_ref) => Ok(RewriteResult::Variable(*var_ref)),
            TypedExprKind::Constant(_) => Ok(RewriteResult::Constant),
            TypedExprKind::Translate(param_ref, param_ty) => {
                let param_ty = *param_ty;
                let param_ref = match self.rewrite_expr_inner(*param_ref, indent.inc())? {
                    RewriteResult::Variable(var_id) => var_id,
                    RewriteResult::Constant => return Ok(RewriteResult::Constant),
                };

                let (cloned_param_id, cloned_param) = self.clone_expr(param_ref);
                let cloned_param_span = cloned_param.span;
                let expr = &self.expressions[expr_ref];

                debug!(
                    "rewrite TypedExprKind::Call with span {:?}, param_id: {:?}",
                    expr.span, param_ref.0
                );

                // invert translation:
                let (inverted_translation_id, _) = self.add_expr(TypedExpr {
                    kind: TypedExprKind::Translate(cloned_param_id, expr.ty),
                    ty: param_ty,
                    span: cloned_param_span,
                });

                // remove the translation call from the source
                self.source_rewrites.rewrite(expr_ref, param_ref);
                // add inverted translation call to the target
                self.target_rewrites
                    .rewrite(param_ref, inverted_translation_id);

                Ok(RewriteResult::Variable(cloned_param_id))
            }
            kind => Err(RewriteError::UnhandledExpr(smart_format!("{kind:?}"))),
        }
    }

    fn rewrite_call(
        &mut self,
        expr_ref: ExprRef,
        proc: BuiltinProc,
        params: SmallVec<[ExprRef; 4]>,
        indent: Indent,
    ) -> Result<RewriteResult, RewriteError> {
        let params_len = params.len();
        let mut param_var_ref = None;

        for (index, param_id) in params.iter().enumerate() {
            match self.rewrite_expr_inner(*param_id, indent.inc())? {
                RewriteResult::Variable(rewritten_node) => {
                    if param_var_ref.is_some() {
                        return Err(RewriteError::MultipleVariablesInCall);
                    }
                    param_var_ref = Some((rewritten_node, index));
                }
                RewriteResult::Constant => {}
            }
        }

        debug!("{indent}rewrite proc {proc:?}");

        let (param_var_ref, var_index) = match param_var_ref {
            Some(rewritten) => rewritten,
            None => {
                debug!("{indent}proc {proc:?} was constant");
                return Ok(RewriteResult::Constant);
                // return Err(RewriteError::NoVariablesInCall);
            }
        };

        let expr = &self.expressions[expr_ref];
        let span = expr.span;

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

            // Make sure the expression representing the variable
            // doesn't get recursively rewritten.
            // e.g. if the expr `:v` gets rewritten to `(* :v 2)`,
            // we only want to do this once.
            // The left `:v` cannot be the same node as the right `:v`.
            let (cloned_var_id, _) = self.clone_expr(param_var_ref);
            cloned_params[var_index] = cloned_var_id;

            let target_expr = TypedExpr {
                kind: TypedExprKind::Call(rule.1.proc(), cloned_params.into()),
                ty: expr_ty,
                span,
            };
            let (target_expr_ref, _) = self.add_expr(target_expr);

            // rewrites
            self.source_rewrites.rewrite(expr_ref, param_var_ref);
            self.target_rewrites.rewrite(param_var_ref, target_expr_ref);

            debug!("{indent}source rewrite: {expr_ref:?}->{param_var_ref:?}");
            debug!("{indent}target rewrite: {param_var_ref:?}->{target_expr_ref:?} (new!)",);

            return Ok(RewriteResult::Variable(cloned_var_id));
        }

        Err(RewriteError::NoRulesMatchedCall)
    }

    fn add_expr(&mut self, expr: TypedExpr<'m>) -> (ExprRef, &TypedExpr<'m>) {
        let id = ExprRef(self.expressions.0.len() as u32);
        self.expressions.0.push(expr);
        self.source_rewrites.push();
        self.target_rewrites.push();
        (id, &self.expressions[id])
    }

    fn clone_expr(&mut self, expr_ref: ExprRef) -> (ExprRef, &TypedExpr<'m>) {
        let expr = &self.expressions[expr_ref];
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
        compiler::Compiler,
        mem::{Intern, Mem},
        typed_expr::{SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable},
        types::Type,
        SourceSpan,
    };

    #[test]
    fn rewrite_test() {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem).with_core();
        let int = compiler.types.intern(Type::Int(DefId(42)));

        let mut table = TypedExprTable::default();
        let var = table.add_expr(TypedExpr {
            kind: TypedExprKind::Variable(SyntaxVar(42)),
            ty: int,
            span: SourceSpan::none(),
        });
        let var_ref = table.add_expr(TypedExpr {
            kind: TypedExprKind::VariableRef(var),
            ty: int,
            span: SourceSpan::none(),
        });
        let constant = table.add_expr(TypedExpr {
            kind: TypedExprKind::Constant(1000),
            ty: int,
            span: SourceSpan::none(),
        });
        let call = table.add_expr(TypedExpr {
            kind: TypedExprKind::Call(BuiltinProc::Mul, [var_ref, constant].into()),
            ty: int,
            span: SourceSpan::none(),
        });

        table.rewriter().rewrite_expr(call).unwrap();

        info!("source: {}", table.debug_tree(&table.source_rewrites, call));
        info!("target: {}", table.debug_tree(&table.target_rewrites, call));
    }
}
