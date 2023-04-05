use std::ops::{Index, IndexMut};

use ontol_runtime::{format_utils::Indent, proc::BuiltinProc, smart_format};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::typed_expr::{ExprRef, TypedExpr, TypedExprKind, TypedExprTable};

use super::equation::TypedExprEquation;

/// A substitution table tracks rewrites of node indices.
///
/// Each node id in the vector is a pointer to some node.
/// If a node points to itself, it is not substituted (i.e. substituted to itself).
/// Rewrites are resolved by following pointers until
/// a "root" is found that is not rewritten.
///
/// Be careful not to introduce circular rewrites.
#[derive(Default, Debug)]
pub struct SubstitutionTable(SmallVec<[ExprRef; 32]>);

impl SubstitutionTable {
    #[inline]
    pub fn push(&mut self) {
        let id = ExprRef(self.0.len() as u32);
        self.0.push(id);
    }

    /// Recursively resolve any expression to its final substituted form.
    pub fn resolve(&self, mut expr_ref: ExprRef) -> ExprRef {
        loop {
            let entry = self.0[expr_ref.0 as usize];
            if entry == expr_ref {
                return expr_ref;
            }

            expr_ref = entry;
        }
    }

    /// Reset all substitutions to original state
    pub fn reset(&mut self, size: usize) {
        self.0.truncate(size);
        for (index, node) in self.0.iter_mut().enumerate() {
            node.0 = index as u32;
        }
    }

    pub fn debug_table(&self) -> Vec<(u32, u32)> {
        let mut out = vec![];
        for i in 0..self.0.len() {
            let source = ExprRef(i as u32);
            let resolved = self.resolve(source);
            if resolved != source {
                out.push((source.0, resolved.0));
            }
        }

        out
    }
}

impl Index<ExprRef> for SubstitutionTable {
    type Output = ExprRef;

    fn index(&self, index: ExprRef) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

impl IndexMut<ExprRef> for SubstitutionTable {
    fn index_mut(&mut self, index: ExprRef) -> &mut Self::Output {
        &mut self.0[index.0 as usize]
    }
}

#[derive(Debug)]
#[allow(unused)]
pub enum SolveError {
    UnhandledExpr(String),
    NoRulesMatchedCall,
    MultipleVariablesInCall,
    NoVariablesInCall,
}

pub enum Substitution {
    Variable(ExprRef),
    Constant,
}

pub struct EquationSolver<'t, 'm> {
    /// The set of expressions that will be rewritten
    expressions: &'t mut TypedExprTable<'m>,
    /// substitutions for the reduced side
    reductions: &'t mut SubstitutionTable,
    /// substitutions for the expanded side
    expansions: &'t mut SubstitutionTable,
}

impl<'t, 'm> EquationSolver<'t, 'm> {
    pub fn new(equation: &'t mut TypedExprEquation<'m>) -> Self {
        Self {
            expressions: &mut equation.expressions,
            reductions: &mut equation.reductions,
            expansions: &mut equation.expansions,
        }
    }

    pub fn reduce_expr(&mut self, expr_ref: ExprRef) -> Result<Substitution, SolveError> {
        self.reduce_expr_inner(expr_ref, Indent::default())
    }

    fn reduce_expr_inner(
        &mut self,
        expr_ref: ExprRef,
        indent: Indent,
    ) -> Result<Substitution, SolveError> {
        match &self.expressions[expr_ref].kind {
            TypedExprKind::Call(proc, params) => {
                let param_refs = params
                    .into_iter()
                    .map(|param_id| self.reductions.resolve(*param_id))
                    .collect();

                self.reduce_call(expr_ref, *proc, param_refs, indent)
            }
            TypedExprKind::ValueObjPattern(value_node) => {
                self.reduce_expr_inner(*value_node, indent.inc())
            }
            TypedExprKind::MapObjPattern(property_map) => {
                let expr_refs: Vec<_> = property_map.iter().map(|(_, node)| *node).collect();
                for expr_ref in expr_refs {
                    self.reduce_expr_inner(expr_ref, indent.inc())?;
                }
                Ok(Substitution::Constant)
            }
            TypedExprKind::Variable(_) => Ok(Substitution::Variable(expr_ref)),
            TypedExprKind::VariableRef(var_ref) => Ok(Substitution::Variable(*var_ref)),
            TypedExprKind::Constant(_) => Ok(Substitution::Constant),
            TypedExprKind::Translate(param_ref, param_ty) => {
                let param_ty = *param_ty;
                let param_ref = match self.reduce_expr_inner(*param_ref, indent.inc())? {
                    Substitution::Variable(var_id) => var_id,
                    Substitution::Constant => return Ok(Substitution::Constant),
                };

                let (cloned_param_id, cloned_param) = self.clone_expr(param_ref);
                let cloned_param_span = cloned_param.span;
                let expr = &self.expressions[expr_ref];

                debug!(
                    "substitute TypedExprKind::Call with span {:?}, param_id: {:?}",
                    expr.span, param_ref.0
                );

                // invert translation:
                let (inverted_translation_id, _) = self.add_expr(TypedExpr {
                    kind: TypedExprKind::Translate(cloned_param_id, expr.ty),
                    ty: param_ty,
                    span: cloned_param_span,
                });

                // remove the translation call from the reductions
                self.reductions[expr_ref] = param_ref;
                // add inverted translation call to the expansions
                self.expansions[param_ref] = inverted_translation_id;

                Ok(Substitution::Variable(cloned_param_id))
            }
            TypedExprKind::SequenceMap(inner_node, _) => {
                self.reduce_expr_inner(*inner_node, indent.inc())
            }
            kind => Err(SolveError::UnhandledExpr(smart_format!("{kind:?}"))),
        }
    }

    fn reduce_call(
        &mut self,
        expr_ref: ExprRef,
        proc: BuiltinProc,
        params: SmallVec<[ExprRef; 4]>,
        indent: Indent,
    ) -> Result<Substitution, SolveError> {
        let params_len = params.len();
        let mut param_var_ref = None;

        for (index, param_id) in params.iter().enumerate() {
            match self.reduce_expr_inner(*param_id, indent.inc())? {
                Substitution::Variable(rewritten_node) => {
                    if param_var_ref.is_some() {
                        return Err(SolveError::MultipleVariablesInCall);
                    }
                    param_var_ref = Some((rewritten_node, index));
                }
                Substitution::Constant => {}
            }
        }

        debug!("{indent}reduce proc {proc:?}");

        let (param_var_ref, var_index) = match param_var_ref {
            Some(rewritten) => rewritten,
            None => {
                debug!("{indent}proc {proc:?} was constant");
                return Ok(Substitution::Constant);
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
            // doesn't get recursively substituted.
            // e.g. if the expr `:v` gets rewritten to `(* :v 2)`,
            // we only want to do this once.
            // The left `:v` cannot be the same node as the right `:v`.
            let (cloned_var_ref, _) = self.clone_expr(param_var_ref);
            cloned_params[var_index] = cloned_var_ref;

            let target_expr = TypedExpr {
                kind: TypedExprKind::Call(rule.1.proc(), cloned_params.into()),
                ty: expr_ty,
                span,
            };
            let (target_expr_ref, _) = self.add_expr(target_expr);

            // substitutions
            self.reductions[expr_ref] = param_var_ref;
            self.expansions[param_var_ref] = target_expr_ref;

            debug!("{indent}reduction subst: {expr_ref:?}->{param_var_ref:?}");
            debug!("{indent}expansion subst: {param_var_ref:?}->{target_expr_ref:?} (new!)");

            return Ok(Substitution::Variable(cloned_var_ref));
        }

        Err(SolveError::NoRulesMatchedCall)
    }

    fn add_expr(&mut self, expr: TypedExpr<'m>) -> (ExprRef, &TypedExpr<'m>) {
        let id = ExprRef(self.expressions.0.len() as u32);
        self.expressions.0.push(expr);
        self.reductions.push();
        self.expansions.push();
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
    pub struct Subst(BuiltinProc, &'static [u8]);

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

    impl Subst {
        pub fn proc(&self) -> BuiltinProc {
            self.0
        }

        pub fn params(&self) -> &[u8] {
            self.1
        }
    }

    pub static RULES: [(Pattern, Subst); 5] = [
        (
            Pattern(BuiltinProc::Add, &[Match::Number, Match::Number]),
            Subst(BuiltinProc::Sub, &[0, 1]),
        ),
        (
            Pattern(BuiltinProc::Sub, &[Match::Number, Match::Number]),
            Subst(BuiltinProc::Add, &[0, 1]),
        ),
        (
            Pattern(BuiltinProc::Mul, &[Match::Number, Match::NonZero]),
            Subst(BuiltinProc::Div, &[0, 1]),
        ),
        (
            Pattern(BuiltinProc::Mul, &[Match::NonZero, Match::Number]),
            Subst(BuiltinProc::Div, &[1, 0]),
        ),
        (
            Pattern(BuiltinProc::Div, &[Match::Number, Match::NonZero]),
            Subst(BuiltinProc::Mul, &[0, 1]),
        ),
    ];
}

#[cfg(test)]
mod tests {
    use ontol_runtime::{proc::BuiltinProc, DefId, PackageId};
    use tracing::info;

    use crate::{
        codegen::equation::TypedExprEquation,
        mem::{Intern, Mem},
        typed_expr::{SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable},
        types::Type,
        Compiler, SourceSpan, Sources,
    };

    #[test]
    fn test_solver() {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, Sources::default()).with_core();
        let int = compiler.types.intern(Type::Int(DefId(PackageId(0), 42)));

        let mut table = TypedExprTable::default();
        let var = table.add(TypedExpr {
            kind: TypedExprKind::Variable(SyntaxVar(42)),
            ty: int,
            span: SourceSpan::none(),
        });
        let var_ref = table.add(TypedExpr {
            kind: TypedExprKind::VariableRef(var),
            ty: int,
            span: SourceSpan::none(),
        });
        let constant = table.add(TypedExpr {
            kind: TypedExprKind::Constant(1000),
            ty: int,
            span: SourceSpan::none(),
        });
        let call = table.add(TypedExpr {
            kind: TypedExprKind::Call(BuiltinProc::Mul, [var_ref, constant].into()),
            ty: int,
            span: SourceSpan::none(),
        });

        let mut eq = TypedExprEquation::new(table);
        eq.solver().reduce_expr(call).unwrap();

        info!("source: {:?}", eq.debug_tree(call, &eq.reductions));
        info!("target: {:?}", eq.debug_tree(call, &eq.expansions));
    }
}
