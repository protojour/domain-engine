use std::fmt::Debug;

use ontol_runtime::value::PropertyId;

use crate::{
    typed_expr::{BindDepth, ExprRef, SyntaxVar, TypedExpr, TypedExprKind, TypedExprTable},
    SourceSpan,
};

use super::equation_solver::{EquationSolver, SubstitutionTable};

/// Table used to store equation-like expression trees
/// suitable for rewriting.
#[derive(Default)]
pub struct TypedExprEquation<'m> {
    /// The set of expressions part of the equation
    pub expressions: TypedExprTable<'m>,
    /// Number of expressions
    original_expr_len: usize,
    /// Rewrites for the reduced side
    pub reductions: SubstitutionTable,
    /// Rewrites for the expanded side
    pub expansions: SubstitutionTable,
}

impl<'m> TypedExprEquation<'m> {
    pub fn new(expressions: TypedExprTable<'m>) -> Self {
        let len = expressions.0.len();
        let mut equation = Self {
            expressions,
            original_expr_len: len,
            reductions: Default::default(),
            expansions: Default::default(),
        };
        for _ in 0..len {
            equation.reductions.push();
            equation.expansions.push();
        }
        equation.make_default_substituions();
        equation
    }

    /// Create a new solver
    pub fn solver<'e>(&'e mut self) -> EquationSolver<'e, 'm> {
        EquationSolver::new(self)
    }

    #[inline]
    pub fn resolve_expr<'e>(
        &'e self,
        substitutions: &SubstitutionTable,
        source_node: ExprRef,
    ) -> (ExprRef, &'e TypedExpr<'m>, SourceSpan) {
        let span = self.expressions[source_node].span;
        let root_node = substitutions.resolve(source_node);
        (root_node, &self.expressions[root_node], span)
    }

    /// Reset all generated expressions and substitutions
    pub fn reset(&mut self) {
        self.expressions.0.truncate(self.original_expr_len);
        self.reductions.reset(self.original_expr_len);
        self.expansions.reset(self.original_expr_len);
        self.make_default_substituions()
    }

    fn make_default_substituions(&mut self) {
        for (index, expr) in self.expressions.0.iter().enumerate() {
            if let TypedExprKind::VariableRef(var_ref) = &expr.kind {
                self.reductions[ExprRef(index as u32)] = *var_ref;
                self.expansions[ExprRef(index as u32)] = *var_ref;
            }
        }
    }

    pub fn debug_tree<'e>(
        &'e self,
        expr_ref: ExprRef,
        substitutions: &'e SubstitutionTable,
    ) -> DebugTree<'e, 'm> {
        DebugTree {
            equation: self,
            substitutions,
            expr_ref,
            property_id: None,
            depth: 0,
        }
    }
}

impl<'m> Debug for TypedExprEquation<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedExprEquation").finish()
    }
}

pub struct DebugTree<'a, 'm> {
    equation: &'a TypedExprEquation<'m>,
    substitutions: &'a SubstitutionTable,
    expr_ref: ExprRef,
    property_id: Option<PropertyId>,
    depth: usize,
}

impl<'a, 'm> DebugTree<'a, 'm> {
    fn child(&self, expr_ref: ExprRef, property_id: Option<PropertyId>) -> Self {
        Self {
            equation: self.equation,
            substitutions: self.substitutions,
            expr_ref,
            property_id,
            depth: self.depth + 1,
        }
    }

    fn header(&self, name: &str, resolved: ExprRef) -> String {
        use std::fmt::Write;
        let mut s = String::new();

        if self.expr_ref != resolved {
            write!(&mut s, "{{{}->{}}}", self.expr_ref.0, resolved.0).unwrap();
        } else {
            write!(&mut s, "{{{}}}", self.expr_ref.0).unwrap();
        }

        if let Some(property_id) = &self.property_id {
            let def_id = &property_id.relation_id.0;
            write!(&mut s, " (rel {}, {}) ", def_id.0 .0, def_id.1).unwrap();
        }

        write!(&mut s, "{}", name).unwrap();

        s
    }
}

impl<'a, 'm> Debug for DebugTree<'a, 'm> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.depth > 20 {
            return write!(f, "[ERROR depth exceeded]");
        }

        let (resolved, expr, _) = self
            .equation
            .resolve_expr(self.substitutions, self.expr_ref);

        match &expr.kind {
            TypedExprKind::Unit => f.debug_tuple(&self.header("Unit", resolved)).finish()?,
            TypedExprKind::Call(proc, params) => {
                let mut tup = f.debug_tuple(&self.header(&format!("{proc:?}"), resolved));
                for param in params {
                    tup.field(&self.child(*param, None));
                }
                tup.finish()?
            }
            TypedExprKind::ValueObjPattern(expr_ref) => f
                .debug_tuple(&self.header("ValueObj", resolved))
                .field(&self.child(*expr_ref, None))
                .finish()?,
            TypedExprKind::MapObjPattern(attributes) => {
                let mut tup = f.debug_tuple(&self.header("MapObj", resolved));
                for (property_id, expr_ref) in attributes {
                    tup.field(&self.child(*expr_ref, Some(*property_id)));
                }
                tup.finish()?;
            }
            TypedExprKind::Constant(c) => f
                .debug_tuple(&self.header(&format!("Constant({c})"), resolved))
                .finish()?,
            TypedExprKind::Variable(SyntaxVar(v, BindDepth(d))) => f
                .debug_tuple(&self.header(&format!("Variable({v} d={d})"), resolved))
                .finish()?,
            TypedExprKind::VariableRef(var_ref) => f
                .debug_tuple(&self.header("VarRef", resolved))
                .field(&self.child(*var_ref, None))
                .finish()?,
            TypedExprKind::Translate(expr_ref, _) => f
                .debug_tuple(&self.header("Translate", resolved))
                .field(&self.child(*expr_ref, None))
                .finish()?,
            TypedExprKind::SequenceMap(expr_ref, var, body, _) => f
                .debug_tuple(&self.header("SequenceMap", resolved))
                .field(&self.child(*expr_ref, None))
                .field(&var)
                .field(&self.child(*body, None))
                .finish()?,
        };

        Ok(())
    }
}
