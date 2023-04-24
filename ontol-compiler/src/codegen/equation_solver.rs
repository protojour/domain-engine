use std::ops::{Index, IndexMut};

use ontol_runtime::{format_utils::Indent, proc::BuiltinProc};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::hir_node::{HirIdx, HirKind, HirNode, HirNodeTable};

use super::equation::HirEquation;

/// A substitution table tracks rewrites of node indices.
///
/// Each node id in the vector is a pointer to some node.
/// If a node points to itself, it is not substituted (i.e. substituted to itself).
/// Rewrites are resolved by following pointers until
/// a "root" is found that is not rewritten.
///
/// Be careful not to introduce circular rewrites.
#[derive(Default, Debug)]
pub struct SubstitutionTable(SmallVec<[HirIdx; 32]>);

impl SubstitutionTable {
    #[inline]
    pub fn push(&mut self) {
        let id = HirIdx(self.0.len() as u32);
        self.0.push(id);
    }

    /// Recursively resolve any node to its final substituted form.
    pub fn resolve(&self, mut node_id: HirIdx) -> HirIdx {
        loop {
            let entry = self.0[node_id.0 as usize];
            if entry == node_id {
                return node_id;
            }

            node_id = entry;
        }
    }

    pub fn resolve_once(&self, node_id: HirIdx) -> Option<HirIdx> {
        let entry = self.0[node_id.0 as usize];
        if entry == node_id {
            return None;
        }

        Some(entry)
    }

    /// Reset all substitutions to original state
    pub fn reset(&mut self, size: usize) {
        self.0.truncate(size);
        for (index, node) in self.0.iter_mut().enumerate() {
            node.0 = index as u32;
        }
    }

    #[allow(unused)]
    pub fn debug_table(&self) -> Vec<(u32, u32)> {
        let mut out = vec![];
        for i in 0..self.0.len() {
            let source = HirIdx(i as u32);
            let resolved = self.resolve(source);
            if resolved != source {
                out.push((source.0, resolved.0));
            }
        }

        out
    }
}

impl Index<HirIdx> for SubstitutionTable {
    type Output = HirIdx;

    fn index(&self, index: HirIdx) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

impl IndexMut<HirIdx> for SubstitutionTable {
    fn index_mut(&mut self, index: HirIdx) -> &mut Self::Output {
        &mut self.0[index.0 as usize]
    }
}

#[derive(Debug)]
#[allow(unused)]
pub enum SolveError {
    UnhandledNode(String),
    NoRulesMatchedCall,
    MultipleVariablesInCall,
    NoVariablesInCall,
}

pub enum Substitution {
    Variable(HirIdx),
    Constant(HirIdx),
}

pub struct EquationSolver<'t, 'm> {
    /// The set of nodes that will be rewritten
    nodes: &'t mut HirNodeTable<'m>,
    /// substitutions for the reduced side
    reductions: &'t mut SubstitutionTable,
    /// substitutions for the expanded side
    expansions: &'t mut SubstitutionTable,
}

impl<'t, 'm> EquationSolver<'t, 'm> {
    pub fn new(equation: &'t mut HirEquation<'m>) -> Self {
        Self {
            nodes: &mut equation.nodes,
            reductions: &mut equation.reductions,
            expansions: &mut equation.expansions,
        }
    }

    pub fn reduce_node(&mut self, node_id: HirIdx) -> Result<Substitution, SolveError> {
        self.reduce_node_inner(node_id, Indent::default())
    }

    fn reduce_node_inner(
        &mut self,
        idx: HirIdx,
        indent: Indent,
    ) -> Result<Substitution, SolveError> {
        debug!("{indent}(enter) reduce node {:?}", self.nodes[idx].kind);

        match &self.nodes[idx].kind {
            HirKind::Unit => todo!(),
            HirKind::Call(proc, params) => {
                let param_idxs = params
                    .into_iter()
                    .map(|param_idx| self.reductions.resolve(*param_idx))
                    .collect();

                self.reduce_call(idx, *proc, param_idxs, indent)
            }
            HirKind::ValuePattern(value_idx) => self.reduce_node_inner(*value_idx, indent.inc()),
            HirKind::StructPattern(property_map) => {
                let prop_node_idxs: Vec<_> =
                    property_map.iter().map(|(_, node_idx)| *node_idx).collect();
                for prop_node_idx in prop_node_idxs {
                    self.reduce_node_inner(prop_node_idx, indent.inc())?;
                }
                Ok(Substitution::Constant(idx))
            }
            HirKind::Variable(_) => Ok(Substitution::Variable(idx)),
            HirKind::VariableRef(var_idx) => Ok(Substitution::Variable(*var_idx)),
            HirKind::Constant(_) => Ok(Substitution::Constant(idx)),
            HirKind::MapCall(param_idx, param_ty) => {
                let param_ty = *param_ty;
                let param_idx = match self.reduce_node_inner(*param_idx, indent.inc())? {
                    Substitution::Variable(idx) => idx,
                    Substitution::Constant(idx) => idx,
                };

                let (cloned_param_id, cloned_param) = self.clone_node(param_idx, indent);
                let cloned_param_span = cloned_param.span;
                let node = &self.nodes[idx];

                debug!(
                    "{indent}substitute IrKind::MapCall with span {:?}, param_id: {:?}",
                    node.span, param_idx.0
                );

                // invert mappping:
                let (inverted_map_idx, _) = self.add_node(
                    HirNode {
                        kind: HirKind::MapCall(cloned_param_id, node.ty),
                        ty: param_ty,
                        span: cloned_param_span,
                    },
                    indent,
                );

                // remove the map call from the reductions
                self.reductions[idx] = param_idx;
                // add inverted map call to the expansions
                self.expansions[param_idx] = inverted_map_idx;

                debug!("{indent}reduction subst: {idx:?}->{param_idx:?}");
                debug!("{indent}expansion subst: {param_idx:?}->{inverted_map_idx:?} (new!)");

                Ok(Substitution::Variable(param_idx))
            }
            HirKind::Aggr(aggr_var_idx, _body_idx) => {
                self.reductions[idx] = *aggr_var_idx;

                Ok(Substitution::Constant(idx))
            }
            HirKind::MapSequence(seq_idx, iter_var, elem_idx, item_ty) => {
                let seq_idx = *seq_idx;
                let iter_var = *iter_var;
                let elem_idx = *elem_idx;
                let item_ty = *item_ty;

                let elem_idx_copy = elem_idx;

                let elem_idx = match self.reduce_node_inner(elem_idx, indent.inc())? {
                    Substitution::Variable(idx) => idx,
                    Substitution::Constant(idx) => idx,
                };

                debug!(
                    "{indent}MapSequence elem_id(before -> after): {elem_idx_copy:?}->{elem_idx:?}"
                );

                // Escape recursive expansion of the sequence reference:
                let seq_var = self.reductions.resolve(seq_idx);
                let (cloned_seq_var, _) = self.clone_node(seq_var, indent);

                debug!("{indent}seq_id->seq_var: {seq_idx:?}->{seq_var:?}");

                // let (cloned_item_id, cloned_item) = self.clone_node(elem_id);
                // let cloned_item_span = cloned_item.span;
                let node = &self.nodes[idx];
                let (inverted_map_idx, _) = self.add_node(
                    HirNode {
                        kind: HirKind::MapSequence(cloned_seq_var, iter_var, elem_idx, item_ty),
                        ty: node.ty,
                        span: node.span,
                    },
                    indent,
                );

                // remove the map call from the reductions
                self.reductions[idx] = seq_idx;
                // add inverted map call to the expansions
                self.expansions[seq_var] = inverted_map_idx;

                debug!("{indent}reduction subst: {idx:?}->{seq_idx:?}");
                debug!("{indent}expansion subst: {seq_var:?}->{inverted_map_idx:?} (new!)");

                Ok(Substitution::Variable(inverted_map_idx))
            }
        }
    }

    fn reduce_call(
        &mut self,
        idx: HirIdx,
        proc: BuiltinProc,
        params: SmallVec<[HirIdx; 4]>,
        indent: Indent,
    ) -> Result<Substitution, SolveError> {
        let params_len = params.len();
        let mut param_var_idx = None;

        for (index, param_idx) in params.iter().enumerate() {
            match self.reduce_node_inner(*param_idx, indent.inc())? {
                Substitution::Variable(rewritten_node) => {
                    if param_var_idx.is_some() {
                        return Err(SolveError::MultipleVariablesInCall);
                    }
                    param_var_idx = Some((rewritten_node, index));
                }
                Substitution::Constant(_) => {}
            }
        }

        debug!("{indent}reduce proc {proc:?}");

        let (param_var_idx, var_index) = match param_var_idx {
            Some(rewritten) => rewritten,
            None => {
                debug!("{indent}proc {proc:?} was constant");
                return Ok(Substitution::Constant(idx));
            }
        };

        let node = &self.nodes[idx];
        let span = node.span;

        for rule in &rules::RULES {
            if rule.0.proc() != proc || rule.0.params().len() != params_len {
                continue;
            }

            for (_index, (param_idx, _rule_param)) in params.iter().zip(rule.0.params()).enumerate()
            {
                let _param_node = &self.nodes[*param_idx];
                // TODO: Check constraints
            }

            let node_ty = node.ty;

            let mut cloned_params: Vec<_> = rule
                .1
                .params()
                .iter()
                .map(|index| params[*index as usize])
                .collect();

            // Make sure the node representing the variable
            // doesn't get recursively substituted.
            // e.g. if the node `:v` gets rewritten to `(* :v 2)`,
            // we only want to do this once.
            // The left `:v` cannot be the same node as the right `:v`.
            let (cloned_var_idx, _) = self.clone_node(param_var_idx, indent);
            cloned_params[var_index] = cloned_var_idx;

            let target_node = HirNode {
                kind: HirKind::Call(rule.1.proc(), cloned_params.into()),
                ty: node_ty,
                span,
            };
            let (target_node_idx, _) = self.add_node(target_node, indent);

            // substitutions
            self.reductions[idx] = param_var_idx;
            self.expansions[param_var_idx] = target_node_idx;

            debug!("{indent}reduction subst: {idx:?}->{param_var_idx:?}");
            debug!("{indent}expansion subst: {param_var_idx:?}->{target_node_idx:?} (new!)");

            return Ok(Substitution::Variable(cloned_var_idx));
        }

        Err(SolveError::NoRulesMatchedCall)
    }

    fn clone_node(&mut self, idx: HirIdx, indent: Indent) -> (HirIdx, &HirNode<'m>) {
        let node = &self.nodes[idx];
        self.add_node(node.clone(), indent)
    }

    fn add_node(&mut self, node: HirNode<'m>, indent: Indent) -> (HirIdx, &HirNode<'m>) {
        let idx = HirIdx(self.nodes.0.len() as u32);
        self.nodes.0.push(node);
        self.reductions.push();
        self.expansions.push();
        debug!("{indent}add_node: {idx:?} => {:?}", self.nodes[idx]);
        (idx, &self.nodes[idx])
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
    use test_log::test;
    use tracing::info;

    use crate::{
        codegen::equation::HirEquation,
        hir_node::{BindDepth, HirKind, HirNode, HirNodeTable, HirVariable},
        mem::{Intern, Mem},
        types::Type,
        Compiler, SourceSpan, Sources,
    };

    #[test]
    fn test_solver() {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, Sources::default()).with_core();
        let int = compiler.types.intern(Type::Int(DefId(PackageId(0), 42)));

        let mut table = HirNodeTable::default();
        let var = table.add(HirNode {
            kind: HirKind::Variable(HirVariable(42, BindDepth(0))),
            ty: int,
            span: SourceSpan::none(),
        });
        let var_ref = table.add(HirNode {
            kind: HirKind::VariableRef(var),
            ty: int,
            span: SourceSpan::none(),
        });
        let constant = table.add(HirNode {
            kind: HirKind::Constant(1000),
            ty: int,
            span: SourceSpan::none(),
        });
        let call = table.add(HirNode {
            kind: HirKind::Call(BuiltinProc::Mul, [var_ref, constant].into()),
            ty: int,
            span: SourceSpan::none(),
        });

        let mut eq = HirEquation::new(table);
        eq.solver().reduce_node(call).unwrap();

        info!("source: {:#?}", eq.debug_tree(call, &eq.reductions));
        info!("target: {:#?}", eq.debug_tree(call, &eq.expansions));
    }
}
