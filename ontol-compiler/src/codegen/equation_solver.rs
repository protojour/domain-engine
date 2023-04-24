use std::ops::{Index, IndexMut};

use ontol_runtime::{format_utils::Indent, proc::BuiltinProc};
use smallvec::SmallVec;
use smartstring::alias::String;
use tracing::debug;

use crate::ir_node::{IrKind, IrNode, IrNodeId, IrNodeTable};

use super::equation::IrNodeEquation;

/// A substitution table tracks rewrites of node indices.
///
/// Each node id in the vector is a pointer to some node.
/// If a node points to itself, it is not substituted (i.e. substituted to itself).
/// Rewrites are resolved by following pointers until
/// a "root" is found that is not rewritten.
///
/// Be careful not to introduce circular rewrites.
#[derive(Default, Debug)]
pub struct SubstitutionTable(SmallVec<[IrNodeId; 32]>);

impl SubstitutionTable {
    #[inline]
    pub fn push(&mut self) {
        let id = IrNodeId(self.0.len() as u32);
        self.0.push(id);
    }

    /// Recursively resolve any node to its final substituted form.
    pub fn resolve(&self, mut node_id: IrNodeId) -> IrNodeId {
        loop {
            let entry = self.0[node_id.0 as usize];
            if entry == node_id {
                return node_id;
            }

            node_id = entry;
        }
    }

    pub fn resolve_once(&self, node_id: IrNodeId) -> Option<IrNodeId> {
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
            let source = IrNodeId(i as u32);
            let resolved = self.resolve(source);
            if resolved != source {
                out.push((source.0, resolved.0));
            }
        }

        out
    }
}

impl Index<IrNodeId> for SubstitutionTable {
    type Output = IrNodeId;

    fn index(&self, index: IrNodeId) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

impl IndexMut<IrNodeId> for SubstitutionTable {
    fn index_mut(&mut self, index: IrNodeId) -> &mut Self::Output {
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
    Variable(IrNodeId),
    Constant(IrNodeId),
}

pub struct EquationSolver<'t, 'm> {
    /// The set of nodes that will be rewritten
    nodes: &'t mut IrNodeTable<'m>,
    /// substitutions for the reduced side
    reductions: &'t mut SubstitutionTable,
    /// substitutions for the expanded side
    expansions: &'t mut SubstitutionTable,
}

impl<'t, 'm> EquationSolver<'t, 'm> {
    pub fn new(equation: &'t mut IrNodeEquation<'m>) -> Self {
        Self {
            nodes: &mut equation.nodes,
            reductions: &mut equation.reductions,
            expansions: &mut equation.expansions,
        }
    }

    pub fn reduce_node(&mut self, node_id: IrNodeId) -> Result<Substitution, SolveError> {
        self.reduce_node_inner(node_id, Indent::default())
    }

    fn reduce_node_inner(
        &mut self,
        node_id: IrNodeId,
        indent: Indent,
    ) -> Result<Substitution, SolveError> {
        debug!("{indent}(enter) reduce node {:?}", self.nodes[node_id].kind);

        match &self.nodes[node_id].kind {
            IrKind::Unit => todo!(),
            IrKind::Call(proc, params) => {
                let param_ids = params
                    .into_iter()
                    .map(|param_id| self.reductions.resolve(*param_id))
                    .collect();

                self.reduce_call(node_id, *proc, param_ids, indent)
            }
            IrKind::ValuePattern(value_node) => self.reduce_node_inner(*value_node, indent.inc()),
            IrKind::StructPattern(property_map) => {
                let prop_node_ids: Vec<_> =
                    property_map.iter().map(|(_, node_id)| *node_id).collect();
                for prop_node_id in prop_node_ids {
                    self.reduce_node_inner(prop_node_id, indent.inc())?;
                }
                Ok(Substitution::Constant(node_id))
            }
            IrKind::Variable(_) => Ok(Substitution::Variable(node_id)),
            IrKind::VariableRef(var_id) => Ok(Substitution::Variable(*var_id)),
            IrKind::Constant(_) => Ok(Substitution::Constant(node_id)),
            IrKind::MapCall(param_id, param_ty) => {
                let param_ty = *param_ty;
                let param_id = match self.reduce_node_inner(*param_id, indent.inc())? {
                    Substitution::Variable(var_id) => var_id,
                    Substitution::Constant(const_id) => const_id,
                };

                let (cloned_param_id, cloned_param) = self.clone_node(param_id, indent);
                let cloned_param_span = cloned_param.span;
                let node = &self.nodes[node_id];

                debug!(
                    "{indent}substitute IrKind::MapCall with span {:?}, param_id: {:?}",
                    node.span, param_id.0
                );

                // invert mappping:
                let (inverted_map_id, _) = self.add_node(
                    IrNode {
                        kind: IrKind::MapCall(cloned_param_id, node.ty),
                        ty: param_ty,
                        span: cloned_param_span,
                    },
                    indent,
                );

                // remove the map call from the reductions
                self.reductions[node_id] = param_id;
                // add inverted map call to the expansions
                self.expansions[param_id] = inverted_map_id;

                debug!("{indent}reduction subst: {node_id:?}->{param_id:?}");
                debug!("{indent}expansion subst: {param_id:?}->{inverted_map_id:?} (new!)");

                Ok(Substitution::Variable(param_id))
            }
            IrKind::Aggr(_) => Ok(Substitution::Constant(node_id)),
            IrKind::MapSequence(seq_id, iter_var_id, elem_id, item_ty) => {
                let seq_id = *seq_id;
                let iter_var_id = *iter_var_id;
                let elem_id = *elem_id;
                let item_ty = *item_ty;

                let elem_id_copy = elem_id;

                let elem_id = match self.reduce_node_inner(elem_id, indent.inc())? {
                    Substitution::Variable(var_id) => var_id,
                    Substitution::Constant(const_id) => const_id,
                };

                debug!(
                    "{indent}MapSequence elem_id(before -> after): {elem_id_copy:?}->{elem_id:?}"
                );

                // Escape recursive expansion of the sequence reference:
                let seq_var = self.reductions.resolve(seq_id);
                let (cloned_seq_var, _) = self.clone_node(seq_var, indent);

                debug!("{indent}seq_id->seq_var: {seq_id:?}->{seq_var:?}");

                // let (cloned_item_id, cloned_item) = self.clone_node(elem_id);
                // let cloned_item_span = cloned_item.span;
                let node = &self.nodes[node_id];
                let (inverted_map_id, _) = self.add_node(
                    IrNode {
                        kind: IrKind::MapSequence(cloned_seq_var, iter_var_id, elem_id, item_ty),
                        ty: node.ty,
                        span: node.span,
                    },
                    indent,
                );

                // remove the map call from the reductions
                self.reductions[node_id] = seq_id;
                // add inverted map call to the expansions
                self.expansions[seq_var] = inverted_map_id;

                debug!("{indent}reduction subst: {node_id:?}->{seq_id:?}");
                debug!("{indent}expansion subst: {seq_var:?}->{inverted_map_id:?} (new!)");

                Ok(Substitution::Variable(inverted_map_id))
            }
        }
    }

    fn reduce_call(
        &mut self,
        node_id: IrNodeId,
        proc: BuiltinProc,
        params: SmallVec<[IrNodeId; 4]>,
        indent: Indent,
    ) -> Result<Substitution, SolveError> {
        let params_len = params.len();
        let mut param_var_id = None;

        for (index, param_id) in params.iter().enumerate() {
            match self.reduce_node_inner(*param_id, indent.inc())? {
                Substitution::Variable(rewritten_node) => {
                    if param_var_id.is_some() {
                        return Err(SolveError::MultipleVariablesInCall);
                    }
                    param_var_id = Some((rewritten_node, index));
                }
                Substitution::Constant(_) => {}
            }
        }

        debug!("{indent}reduce proc {proc:?}");

        let (param_var_id, var_index) = match param_var_id {
            Some(rewritten) => rewritten,
            None => {
                debug!("{indent}proc {proc:?} was constant");
                return Ok(Substitution::Constant(node_id));
            }
        };

        let node = &self.nodes[node_id];
        let span = node.span;

        for rule in &rules::RULES {
            if rule.0.proc() != proc || rule.0.params().len() != params_len {
                continue;
            }

            for (_index, (param_id, _rule_param)) in params.iter().zip(rule.0.params()).enumerate()
            {
                let _param_node = &self.nodes[*param_id];
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
            let (cloned_var_id, _) = self.clone_node(param_var_id, indent);
            cloned_params[var_index] = cloned_var_id;

            let target_node = IrNode {
                kind: IrKind::Call(rule.1.proc(), cloned_params.into()),
                ty: node_ty,
                span,
            };
            let (target_node_id, _) = self.add_node(target_node, indent);

            // substitutions
            self.reductions[node_id] = param_var_id;
            self.expansions[param_var_id] = target_node_id;

            debug!("{indent}reduction subst: {node_id:?}->{param_var_id:?}");
            debug!("{indent}expansion subst: {param_var_id:?}->{target_node_id:?} (new!)");

            return Ok(Substitution::Variable(cloned_var_id));
        }

        Err(SolveError::NoRulesMatchedCall)
    }

    fn clone_node(&mut self, node_id: IrNodeId, indent: Indent) -> (IrNodeId, &IrNode<'m>) {
        let node = &self.nodes[node_id];
        self.add_node(node.clone(), indent)
    }

    fn add_node(&mut self, node: IrNode<'m>, indent: Indent) -> (IrNodeId, &IrNode<'m>) {
        let node_id = IrNodeId(self.nodes.0.len() as u32);
        self.nodes.0.push(node);
        self.reductions.push();
        self.expansions.push();
        debug!("{indent}add_node: {node_id:?} => {:?}", self.nodes[node_id]);
        (node_id, &self.nodes[node_id])
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
        codegen::equation::IrNodeEquation,
        ir_node::{BindDepth, IrKind, IrNode, IrNodeTable, SyntaxVar},
        mem::{Intern, Mem},
        types::Type,
        Compiler, SourceSpan, Sources,
    };

    #[test]
    fn test_solver() {
        let mem = Mem::default();
        let mut compiler = Compiler::new(&mem, Sources::default()).with_core();
        let int = compiler.types.intern(Type::Int(DefId(PackageId(0), 42)));

        let mut table = IrNodeTable::default();
        let var = table.add(IrNode {
            kind: IrKind::Variable(SyntaxVar(42, BindDepth(0))),
            ty: int,
            span: SourceSpan::none(),
        });
        let var_ref = table.add(IrNode {
            kind: IrKind::VariableRef(var),
            ty: int,
            span: SourceSpan::none(),
        });
        let constant = table.add(IrNode {
            kind: IrKind::Constant(1000),
            ty: int,
            span: SourceSpan::none(),
        });
        let call = table.add(IrNode {
            kind: IrKind::Call(BuiltinProc::Mul, [var_ref, constant].into()),
            ty: int,
            span: SourceSpan::none(),
        });

        let mut eq = IrNodeEquation::new(table);
        eq.solver().reduce_node(call).unwrap();

        info!("source: {:#?}", eq.debug_tree(call, &eq.reductions));
        info!("target: {:#?}", eq.debug_tree(call, &eq.expansions));
    }
}
