use std::ops::{Index, IndexMut};

use crate::{codegen::equation_solver::SubstitutionTable, HirIdx, SourceSpan};

use super::node::{Hir2Kind, Hir2Node};

#[derive(Default, Debug)]
pub struct Hir2NodeTable<'m>(pub(super) Vec<Hir2Node<'m>>);

impl<'m> Hir2NodeTable<'m> {
    pub fn add(&mut self, expr: Hir2Node<'m>) -> HirIdx {
        let id = HirIdx(self.0.len() as u32);
        self.0.push(expr);
        id
    }
}

impl<'m> Index<HirIdx> for Hir2NodeTable<'m> {
    type Output = Hir2Node<'m>;

    fn index(&self, index: HirIdx) -> &Self::Output {
        &self.0[index.0 as usize]
    }
}

impl<'m> IndexMut<HirIdx> for Hir2NodeTable<'m> {
    fn index_mut(&mut self, index: HirIdx) -> &mut Self::Output {
        &mut self.0[index.0 as usize]
    }
}

pub struct UnificationTable<'m> {
    /// The set of nodes part of the equation
    pub nodes: Hir2NodeTable<'m>,
    /// Number of nodes
    original_nodes_len: usize,
    /// The registered substitutions for this specific unification
    substitutions: SubstitutionTable,
}

impl<'m> UnificationTable<'m> {
    pub fn new(nodes: Hir2NodeTable<'m>) -> Self {
        let len = nodes.0.len();
        let mut table = Self {
            nodes,
            original_nodes_len: len,
            substitutions: Default::default(),
        };
        for _ in 0..len {
            table.substitutions.push();
        }
        table.make_default_substitutions();
        table
    }

    #[inline]
    pub fn resolve_node<'e>(
        &'e self,
        substitutions: &SubstitutionTable,
        source_idx: HirIdx,
    ) -> (HirIdx, &'e Hir2Node<'m>, SourceSpan) {
        let span = self.nodes[source_idx].span;
        let root_idx = substitutions.resolve(source_idx);
        (root_idx, &self.nodes[root_idx], span)
    }

    fn make_default_substitutions(&mut self) {
        for (index, node) in self.nodes.0.iter().enumerate() {
            if let Hir2Kind::VariableRef(id) = &node.kind {
                self.substitutions[HirIdx(index as u32)] = *id;
            }
        }
    }
}
