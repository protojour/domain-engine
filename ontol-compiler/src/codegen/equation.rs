use std::fmt::Debug;

use ontol_runtime::value::PropertyId;

use crate::{
    hir_node::{BindDepth, HirIdx, HirKind, HirNode, HirNodeTable, HirVariable},
    SourceSpan,
};

use super::equation_solver::{EquationSolver, SubstitutionTable};

/// Table used to store equation-like expression trees
/// suitable for rewriting.
#[derive(Default)]
pub struct HirEquation<'m> {
    /// The set of nodes part of the equation
    pub nodes: HirNodeTable<'m>,
    /// Number of nodes
    original_nodes_len: usize,
    /// Rewrites for the reduced side
    /// These nodes should only represent destructuring and variable definitions, no "computation".
    pub reductions: SubstitutionTable,
    /// Rewrites for the expanded side.
    /// The resulting "expression" should produce the output value.
    pub expansions: SubstitutionTable,
}

impl<'m> HirEquation<'m> {
    pub fn new(nodes: HirNodeTable<'m>) -> Self {
        let len = nodes.0.len();
        let mut equation = Self {
            nodes,
            original_nodes_len: len,
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
    pub fn resolve_node<'e>(
        &'e self,
        substitutions: &SubstitutionTable,
        source_idx: HirIdx,
    ) -> (HirIdx, &'e HirNode<'m>, SourceSpan) {
        let span = self.nodes[source_idx].span;
        let root_idx = substitutions.resolve(source_idx);
        (root_idx, &self.nodes[root_idx], span)
    }

    /// Reset all generated nodes and substitutions
    pub fn reset(&mut self) {
        self.nodes.0.truncate(self.original_nodes_len);
        self.reductions.reset(self.original_nodes_len);
        self.expansions.reset(self.original_nodes_len);
        self.make_default_substituions()
    }

    fn make_default_substituions(&mut self) {
        for (index, node) in self.nodes.0.iter().enumerate() {
            if let HirKind::VariableRef(id) = &node.kind {
                self.reductions[HirIdx(index as u32)] = *id;
                self.expansions[HirIdx(index as u32)] = *id;
            }
        }
    }

    pub fn debug_tree<'e>(
        &'e self,
        node_id: HirIdx,
        substitutions: &'e SubstitutionTable,
    ) -> DebugTree<'e, 'm> {
        DebugTree {
            equation: self,
            substitutions,
            node_id,
            property_id: None,
            depth: 0,
        }
    }
}

impl<'m> Debug for HirEquation<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HirEquation").finish()
    }
}

pub struct DebugTree<'a, 'm> {
    equation: &'a HirEquation<'m>,
    substitutions: &'a SubstitutionTable,
    node_id: HirIdx,
    property_id: Option<PropertyId>,
    depth: usize,
}

impl<'a, 'm> DebugTree<'a, 'm> {
    fn child(&self, node_id: HirIdx, property_id: Option<PropertyId>) -> Self {
        Self {
            equation: self.equation,
            substitutions: self.substitutions,
            node_id,
            property_id,
            depth: self.depth + 1,
        }
    }

    fn header(&self, name: &str) -> String {
        use std::fmt::Write;
        let mut s = String::new();

        {
            let mut node_id = self.node_id;
            write!(&mut s, "{{{}", node_id.0).unwrap();
            while let Some(next) = self.substitutions.resolve_once(node_id) {
                write!(&mut s, "->{}", next.0).unwrap();
                node_id = next;
            }
            write!(&mut s, "}} ").unwrap();
        }

        if let Some(property_id) = &self.property_id {
            let def_id = &property_id.relation_id.0;
            write!(&mut s, "(rel {}, {}) ", def_id.0 .0, def_id.1).unwrap();
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

        let (_, node, _) = self.equation.resolve_node(self.substitutions, self.node_id);

        match &node.kind {
            HirKind::Unit => f.debug_tuple(&self.header("Unit")).finish()?,
            HirKind::Call(proc, params) => {
                let mut tup = f.debug_tuple(&self.header(&format!("{proc:?}")));
                for param in params {
                    tup.field(&self.child(*param, None));
                }
                tup.finish()?
            }
            HirKind::ValuePattern(node_id) => f
                .debug_tuple(&self.header("ValuePattern"))
                .field(&self.child(*node_id, None))
                .finish()?,
            HirKind::StructPattern(attributes) => {
                let mut tup = f.debug_tuple(&self.header("StructPattern"));
                for (property_id, node_id) in attributes {
                    tup.field(&self.child(*node_id, Some(*property_id)));
                }
                tup.finish()?;
            }
            HirKind::Constant(c) => f
                .debug_tuple(&self.header(&format!("Constant({c})")))
                .finish()?,
            HirKind::Variable(HirVariable(v, BindDepth(d))) => f
                .debug_tuple(&self.header(&format!("Variable({v} d={d})")))
                .finish()?,
            HirKind::VariableRef(var_id) => f
                .debug_tuple(&self.header("VarRef"))
                .field(&self.child(*var_id, None))
                .finish()?,
            HirKind::MapCall(node_id, _) => f
                .debug_tuple(&self.header("MapCall"))
                .field(&self.child(*node_id, None))
                .finish()?,
            HirKind::Aggr(var_idx, body_id) => f
                .debug_tuple(&self.header("Aggr"))
                .field(&self.child(*var_idx, None))
                .field(body_id)
                .finish()?,
            HirKind::MapSequence(node_id, var, body, _) => f
                .debug_tuple(&self.header("MapSequence"))
                .field(&self.child(*node_id, None))
                .field(&var)
                .field(&self.child(*body, None))
                .finish()?,
        };

        Ok(())
    }
}
