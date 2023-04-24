use std::fmt::Debug;

use ontol_runtime::value::PropertyId;

use crate::{
    ir_node::{BindDepth, IrKind, IrNode, IrNodeId, IrNodeTable, SyntaxVar},
    SourceSpan,
};

use super::equation_solver::{EquationSolver, SubstitutionTable};

/// Table used to store equation-like expression trees
/// suitable for rewriting.
#[derive(Default)]
pub struct IrNodeEquation<'m> {
    /// The set of nodes part of the equation
    pub nodes: IrNodeTable<'m>,
    /// Number of nodes
    original_nodes_len: usize,
    /// Rewrites for the reduced side
    pub reductions: SubstitutionTable,
    /// Rewrites for the expanded side
    pub expansions: SubstitutionTable,
}

impl<'m> IrNodeEquation<'m> {
    pub fn new(nodes: IrNodeTable<'m>) -> Self {
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
        source_node: IrNodeId,
    ) -> (IrNodeId, &'e IrNode<'m>, SourceSpan) {
        let span = self.nodes[source_node].span;
        let root_node = substitutions.resolve(source_node);
        (root_node, &self.nodes[root_node], span)
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
            if let IrKind::VariableRef(id) = &node.kind {
                self.reductions[IrNodeId(index as u32)] = *id;
                self.expansions[IrNodeId(index as u32)] = *id;
            }
        }
    }

    pub fn debug_tree<'e>(
        &'e self,
        node_id: IrNodeId,
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

impl<'m> Debug for IrNodeEquation<'m> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrNodeEquation").finish()
    }
}

pub struct DebugTree<'a, 'm> {
    equation: &'a IrNodeEquation<'m>,
    substitutions: &'a SubstitutionTable,
    node_id: IrNodeId,
    property_id: Option<PropertyId>,
    depth: usize,
}

impl<'a, 'm> DebugTree<'a, 'm> {
    fn child(&self, node_id: IrNodeId, property_id: Option<PropertyId>) -> Self {
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
            IrKind::Unit => f.debug_tuple(&self.header("Unit")).finish()?,
            IrKind::Call(proc, params) => {
                let mut tup = f.debug_tuple(&self.header(&format!("{proc:?}")));
                for param in params {
                    tup.field(&self.child(*param, None));
                }
                tup.finish()?
            }
            IrKind::ValuePattern(node_id) => f
                .debug_tuple(&self.header("ValuePattern"))
                .field(&self.child(*node_id, None))
                .finish()?,
            IrKind::StructPattern(attributes) => {
                let mut tup = f.debug_tuple(&self.header("StructPattern"));
                for (property_id, node_id) in attributes {
                    tup.field(&self.child(*node_id, Some(*property_id)));
                }
                tup.finish()?;
            }
            IrKind::Constant(c) => f
                .debug_tuple(&self.header(&format!("Constant({c})")))
                .finish()?,
            IrKind::Variable(SyntaxVar(v, BindDepth(d))) => f
                .debug_tuple(&self.header(&format!("Variable({v} d={d})")))
                .finish()?,
            IrKind::VariableRef(var_id) => f
                .debug_tuple(&self.header("VarRef"))
                .field(&self.child(*var_id, None))
                .finish()?,
            IrKind::MapCall(node_id, _) => f
                .debug_tuple(&self.header("MapCall"))
                .field(&self.child(*node_id, None))
                .finish()?,
            IrKind::MapSequence(node_id, var, body, _) => f
                .debug_tuple(&self.header("MapSequence"))
                .field(&self.child(*node_id, None))
                .field(&var)
                .field(&self.child(*body, None))
                .finish()?,
            IrKind::MapSequenceBalanced(node_id, var, body, _) => f
                .debug_tuple(&self.header("MapSequenceBalanced"))
                .field(&self.child(*node_id, None))
                .field(&var)
                .field(&self.child(*body, None))
                .finish()?,
        };

        Ok(())
    }
}
