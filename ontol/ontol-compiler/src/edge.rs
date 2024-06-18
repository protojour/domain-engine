//! ONTOL edge / sym group model.
//!
//! Symbol groups map to EdgeId.

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{ontology::ontol::TextConstant, tuple::CardinalIdx, DefId, EdgeId};

use crate::SourceSpan;

#[derive(Default)]
pub struct EdgeCtx {
    /// Entrypoints into the edges are the relation symbols defined by the edge.
    ///
    /// This table maps from a symbol definition to an edge definition.
    pub symbols: FnvHashMap<DefId, EdgeId>,

    pub symbolic_edges: FnvHashMap<EdgeId, SymbolicEdge>,

    pub store_keys: FnvHashMap<DefId, TextConstant>,
}

impl EdgeCtx {
    #[allow(unused)]
    pub fn edge_id_by_symbol(&self, symbol: DefId) -> Option<EdgeId> {
        self.symbols.get(&symbol).copied()
    }

    /// Look up an edge using a symbol from that edge
    #[allow(unused)]
    pub fn edge_by_symbol(&self, symbol: DefId) -> Option<(EdgeId, &SymbolicEdge)> {
        let edge_id = self.symbols.get(&symbol)?;
        self.symbolic_edges
            .get(edge_id)
            .map(|edge| (*edge_id, edge))
    }
}

/// An edge models relationships between certain symbols in a domain.
///
/// These symbols represents the _aspect_ of an edge when seen from a certain viewpoint.
pub struct SymbolicEdge {
    /// There is one slot per edge symbol, one slot connects two vertices together using a symbol.
    pub symbols: FnvHashMap<DefId, Slot>,

    /// The variables in this symbolic edge
    /// The variable count is the edge's arity.
    pub variables: FnvHashMap<CardinalIdx, SymbolicEdgeVariable>,
}

#[derive(Debug)]
#[allow(unused)]
pub struct Slot {
    pub left: CardinalIdx,
    pub depth: u8,
    pub right: CardinalIdx,
}

pub struct SymbolicEdgeVariable {
    pub span: SourceSpan,
    pub def_set: FnvHashSet<DefId>,
}
