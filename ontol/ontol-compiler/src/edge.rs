//! ONTOL edge / sym group model.
//!
//! Symbol groups map to EdgeId.

use fnv::FnvHashMap;
use ontol_runtime::{
    ontology::{domain::EdgeCardinalProjection, ontol::TextConstant},
    tuple::CardinalIdx,
    DefId, EdgeId, RelationshipId,
};

#[derive(Default)]
pub struct EdgeCtx {
    /// Entrypoints into the edges are the relation symbols defined by the edge.
    ///
    /// This table maps from a symbol definition to an edge definition.
    pub symbols: FnvHashMap<DefId, EdgeId>,

    pub edges: FnvHashMap<EdgeId, MaterializedEdge>,

    pub rel_to_edge: FnvHashMap<RelationshipId, EdgeCardinalProjection>,

    pub store_keys: FnvHashMap<DefId, TextConstant>,
}

impl EdgeCtx {
    #[allow(unused)]
    pub fn edge_id_by_symbol(&self, symbol: DefId) -> Option<EdgeId> {
        self.symbols.get(&symbol).copied()
    }

    /// Look up an edge using a symbol from that edge
    #[allow(unused)]
    pub fn edge_by_symbol(&self, symbol: DefId) -> Option<(EdgeId, &MaterializedEdge)> {
        let edge_id = self.symbols.get(&symbol)?;
        self.edges.get(edge_id).map(|edge| (*edge_id, edge))
    }
}

/// An edge models relationships between certain symbols in a domain.
///
/// These symbols represents the _aspect_ of an edge when seen from a certain viewpoint.
#[allow(unused)]
pub struct MaterializedEdge {
    /// There is one slot per edge symbol, one slot connects two vertices together using a symbol.
    pub slots: FnvHashMap<DefId, Slot>,

    /// How many vertex variables are in the edge.
    ///
    /// arity of 2 is a classical edge.
    /// arity of more than 2 is a "hyperedge".
    pub arity: u8,
}

#[derive(Debug)]
#[allow(unused)]
pub struct Slot {
    pub left: CardinalIdx,
    pub depth: u8,
    pub right: CardinalIdx,
}
