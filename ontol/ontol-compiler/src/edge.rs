//! ONTOL edge / sym group model.
//!
//! Symbol groups map to EdgeId.

use std::collections::{BTreeMap, BTreeSet};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{
    ontology::ontol::TextConstant, tuple::CardinalIdx, DefId, EdgeId, PackageId, RelId,
};
use tracing::{debug, error};

use crate::{
    relation::rel_def_meta, repr::repr_model::ReprKind, CompileError, Compiler, SourceSpan,
};

#[derive(Default)]
pub struct EdgeCtx {
    pub ids: FnvHashMap<PackageId, u16>,

    /// Entrypoints into the edges are the relation symbols defined by the edge.
    ///
    /// This table maps from a symbol definition to an edge definition.
    pub symbols: FnvHashMap<DefId, EdgeId>,

    pub symbolic_edges: FnvHashMap<EdgeId, SymbolicEdge>,

    // pub edge_participants: FnvHashMap<DefId, FnvHashMap<EdgeId, EdgeParticipant>>,
    pub store_keys: FnvHashMap<DefId, TextConstant>,
    pub edge_store_keys: FnvHashMap<EdgeId, TextConstant>,
}

impl EdgeCtx {
    pub fn alloc_edge_id(&mut self, package_id: PackageId) -> EdgeId {
        let next = self.ids.entry(package_id).or_default();
        let tag = *next;
        *next += 1;
        EdgeId(package_id, tag)
    }

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
    pub variables: BTreeMap<CardinalIdx, SymbolicEdgeVariable>,
}

#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub struct Slot {
    pub left: CardinalIdx,
    pub depth: u8,
    pub right: CardinalIdx,
}

#[derive(Debug)]
pub struct SymbolicEdgeVariable {
    pub span: SourceSpan,
    /// Defs participating in this variable.
    pub members: FnvHashMap<DefId, BTreeSet<RelId>>,
    pub one_to_one_count: usize,
}

impl<'m> Compiler<'m> {
    /// Check/process all symbolic edges.
    ///
    /// This happens after all domains have been compiled
    pub fn check_symbolic_edges(&mut self) {
        self.normalize_variable_participants();
        self.mark_partial_participations();
    }

    /// Resolve all variable participants to entities/vertices
    fn normalize_variable_participants(&mut self) {
        for edge in self.edge_ctx.symbolic_edges.values_mut() {
            for var in edge.variables.values_mut() {
                if var.members.is_empty() {
                    // FIXME: This is actually not an error.
                    // An edge does not need to have participants.
                    // The error case is instead that some variables are populated,
                    // while others aren't.
                    CompileError::SymEdgeNoDefinitionForExistentialVar
                        .span(var.span)
                        .report(&mut self.errors);
                    continue;
                }

                // refine/normalize def set
                // refinement means that it should consist only of entity defs.

                debug!("raw participants: {:?}", var.members);

                // step 1: resolve unions
                for (def_id, rel_set) in std::mem::take(&mut var.members) {
                    match self.repr_ctx.get_repr_kind(&def_id) {
                        Some(ReprKind::Union(members, _)) => {
                            for (member, _span) in members {
                                var.members.entry(*member).or_default().extend(&rel_set);
                            }
                        }
                        _ => {
                            var.members.entry(def_id).or_default().extend(&rel_set);
                        }
                    }
                }

                // step 2: resolve identifier types
                for (def_id, rel_set) in std::mem::take(&mut var.members) {
                    if let Some(properties) = self.prop_ctx.properties_by_def_id(def_id) {
                        if let Some(identifies_rel_id) = properties.identifies {
                            let meta = rel_def_meta(identifies_rel_id, &self.rel_ctx, &self.defs);

                            var.members.insert(meta.relationship.object.0, rel_set);
                        } else {
                            var.members.insert(def_id, rel_set);
                        }
                    }
                }

                // step 3: edge entity check
                for (def_id, rel_set) in &var.members {
                    if !self.entity_ctx.entities.contains_key(def_id) {
                        let rel_id = rel_set.first().unwrap();
                        CompileError::TODO("must be entity to participate in edge")
                            .span(self.rel_ctx.span(*rel_id))
                            .report(&mut self.errors);
                    }
                }
            }
        }
    }

    /// analyze the edges, for entities with partial participation, write
    /// `Property::is_edge_partial = true` back into RelCtx.
    /// The point is that partial edge participation must be read-only properties, because there is
    /// not information there to construct a new edge.
    fn mark_partial_participations(&mut self) {
        for edge in self.edge_ctx.symbolic_edges.values() {
            let mut slot_set_per_member: BTreeMap<DefId, FnvHashSet<CardinalIdx>> =
                Default::default();

            for var in edge.variables.values() {
                for (member_id, rel_ids) in &var.members {
                    for rel_id in rel_ids {
                        let meta = rel_def_meta(*rel_id, &self.rel_ctx, &self.defs);

                        if &meta.relationship.subject.0 == member_id {
                            let slot_set = slot_set_per_member.entry(*member_id).or_default();
                            let symbol = edge
                                .symbols
                                .get(&meta.relationship.relation_def_id)
                                .unwrap();

                            slot_set.insert(symbol.left);
                            slot_set.insert(symbol.right);
                        }
                    }
                }
            }

            for (member_id, slot_set) in slot_set_per_member {
                if slot_set.len() >= edge.variables.len() {
                    continue;
                }

                debug!("{member_id:?} has partial participation: {slot_set:?}");

                let properties = self.prop_ctx.properties_by_def_id_mut(member_id);
                let Some(table) = &mut properties.table else {
                    continue;
                };

                // collect relationships again
                for var in edge.variables.values() {
                    for rel_ids in var.members.values() {
                        for rel_id in rel_ids {
                            let meta = rel_def_meta(*rel_id, &self.rel_ctx, &self.defs);

                            if meta.relationship.subject.0 == member_id {
                                if let Some(property) = table.get_mut(rel_id) {
                                    property.is_edge_partial = true;
                                } else {
                                    error!(
                                        "property {member_id:?}.{rel_id:?} not found in {keys:?}",
                                        keys = table.keys()
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
