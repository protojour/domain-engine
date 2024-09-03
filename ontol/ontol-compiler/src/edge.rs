//! ONTOL edge / sym group model.
//!
//! Symbol groups map to EdgeId.

use std::collections::{BTreeMap, BTreeSet};

use fnv::{FnvHashMap, FnvHashSet};
use ontol_runtime::{ontology::ontol::TextConstant, tuple::CardinalIdx, DefId, PropId};
use tracing::{debug, error};

use crate::{
    relation::rel_def_meta, repr::repr_model::ReprKind, CompileError, Compiler, SourceSpan,
};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct EdgeId(pub DefId);

#[derive(Default)]
pub struct EdgeCtx {
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

    /// The cardinals in this symbolic edge
    /// The cardinal count is the edge's arity.
    pub cardinals: BTreeMap<CardinalIdx, SymbolicEdgeCardinal>,

    pub clause_widths: BTreeMap<usize, usize>,
}

impl SymbolicEdge {
    pub fn vertex_cardinals(
        &self,
    ) -> impl Iterator<
        Item = (
            CardinalIdx,
            &SymbolicEdgeCardinal,
            &FnvHashMap<DefId, BTreeSet<PropId>>,
        ),
    > {
        self.cardinals
            .iter()
            .filter_map(|(idx, cardinal)| match &cardinal.kind {
                CardinalKind::Vertex { members } => Some((*idx, cardinal, members)),
                CardinalKind::Parameter { .. } => None,
            })
    }

    pub fn find_parameter_cardinal(&self) -> Option<(CardinalIdx, DefId)> {
        self.cardinals.iter().find_map(|(idx, cardinal)| {
            if let CardinalKind::Parameter { def_id } = &cardinal.kind {
                Some((*idx, *def_id))
            } else {
                None
            }
        })
    }
}

#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub struct Slot {
    pub clause_idx: usize,
    pub left: CardinalIdx,
    pub depth: u8,
    pub right: CardinalIdx,
}

#[derive(Debug)]
pub struct SymbolicEdgeCardinal {
    pub span: SourceSpan,
    pub kind: CardinalKind,
    pub unique_count: usize,
    pub pinned_count: usize,
}

#[derive(Debug)]
pub enum CardinalKind {
    Vertex {
        members: FnvHashMap<DefId, BTreeSet<PropId>>,
    },
    Parameter {
        def_id: DefId,
    },
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
            for cardinal in edge.cardinals.values_mut() {
                let CardinalKind::Vertex {
                    members: var_members,
                } = &mut cardinal.kind
                else {
                    continue;
                };

                if var_members.is_empty() {
                    // FIXME: This is actually not an error.
                    // An edge does not need to have participants.
                    // The error case is instead that some variables are populated,
                    // while others aren't.
                    CompileError::ArcNoDefinitionForExistentialVar
                        .span(cardinal.span)
                        .report(&mut self.errors);
                    continue;
                }

                // refine/normalize def set
                // refinement means that it should consist only of entity defs.

                debug!("raw participants: {:?}", var_members);

                // step 1: resolve unions
                for (def_id, prop_set) in std::mem::take(var_members) {
                    match self.repr_ctx.get_repr_kind(&def_id) {
                        Some(ReprKind::Union(union_members, _)) => {
                            for (member, _span) in union_members {
                                var_members.entry(*member).or_default().extend(&prop_set);
                            }
                        }
                        _ => {
                            var_members.entry(def_id).or_default().extend(&prop_set);
                        }
                    }
                }

                // step 2: resolve identifier types
                for (def_id, prop_set) in std::mem::take(var_members) {
                    if let Some(properties) = self.prop_ctx.properties_by_def_id(def_id) {
                        if let Some(identifies_rel_id) = properties.identifies {
                            let meta = rel_def_meta(identifies_rel_id, &self.rel_ctx, &self.defs);

                            var_members.insert(meta.relationship.object.0, prop_set);
                        } else {
                            var_members.insert(def_id, prop_set);
                        }
                    }
                }

                // step 3: edge entity check
                for (def_id, prop_set) in var_members {
                    if !self.entity_ctx.entities.contains_key(def_id) {
                        let prop_id = prop_set.first().unwrap();
                        let property = self.prop_ctx.property_by_id(*prop_id).unwrap();
                        CompileError::TODO("must be entity to participate in edge")
                            .span(self.rel_ctx.span(property.rel_id))
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
            let mut vertex_slot_set_per_member: BTreeMap<DefId, FnvHashSet<CardinalIdx>> =
                Default::default();

            for cardinal in edge.cardinals.values() {
                let CardinalKind::Vertex { members } = &cardinal.kind else {
                    continue;
                };

                for (member_id, prop_ids) in members {
                    for prop_id in prop_ids {
                        let property = self.prop_ctx.property_by_id(*prop_id).unwrap();
                        let meta = rel_def_meta(property.rel_id, &self.rel_ctx, &self.defs);

                        if &meta.relationship.subject.0 == member_id {
                            let slot_set =
                                vertex_slot_set_per_member.entry(*member_id).or_default();
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

            for (member_id, slot_set) in vertex_slot_set_per_member {
                if slot_set.len() >= edge.vertex_cardinals().count() {
                    continue;
                }

                debug!("{member_id:?} has partial participation: {slot_set:?}");

                if self
                    .prop_ctx
                    .properties_table_by_def_id(member_id)
                    .is_none()
                {
                    continue;
                }

                // collect relationships again
                for cardinal in edge.cardinals.values() {
                    let CardinalKind::Vertex { members } = &cardinal.kind else {
                        continue;
                    };

                    for prop_ids in members.values() {
                        for prop_id in prop_ids {
                            let property = self.prop_ctx.property_by_id(*prop_id).unwrap();
                            let meta = rel_def_meta(property.rel_id, &self.rel_ctx, &self.defs);

                            if meta.relationship.subject.0 == member_id {
                                let properties = self.prop_ctx.properties_by_def_id_mut(member_id);
                                let table = properties.table.as_mut().unwrap();

                                if let Some(property) = table.get_mut(prop_id) {
                                    property.is_edge_partial = true;
                                } else {
                                    error!(
                                        "property {member_id:?}.{prop_id:?} not found in {keys:?}",
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
