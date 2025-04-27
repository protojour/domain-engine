use std::collections::{BTreeMap, HashMap, hash_map::Entry};

use fnv::FnvHashMap;
use ontol_log::tag::Tag;
use ontol_runtime::{DefId, tuple::CardinalIdx};

use crate::edge::{CardinalKind, EdgeId, Slot, SymbolicEdge, SymbolicEdgeCardinal};

use super::LogLowering;

impl LogLowering<'_, '_, '_> {
    /// FIXME: implement type params and clause widths
    pub(super) fn lower_arc(&mut self, tag: Tag, arc: &ontol_log::sem_stmts::Arc) {
        let def_id = self.mk_persistent_def_id(self.domain_index, tag);

        let mut symbols: FnvHashMap<DefId, Slot> = HashMap::default();
        let mut cardinals: BTreeMap<CardinalIdx, SymbolicEdgeCardinal> = Default::default();
        #[expect(unused)]
        let mut clause_widths: BTreeMap<usize, usize> = Default::default();

        let mut cardinal_name_table: HashMap<&str, CardinalIdx> = Default::default();

        let mut clause_idx = 0_u8;
        loop {
            let mut found = 0;

            for (coord, var) in &arc.vars {
                if coord.0 != clause_idx {
                    continue;
                }

                match cardinal_name_table.entry(var.0.text()) {
                    Entry::Vacant(v) => {
                        v.insert(CardinalIdx(coord.1));

                        cardinals.insert(
                            CardinalIdx(coord.1),
                            SymbolicEdgeCardinal {
                                span: self.source_id.span(var.span()),
                                kind: CardinalKind::Vertex {
                                    members: Default::default(),
                                },
                                unique_count: 0,
                                pinned_count: 0,
                            },
                        );
                    }
                    Entry::Occupied(_) => {}
                }

                found += 1;
            }

            if found == 0 {
                break;
            } else {
                clause_idx += 1;
            }
        }

        let mut clause_idx = 0_usize;
        loop {
            let mut found = 0;

            for (coord, slot_symbol) in &arc.slots {
                if coord.0 as usize != clause_idx {
                    continue;
                }

                let slot_symbol_def_id =
                    self.coin_symbol(None, slot_symbol.0.text(), slot_symbol.span());

                symbols.insert(
                    slot_symbol_def_id,
                    Slot {
                        clause_idx,
                        left: CardinalIdx(coord.1 - 1),
                        depth: 0,
                        right: CardinalIdx(coord.1),
                    },
                );

                found += 1;
            }

            if found == 0 {
                break;
            } else {
                clause_idx += 1;
            }
        }

        for slot_symbol_def_id in symbols.keys() {
            self.compiler
                .edge_ctx
                .symbols
                .insert(*slot_symbol_def_id, EdgeId(def_id));
        }

        self.compiler.edge_ctx.symbolic_edges.insert(
            EdgeId(def_id),
            SymbolicEdge {
                symbols,
                cardinals,
                clause_widths,
            },
        );
    }
}
