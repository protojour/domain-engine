use std::collections::{hash_map::Entry, BTreeMap, HashMap};

use fnv::FnvHashMap;
use ontol_parser::{
    cst::{
        inspect as insp,
        view::{NodeView, TokenView},
    },
    U32Span,
};
use ontol_runtime::{tuple::CardinalIdx, DefId, OntolDefTag};

use crate::{
    def::DefKind,
    edge::{CardinalKind, EdgeId, Slot, SymbolicEdge, SymbolicEdgeCardinal},
    namespace::Space,
    CompileError,
};

use super::context::{Coinage, CstLowering, RootDefs};

struct EdgeBuilder<V> {
    edge_def_id: DefId,
    cardinal_name_table: HashMap<String, CardinalIdx>,
    slots: FnvHashMap<DefId, Slot>,
    cardinals: BTreeMap<CardinalIdx, SymbolicEdgeCardinal>,
    clause_widths: BTreeMap<usize, usize>,
    params: BTreeMap<CardinalIdx, insp::ArcTypeParam<V>>,
}

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub fn lower_arc_statement(
        &mut self,
        arc_stmt: insp::ArcStatement<V>,
    ) -> Option<(
        RootDefs,
        EdgeId,
        BTreeMap<CardinalIdx, insp::ArcTypeParam<V>>,
    )> {
        let ident_path = arc_stmt.ident_path()?;
        let ident_symbol = ident_path.symbols().next()?;
        let (edge_def_id, coinage, ident) = self.catch(|zelf| {
            zelf.ctx.named_def_id(
                zelf.ctx.pkg_def_id,
                Space::Def,
                ident_symbol.slice(),
                ident_symbol.span(),
            )
        })?;

        if matches!(coinage, Coinage::Used) {
            CompileError::ArcMustHaveUniqueIdentifier
                .span_report(ident_symbol.span(), &mut self.ctx);
        }

        let mut root_defs = vec![edge_def_id];

        self.ctx
            .set_def_kind(edge_def_id, DefKind::Arc(ident), ident_symbol.span());

        let mut edge_builder = EdgeBuilder {
            edge_def_id,
            slots: Default::default(),
            cardinals: Default::default(),
            clause_widths: Default::default(),
            cardinal_name_table: Default::default(),
            params: Default::default(),
        };

        for (clause_idx, arc_clause) in arc_stmt.arc_clauses().enumerate() {
            let mut item_iter = arc_clause.items().peekable();

            match self.next_arc_item(&mut item_iter, arc_clause.view().span()) {
                Some(insp::ArcItem::ArcVar(edge_var)) => {
                    let Some(mut prev_cardinal) =
                        self.add_arc_variable(edge_var, &mut edge_builder)
                    else {
                        continue;
                    };

                    loop {
                        if let Some((slot_def_id, next_cardinal)) = self
                            .add_edge_slot_symbol_then_cardinal(
                                clause_idx,
                                &mut item_iter,
                                prev_cardinal,
                                &mut edge_builder,
                                arc_stmt.view().span(),
                            )
                        {
                            self.ctx
                                .compiler
                                .edge_ctx
                                .symbols
                                .insert(slot_def_id, EdgeId(edge_builder.edge_def_id));

                            root_defs.push(slot_def_id);
                            prev_cardinal = next_cardinal;
                        } else {
                            break;
                        }

                        if item_iter.peek().is_none() {
                            break;
                        }
                    }
                }
                Some(insp::ArcItem::ArcSlot(_slot)) => {
                    unreachable!("arc cannot start with a slot");
                }
                Some(insp::ArcItem::ArcTypeParam(_)) => {
                    unreachable!("statement cannot start with ArcTypeParam")
                }
                None => {}
            }
        }

        self.ctx.compiler.edge_ctx.symbolic_edges.insert(
            EdgeId(edge_builder.edge_def_id),
            SymbolicEdge {
                symbols: edge_builder.slots,
                cardinals: edge_builder.cardinals,
                clause_widths: edge_builder.clause_widths,
            },
        );

        Some((root_defs, EdgeId(edge_def_id), edge_builder.params))
    }

    fn next_arc_item(
        &mut self,
        item_iter: &mut impl Iterator<Item = insp::ArcItem<V>>,
        edge_relation_span: U32Span,
    ) -> Option<insp::ArcItem<V>> {
        let item = item_iter.next();

        if item.is_none() {
            CompileError::ArcClauseExpectedTrailingItem
                .span_report(edge_relation_span, &mut self.ctx);
        }

        item
    }

    fn add_edge_slot_symbol_then_cardinal(
        &mut self,
        clause_idx: usize,
        item_iter: &mut impl Iterator<Item = insp::ArcItem<V>>,
        prev_cardinal_idx: CardinalIdx,
        edge_builder: &mut EdgeBuilder<V>,
        edge_relation_span: U32Span,
    ) -> Option<(DefId, CardinalIdx)> {
        let next_item = self.next_arc_item(item_iter, edge_relation_span)?;

        let insp::ArcItem::ArcSlot(arc_slot) = next_item else {
            CompileError::ArcClauseExpectedSymbol
                .span_report(next_item.view().span(), &mut self.ctx);
            return None;
        };

        let symbol = arc_slot.symbol()?;
        let slot_symbol_def_id = self.catch(|zelf| {
            zelf.ctx
                .coin_symbol(edge_builder.edge_def_id, symbol.slice(), symbol.span())
        });
        let next_item = self.next_arc_item(item_iter, edge_relation_span)?;

        let cardinal_idx = match next_item {
            insp::ArcItem::ArcVar(arc_var) => self.add_arc_variable(arc_var, edge_builder),
            insp::ArcItem::ArcTypeParam(arc_type_param) => {
                self.add_edge_type_param(arc_type_param, edge_builder)
            }
            insp::ArcItem::ArcSlot(_) => {
                CompileError::ArcClauseExpectedVariable
                    .span_report(next_item.view().span(), &mut self.ctx);
                return None;
            }
        };

        match (slot_symbol_def_id, cardinal_idx) {
            (Some(slot_symbol_def_id), Some(cardinal_idx)) => {
                let clause_width = edge_builder
                    .clause_widths
                    .entry(clause_idx)
                    .or_insert_with(|| 1);
                *clause_width += 1;

                edge_builder.slots.insert(
                    slot_symbol_def_id,
                    Slot {
                        clause_idx,
                        left: prev_cardinal_idx,
                        depth: 0,
                        right: cardinal_idx,
                    },
                );

                Some((slot_symbol_def_id, cardinal_idx))
            }
            _ => None,
        }
    }

    fn add_arc_variable(
        &mut self,
        arc_var: insp::ArcVar<V>,
        edge_builder: &mut EdgeBuilder<V>,
    ) -> Option<CardinalIdx> {
        let var_symbol = arc_var.symbol()?;
        let len = edge_builder.cardinals.len();

        match edge_builder
            .cardinal_name_table
            .entry(var_symbol.slice().to_string())
        {
            Entry::Occupied(occupied) => Some(*occupied.get()),
            Entry::Vacant(vacant) => {
                let Ok(cardinal_idx): Result<u8, _> = len.try_into() else {
                    CompileError::EdgeArityOverflow
                        .span_report(arc_var.view().span(), &mut self.ctx);

                    return None;
                };
                let cardinal_idx = CardinalIdx(cardinal_idx);

                edge_builder.cardinals.insert(
                    cardinal_idx,
                    SymbolicEdgeCardinal {
                        span: self.ctx.source_span(var_symbol.span()),
                        kind: CardinalKind::Vertex {
                            members: Default::default(),
                        },
                        unique_count: 0,
                        pinned_count: 0,
                    },
                );

                vacant.insert(cardinal_idx);
                Some(cardinal_idx)
            }
        }
    }

    /// note: the path of the type param is not looked up in the pre-define stage,
    /// that's instead isdone in the post-processing stage, to be able to resolve later definitions.
    fn add_edge_type_param(
        &mut self,
        edge_type_param: insp::ArcTypeParam<V>,
        edge_builder: &mut EdgeBuilder<V>,
    ) -> Option<CardinalIdx> {
        let ident_path = edge_type_param.ident_path()?;
        let leaf_symbol = ident_path.symbols().last()?;

        let len = edge_builder.cardinals.len();

        match edge_builder
            .cardinal_name_table
            .entry(leaf_symbol.slice().to_string())
        {
            Entry::Occupied(occupied) => Some(*occupied.get()),
            Entry::Vacant(vacant) => {
                let Ok(cardinal_idx): Result<u8, _> = len.try_into() else {
                    CompileError::EdgeArityOverflow
                        .span_report(edge_type_param.view().span(), &mut self.ctx);

                    return None;
                };
                let cardinal_idx = CardinalIdx(cardinal_idx);

                edge_builder.cardinals.insert(
                    cardinal_idx,
                    SymbolicEdgeCardinal {
                        span: self.ctx.source_span(leaf_symbol.span()),
                        kind: CardinalKind::Parameter {
                            // will be resolved later..
                            def_id: OntolDefTag::Unit.def_id(),
                        },
                        unique_count: 0,
                        pinned_count: 0,
                    },
                );
                // .. by this "params" thing which stores the source position:
                edge_builder.params.insert(cardinal_idx, edge_type_param);

                vacant.insert(cardinal_idx);
                Some(cardinal_idx)
            }
        }
    }
}
