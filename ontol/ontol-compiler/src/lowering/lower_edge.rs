use std::collections::{hash_map::Entry, BTreeMap, HashMap};

use fnv::FnvHashMap;
use ontol_parser::{
    cst::{
        inspect::{self as insp, EdgeTypeParam},
        view::{NodeView, TokenView},
    },
    U32Span,
};
use ontol_runtime::{tuple::CardinalIdx, DefId, EdgeId, OntolDefTag};

use crate::{
    edge::{CardinalKind, Slot, SymbolicEdge, SymbolicEdgeCardinal},
    CompileError,
};

use super::context::{CstLowering, RootDefs};

struct EdgeBuilder<V> {
    edge_id: EdgeId,
    cardinal_name_table: HashMap<String, CardinalIdx>,
    slots: FnvHashMap<DefId, Slot>,
    cardinals: BTreeMap<CardinalIdx, SymbolicEdgeCardinal>,
    params: BTreeMap<CardinalIdx, EdgeTypeParam<V>>,
}

impl<'c, 'm, V: NodeView> CstLowering<'c, 'm, V> {
    pub fn lower_edge_statement(
        &mut self,
        edge_stmt: insp::EdgeStatement<V>,
    ) -> Option<(
        RootDefs,
        EdgeId,
        BTreeMap<CardinalIdx, insp::EdgeTypeParam<V>>,
    )> {
        let mut root_defs = vec![];

        let Some(_ident_path) = edge_stmt.ident_path() else {
            return None;
        };

        let edge_id = self
            .ctx
            .compiler
            .edge_ctx
            .alloc_edge_id(self.ctx.package_id);

        let mut edge_builder = EdgeBuilder {
            edge_id,
            slots: Default::default(),
            cardinals: Default::default(),
            cardinal_name_table: Default::default(),
            params: Default::default(),
        };

        for edge_relation in edge_stmt.edge_relations() {
            let mut item_iter = edge_relation.items().peekable();

            match self.next_edge_item(&mut item_iter, edge_relation.view().span()) {
                Some(insp::EdgeItem::EdgeVar(edge_var)) => {
                    let Some(mut prev_cardinal) =
                        self.add_edge_variable(edge_var, &mut edge_builder)
                    else {
                        continue;
                    };

                    loop {
                        if let Some((slot_def_id, next_cardinal)) = self
                            .add_edge_slot_symbol_then_cardinal(
                                &mut item_iter,
                                prev_cardinal,
                                &mut edge_builder,
                                edge_stmt.view().span(),
                            )
                        {
                            self.ctx
                                .compiler
                                .edge_ctx
                                .symbols
                                .insert(slot_def_id, edge_builder.edge_id);

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
                Some(insp::EdgeItem::EdgeSlot(edge_slot)) => {
                    CompileError::EdgeCannotMixStandaloneSymbolsAndSymbolicEdge
                        .span_report(edge_slot.view().span(), &mut self.ctx);
                }
                Some(insp::EdgeItem::EdgeTypeParam(_)) => {
                    unreachable!("statement cannot start with EdgeTypeParam")
                }
                None => {}
            }
        }

        self.ctx.compiler.edge_ctx.symbolic_edges.insert(
            edge_builder.edge_id,
            SymbolicEdge {
                symbols: edge_builder.slots,
                cardinals: edge_builder.cardinals,
            },
        );

        Some((root_defs, edge_id, edge_builder.params))
    }

    fn next_edge_item(
        &mut self,
        item_iter: &mut impl Iterator<Item = insp::EdgeItem<V>>,
        edge_relation_span: U32Span,
    ) -> Option<insp::EdgeItem<V>> {
        let item = item_iter.next();

        if item.is_none() {
            CompileError::EdgeExpectedTrailingItem.span_report(edge_relation_span, &mut self.ctx);
        }

        item
    }

    fn add_edge_slot_symbol_then_cardinal(
        &mut self,
        item_iter: &mut impl Iterator<Item = insp::EdgeItem<V>>,
        prev_cardinal_idx: CardinalIdx,
        edge_builder: &mut EdgeBuilder<V>,
        edge_relation_span: U32Span,
    ) -> Option<(DefId, CardinalIdx)> {
        let next_item = self.next_edge_item(item_iter, edge_relation_span)?;

        let insp::EdgeItem::EdgeSlot(edge_slot) = next_item else {
            CompileError::EdgeExpectedSymbol.span_report(next_item.view().span(), &mut self.ctx);
            return None;
        };

        let symbol = edge_slot.symbol()?;
        let slot_symbol_def_id =
            self.catch(|zelf| zelf.ctx.coin_symbol(symbol.slice(), symbol.span()));
        let next_item = self.next_edge_item(item_iter, edge_relation_span)?;

        let cardinal_idx = match next_item {
            insp::EdgeItem::EdgeVar(edge_var) => self.add_edge_variable(edge_var, edge_builder),
            insp::EdgeItem::EdgeTypeParam(edge_type_param) => {
                self.add_edge_type_param(edge_type_param, edge_builder)
            }
            insp::EdgeItem::EdgeSlot(_) => {
                CompileError::EdgeExpectedVariable
                    .span_report(next_item.view().span(), &mut self.ctx);
                return None;
            }
        };

        match (slot_symbol_def_id, cardinal_idx) {
            (Some(def_id), Some(cardinal_idx)) => {
                edge_builder.slots.insert(
                    def_id,
                    Slot {
                        left: prev_cardinal_idx,
                        depth: 0,
                        right: cardinal_idx,
                    },
                );

                Some((def_id, cardinal_idx))
            }
            _ => None,
        }
    }

    fn add_edge_variable(
        &mut self,
        sym_var: insp::EdgeVar<V>,
        edge_builder: &mut EdgeBuilder<V>,
    ) -> Option<CardinalIdx> {
        let var_symbol = sym_var.symbol()?;
        let len = edge_builder.cardinals.len();

        match edge_builder
            .cardinal_name_table
            .entry(var_symbol.slice().to_string())
        {
            Entry::Occupied(occupied) => Some(*occupied.get()),
            Entry::Vacant(vacant) => {
                let Ok(cardinal_idx): Result<u8, _> = len.try_into() else {
                    CompileError::EdgeArityOverflow
                        .span_report(sym_var.view().span(), &mut self.ctx);

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
                        one_to_one_count: 0,
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
        edge_type_param: insp::EdgeTypeParam<V>,
        edge_builder: &mut EdgeBuilder<V>,
    ) -> Option<CardinalIdx> {
        let ident_path = edge_type_param.ident_path()?;
        let Some(leaf_symbol) = ident_path.symbols().last() else {
            return None;
        };

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
                        one_to_one_count: 0,
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
