use std::collections::{BTreeMap, BTreeSet};

use arcstr::ArcStr;
use ontol_core::{
    property::{PropertyCardinality, ValueCardinality},
    span::U32Span,
};
use ontol_parser::cst::{
    inspect::{self as insp},
    view::{NodeView, TokenView, TypedView},
};
use ontol_syntax::syntax_view::RowanNodeView;
use smallvec::{SmallVec, smallvec};
use thin_vec::thin_vec;

use crate::{
    NoneIfEmpty,
    diff::diff_btree_maps,
    error::{OptionExt, SemError},
    log_model::{ArcCoord, ArcProp, DefModifier, EKind, Log},
    lookup::{
        lookup_type_path, resolve_quant_type_reference, resolve_quant_type_reference_or_pattern,
        resolve_type_reference,
    },
    sem_diff::{Differ, MatchStage},
    sem_match::MatchTracker,
    sem_model::{ProjectedSemModel, StmtNode, StmtRef},
    sem_stmts::{Arc, Def, Rel, Use},
    tag::Tag,
    with_span::{SetSpan, WithSpan},
};

pub struct DomainAnalyzer<'a> {
    log: &'a mut Log,
    domain: Tag,
    origin: ProjectedSemModel,
    // lazy_tasks: Vec<LazyTask>,
    errors: Vec<(SemError, U32Span)>,
}

#[derive(Clone, Copy)]
pub enum BlockContext {
    NoContext,
    Def(Tag),
    RelParams { def: u16 },
    FmtLeading,
}

impl BlockContext {
    pub fn def(&self) -> Option<Tag> {
        match self {
            Self::Def(tag) => Some(*tag),
            _ => None,
        }
    }
}

enum SubNode {
    DefBody(insp::DefBody<RowanNodeView>),
    ArcTypeParams(Vec<(ArcCoord, insp::ArcTypeParam<RowanNodeView>)>),
}

impl<'a> DomainAnalyzer<'a> {
    pub fn new(origin: ProjectedSemModel, log: &'a mut Log) -> Self {
        Self {
            log,
            domain: origin.domain(),
            origin,
            // lazy_tasks: vec![],
            errors: Default::default(),
        }
    }

    pub fn ontol(&mut self, new_root: RowanNodeView) -> Result<(), SemError> {
        let mut tracker = self.origin.new_tracker();

        let Some(new_ontol) = insp::Ontol::from_view(new_root) else {
            return Err(SemError::Ontol);
        };

        // TODO: undo changes
        self.statements(
            new_ontol.statements(),
            BlockContext::NoContext,
            &mut tracker,
        )?;

        for tag in tracker.unmatched_uses() {
            self.log.stage(EKind::UseRemove(tag));
        }

        for tag in tracker.unmatched_defs() {
            self.log.stage(EKind::DefRemove(tag));
        }

        for tag in tracker.unmatched_arcs() {
            self.log.stage(EKind::ArcRemove(tag));
        }

        for tag in tracker.unmatched_rels() {
            self.log.stage(EKind::RelRemove(tag));
        }

        self.origin
            .local
            .sync_to_log(self.log)
            .map_err(SemError::Apply)?;

        self.origin.merge_tracker(tracker);

        Ok(())
    }

    pub fn finish(self) -> ProjectedSemModel {
        self.origin
    }

    fn statements(
        &mut self,
        new: impl Iterator<Item = insp::Statement<RowanNodeView>>,
        context: BlockContext,
        tracker: &mut MatchTracker,
    ) -> Result<(), SemError> {
        let parent = context.def();
        let mut sub_nodes: Vec<(Tag, SubNode)> = vec![];
        let mut pushback = Vec::with_capacity(new.size_hint().0);
        let mut init_diff = tracker.init_diff(parent);
        let mut stage = init_diff.initial_stage();
        let mut differ = init_diff.differ(&self.origin);

        let mut syntax_index: u32 = 0;

        for stmt in new {
            for (stmt_node, sub_node) in self.lower_stmt(stmt, context)? {
                if self.match_statement(
                    stage,
                    (syntax_index, stmt_node, sub_node),
                    &mut differ,
                    &mut pushback,
                    &mut sub_nodes,
                )? {
                    self.origin
                        .local
                        .sync_to_log(self.log)
                        .map_err(SemError::Apply)?;

                    syntax_index += 1;
                }
            }
        }

        while !pushback.is_empty() {
            let Some(next_stage) = stage.bump() else {
                break;
            };
            stage = next_stage;

            for (syntax_index, stmt_node, sub_node) in std::mem::take(&mut pushback) {
                if self.match_statement(
                    stage,
                    (syntax_index, stmt_node, sub_node),
                    &mut differ,
                    &mut pushback,
                    &mut sub_nodes,
                )? {
                    self.origin
                        .local
                        .sync_to_log(self.log)
                        .map_err(SemError::Apply)?;
                }
            }
        }

        for (tag, subnode) in sub_nodes {
            match subnode {
                SubNode::DefBody(def_body) => {
                    self.statements(def_body.statements(), BlockContext::Def(tag), tracker)?;
                }
                SubNode::ArcTypeParams(params) => {
                    let Some(orig) = self.origin.local.arcs.get(&tag) else {
                        continue;
                    };

                    let mut new_params = BTreeMap::new();

                    for (coord, type_param) in params {
                        let type_ref =
                            lookup_type_path(&type_param.ident_path().stx_err()?, &self.origin)?;

                        new_params.insert(coord, type_ref);
                    }

                    let mut props = thin_vec![];
                    diff_btree_maps(&new_params, &orig.params, |added, coord, value| {
                        if added {
                            props.push(ArcProp::TypeParam(*coord, value.clone()));
                        } else {
                            todo!("arc \"slot\" removal");
                        }
                    });

                    if let Some(props) = props.none_if_empty() {
                        self.log.stage(EKind::ArcChange(tag, props));
                    }
                }
            }
        }

        Ok(())
    }

    fn match_statement(
        &mut self,
        stage: MatchStage,
        (syntax_index, stmt_node, sub_node): (u32, StmtNode, Option<SubNode>),
        differ: &mut Differ,
        pushback: &mut Vec<(u32, StmtNode, Option<SubNode>)>,
        sub_nodes: &mut Vec<(Tag, SubNode)>,
    ) -> Result<bool, SemError> {
        let stmt_ref = match &stmt_node {
            StmtNode::Use(u) => StmtRef::Use(&u),
            StmtNode::Def(def) => StmtRef::Def(&def),
            StmtNode::Arc(arc) => StmtRef::Arc(&arc),
            StmtNode::Rel(rel) => StmtRef::Rel(&rel),
            StmtNode::Todo => return Ok(false),
        };

        let Some((tag, origin)) = differ.try_match(stage, syntax_index, stmt_ref, &self.origin)?
        else {
            pushback.push((syntax_index, stmt_node, sub_node));
            return Ok(false);
        };

        match (stmt_node, origin) {
            (StmtNode::Use(u), Some(StmtRef::Use(origin))) => {
                if let Some(props) = u.diff(origin) {
                    self.log.stage(EKind::UseChange(tag, props));
                }
            }
            (StmtNode::Use(u), None) => {
                if let Some(props) = u.diff(&Use::default()) {
                    self.log.stage(EKind::UseAdd(tag, props));
                }
            }
            (StmtNode::Def(def), Some(StmtRef::Def(origin))) => {
                if let Some(props) = def.diff(origin) {
                    self.log.stage(EKind::DefChange(tag, props));
                }
            }
            (StmtNode::Def(def), None) => {
                if let Some(props) = def.diff(&Def::default()) {
                    self.log.stage(EKind::DefAdd(tag, props));
                }
            }
            (StmtNode::Arc(arc), Some(StmtRef::Arc(origin))) => {
                if let Some(props) = arc.diff_sans_params(origin) {
                    self.log.stage(EKind::ArcChange(tag, props));
                }
            }
            (StmtNode::Arc(arc), None) => {
                if let Some(props) = arc.diff_sans_params(&Arc::default()) {
                    self.log.stage(EKind::ArcAdd(tag, props));
                }
            }
            (StmtNode::Rel(rel), Some(StmtRef::Rel(origin))) => {
                if let Some(props) = rel.diff(Some(origin)) {
                    self.log.stage(EKind::RelChange(tag, props));
                }
            }
            (StmtNode::Rel(rel), None) => {
                if let Some(props) = rel.diff(None) {
                    self.log.stage(EKind::RelAdd(tag, props));
                }
            }
            _ => return Ok(false),
        };

        if let Some(sub_node) = sub_node {
            sub_nodes.push((tag, sub_node));
        }

        Ok(true)
    }

    fn lower_stmt(
        &mut self,
        stmt: insp::Statement<RowanNodeView>,
        context: BlockContext,
    ) -> Result<SmallVec<(StmtNode, Option<SubNode>), 1>, SemError> {
        match stmt {
            insp::Statement::DomainStatement(stmt) => Ok(smallvec![(StmtNode::Todo, None)]),
            insp::Statement::UseStatement(stmt) => Ok(smallvec![self.lower_use(stmt)?]),
            insp::Statement::DefStatement(stmt) => Ok(smallvec![self.lower_def(stmt)?]),
            insp::Statement::SymStatement(stmt) => self.lower_sym(stmt),
            insp::Statement::ArcStatement(stmt) => Ok(smallvec![self.lower_arc(stmt)?]),
            insp::Statement::RelStatement(stmt) => {
                Ok(smallvec![(self.lower_rel(stmt, context)?, None)])
            }
            insp::Statement::FmtStatement(stmt) => Ok(smallvec![(StmtNode::Todo, None)]),
            insp::Statement::MapStatement(stmt) => Ok(smallvec![(StmtNode::Todo, None)]),
        }
    }

    fn lower_use(
        &mut self,
        stmt: insp::UseStatement<RowanNodeView>,
    ) -> Result<(StmtNode, Option<SubNode>), SemError> {
        let uri = stmt.uri().stx_err()?;
        let uri_text = uri.text().stx_err()?.map_err(SemError::Parse)?;

        let ident = stmt.ident_path().stx_err()?.symbols().next().stx_err()?;

        Ok((
            StmtNode::Use(Use {
                domain: Some(self.domain),
                uri: Some(ArcStr::from(uri_text).set_span(uri.view().span())),
                ident: Some(WithSpan::from(ident)),
            }),
            None,
        ))
    }

    fn lower_def(
        &mut self,
        stmt: insp::DefStatement<RowanNodeView>,
    ) -> Result<(StmtNode, Option<SubNode>), SemError> {
        let ident = stmt.ident_path().stx_err()?.symbols().next().stx_err()?;
        let mut modifiers = BTreeSet::new();

        for modifier in stmt.modifiers() {
            modifiers.insert(def_modifier_prop(modifier).stx_err()?);
        }

        Ok((
            StmtNode::Def(Def {
                domain: Some(self.domain),
                ident: Some(WithSpan::from(ident)),
                // FIXME: doc
                doc: None,
                symbol: false,
                modifiers,
            }),
            stmt.body().map(SubNode::DefBody),
        ))
    }

    fn lower_sym(
        &mut self,
        stmt: insp::SymStatement<RowanNodeView>,
    ) -> Result<SmallVec<(StmtNode, Option<SubNode>), 1>, SemError> {
        let mut stmts = smallvec![];

        for sym_rel in stmt.sym_relations() {
            let symbol = sym_rel.decl().stx_err()?.symbol().stx_err()?;

            stmts.push((
                StmtNode::Def(Def {
                    domain: Some(self.domain),
                    ident: Some(WithSpan::from(symbol)),
                    // FIXME: doc
                    doc: None,
                    symbol: true,
                    modifiers: Default::default(),
                }),
                None,
            ));
        }

        Ok(stmts)
    }

    fn lower_arc(
        &mut self,
        stmt: insp::ArcStatement<RowanNodeView>,
    ) -> Result<(StmtNode, Option<SubNode>), SemError> {
        let mut param_coords = vec![];
        let ident = stmt.ident_path().stx_err()?.symbols().next().stx_err()?;

        let mut arc = Arc {
            domain: Some(self.domain),
            ident: Some(WithSpan::from(ident)),
            vars: Default::default(),
            params: Default::default(),
            slots: Default::default(),
        };

        for (row, arc_clause) in stmt.arc_clauses().enumerate() {
            let row = u8::try_from(row).ok().stx_err()?;

            let mut items = arc_clause.items();

            let insp::ArcItem::ArcVar(var) = items.next().stx_err()? else {
                return Err(SemError::ArcSyntax);
            };

            arc.vars
                .insert(ArcCoord(row, 0), WithSpan::from(var.symbol().stx_err()?));

            let mut column = 1;

            while let Some(next) = items.next() {
                match next {
                    insp::ArcItem::ArcSlot(slot) => {
                        arc.slots.insert(
                            ArcCoord(row, column),
                            WithSpan::from(slot.symbol().stx_err()?),
                        );
                    }
                    insp::ArcItem::ArcVar(_) | insp::ArcItem::ArcTypeParam(_) => {
                        return Err(SemError::ArcSyntax);
                    }
                }

                match items.next().stx_err()? {
                    insp::ArcItem::ArcVar(var) => {
                        arc.vars
                            .insert(ArcCoord(row, 0), WithSpan::from(var.symbol().stx_err()?));
                    }
                    insp::ArcItem::ArcTypeParam(type_param) => {
                        // ArcTypeParam is handled in the lazy task,
                        // since its type may be unknown
                        param_coords.push((ArcCoord(row, 0), type_param));
                    }
                    insp::ArcItem::ArcSlot(_) => {
                        return Err(SemError::ArcSyntax);
                    }
                }

                column += 1;
            }
        }

        Ok((
            StmtNode::Arc(arc),
            param_coords.none_if_empty().map(SubNode::ArcTypeParams),
        ))
    }

    fn lower_rel(
        &mut self,
        stmt: insp::RelStatement<RowanNodeView>,
        context: BlockContext,
    ) -> Result<StmtNode, SemError> {
        const SUBJ_PROP_CRD: PropertyCardinality = PropertyCardinality::Mandatory;
        const OBJ_PROP_CRD: PropertyCardinality = PropertyCardinality::Mandatory;

        let relation = stmt.relation().stx_err()?;

        let rel_type = resolve_type_reference(
            relation.relation_type().stx_err()?.type_ref().stx_err()?,
            &mut ValueCardinality::Unit,
            context,
            &self.origin,
        )?
        .left()
        .stx_err()?;

        let (subj_type, subj_crd) = resolve_quant_type_reference(
            stmt.subject().stx_err()?.type_quant().stx_err()?,
            context,
            &self.origin,
        )?;

        let (obj_type, obj_crd) = resolve_quant_type_reference_or_pattern(
            stmt.object().stx_err()?.type_quant_or_pattern().stx_err()?,
            context,
            &self.origin,
        )?;

        Ok(StmtNode::Rel(Rel {
            domain: Some(self.domain),
            rel_type: Some(rel_type),
            subj_cardinality: (SUBJ_PROP_CRD, subj_crd).into(),
            subj_type,
            obj_cardinality: (OBJ_PROP_CRD, obj_crd).into(),
            obj_type,
        }))
    }
}

pub fn def_modifier_prop(modifier: insp::Modifier<RowanNodeView>) -> Option<WithSpan<DefModifier>> {
    let token = modifier.token()?;
    let span = token.span();
    match token.0.text() {
        "@private" => Some(DefModifier::Private.set_span(span)),
        "@open" => Some(DefModifier::Open.set_span(span)),
        "@extern" => Some(DefModifier::Extern.set_span(span)),
        "@macro" => Some(DefModifier::Macro.set_span(span)),
        "@crdt" => Some(DefModifier::Crdt.set_span(span)),
        _ => None,
    }
}
